import json
from unittest.mock import AsyncMock, MagicMock

import pytest
import student_shadow
from detection import WindowScore, update_window_and_detect
from listener_telemetry import student_shadow_scores_total
from models import MarketTick, TickKind
from shared_utils import ListingRateTracker
from student_features import listing_features
from student_scorer import StudentModelError, StudentScore
from student_shadow import StudentShadow, load_student_scorer

BASELINE = {"avg_30d_cents": 1200, "latest_price_cents": 1150, "support_floor_cents": 1000, "macro_cv": 0.1}


def tick(listing_id: str | None = "L1", price_usd=10.0, at=1_790_000_000, name="AK-47 | Redline (Field-Tested)"):
    return MarketTick(
        market_hash_name=name,
        price_usd=price_usd,
        venue="skinport",
        kind=TickKind.LISTED if listing_id else TickKind.REST_SNAPSHOT,
        timestamp=at,
        observed_at_ms=at * 1000 + 250,
        listing_id=listing_id,
        event_type="listed" if listing_id else None,
        float_value=0.21 if listing_id else None,
    )


def window(z_score=-2.5):
    return WindowScore(z_score=z_score, mean_cents=1300.0, source="local", window_size=20, baseline=BASELINE)


class RecordingScorer:
    """Scores every listing 0.7 against a 0.5 threshold and keeps the features it was given."""

    model_version = "brand-sniper-student/1"
    feature_version = 3

    def __init__(self, probability=0.7, error: Exception | None = None):
        self.probability, self.error = probability, error
        self.seen: list[dict] = []

    def score(self, features):
        if self.error is not None:
            raise self.error
        self.seen.append(dict(features))
        return StudentScore(probability=self.probability, threshold=0.5)


class QueuePool:
    """try_submit keeps the jobs, so a test runs them when it wants; `full` refuses them."""

    def __init__(self, full=False):
        self.full, self.jobs = full, []

    def try_submit(self, factory):
        if self.full:
            return False
        self.jobs.append(factory)
        return True

    async def run_all(self):
        jobs, self.jobs = self.jobs, []
        for job in jobs:
            await job()


def counter(outcome: str) -> float:
    return student_shadow_scores_total.labels(outcome=outcome)._value.get()


def redis_with_baseline():
    cache = MagicMock()
    cache.hget = AsyncMock(return_value=json.dumps(BASELINE))
    return cache


@pytest.mark.asyncio
async def test_a_listing_is_scored_off_the_consumer_and_shipped_with_the_next_batch():
    scorer, pool = RecordingScorer(), QueuePool()
    shadow = StudentShadow(scorer, pool, redis_with_baseline())
    approvals_before = counter("would_approve")

    shadow.observe(tick(), window())
    # Nothing is scored on the consumer itself: the job waits for the shadow pool.
    assert scorer.seen == [] and len(pool.jobs) == 1
    await pool.run_all()

    (score,) = shadow.drain()
    assert score["listing_id"] == "L1"
    assert (score["model_version"], score["feature_version"]) == ("brand-sniper-student/1", 3)
    assert (score["probability"], score["threshold"], score["would_approve"]) == (0.7, 0.5, True)
    assert score["observed_at_ms"] == 1_790_000_000_250
    assert score["scoring_ms"] >= 0
    assert shadow.drain() == []
    assert counter("would_approve") == approvals_before + 1


@pytest.mark.asyncio
async def test_a_listing_below_the_threshold_is_recorded_as_a_shadow_rejection():
    pool = QueuePool()
    shadow = StudentShadow(RecordingScorer(probability=0.2), pool, redis_with_baseline())
    approvals_before, scored_before = counter("would_approve"), counter("scored")

    shadow.observe(tick(), window())
    await pool.run_all()

    (score,) = shadow.drain()
    assert (score["probability"], score["would_approve"]) == (0.2, False)
    assert counter("scored") == scored_before + 1
    assert counter("would_approve") == approvals_before


@pytest.mark.asyncio
async def test_rest_snapshots_name_no_listing_and_are_not_scored():
    pool = QueuePool()
    StudentShadow(RecordingScorer(), pool, redis_with_baseline()).observe(tick(listing_id=None), window())

    assert pool.jobs == []


@pytest.mark.asyncio
async def test_live_shadow_features_match_the_replay_features_in_decision_order():
    # Three listings of one item and one of another; the second has no scored window (too little history).
    sequence = [
        (tick("A", 9.0, at=1_790_000_000), window()),
        (tick("B", 9.5, at=1_790_000_600), None),
        (tick("C", 8.0, at=1_790_001_200, name="AWP | Asiimov (Field-Tested)"), window(-3.1)),
        (tick("D", 8.5, at=1_790_001_800), window(-1.2)),
    ]
    scorer, pool = RecordingScorer(), QueuePool()
    shadow = StudentShadow(scorer, pool, redis_with_baseline())
    for listing, scored_window in sequence:
        shadow.observe(listing, scored_window)
    # The background jobs run after every listing was recorded, as they may live; the rates are already fixed.
    await pool.run_all()

    rates = ListingRateTracker()
    replay = [await listing_features(listing, w, redis_with_baseline(), rates) for listing, w in sequence]

    assert scorer.seen == replay


@pytest.mark.asyncio
async def test_a_full_shadow_queue_drops_the_score_and_never_waits():
    shadow = StudentShadow(RecordingScorer(), QueuePool(full=True), redis_with_baseline())
    dropped_before = counter("dropped")

    shadow.observe(tick(), window())

    assert counter("dropped") == dropped_before + 1
    assert shadow.drain() == []


@pytest.mark.asyncio
async def test_a_scoring_failure_is_counted_and_never_reaches_the_caller():
    pool = QueuePool()
    shadow = StudentShadow(RecordingScorer(error=RuntimeError("onnx exploded")), pool, redis_with_baseline())
    failed_before = counter("failed")

    shadow.observe(tick(), window())
    await pool.run_all()

    assert counter("failed") == failed_before + 1
    assert shadow.drain() == []


@pytest.mark.asyncio
async def test_a_failure_to_queue_is_counted_and_never_reaches_the_consumer():
    pool = MagicMock()
    pool.try_submit.side_effect = RuntimeError("pool closed")
    failed_before = counter("failed")

    StudentShadow(RecordingScorer(), pool, redis_with_baseline()).observe(tick(), window())

    assert counter("failed") == failed_before + 1


@pytest.mark.asyncio
async def test_detection_hands_every_listing_to_the_shadow_even_without_a_scored_window(monkeypatch):
    shadow = MagicMock()
    monkeypatch.setattr("detection.push_to_window", AsyncMock())
    monkeypatch.setattr("detection.score_window", AsyncMock(return_value=None))
    listing = tick()

    await update_window_and_detect(listing, MagicMock(), AsyncMock(), AsyncMock(), shadow=shadow)

    shadow.observe.assert_called_once_with(listing, None)


@pytest.mark.asyncio
async def test_no_model_version_means_no_shadow(monkeypatch):
    monkeypatch.delenv("STUDENT_MODEL_VERSION", raising=False)
    session_factory = AsyncMock()

    assert await load_student_scorer(session_factory) is None
    session_factory.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_model_that_cannot_be_loaded_leaves_the_rules_alone(monkeypatch):
    monkeypatch.setenv("STUDENT_MODEL_VERSION", "9")
    monkeypatch.setattr(student_shadow, "fetch_registered_model", AsyncMock(side_effect=StudentModelError("gone")))

    assert await load_student_scorer(AsyncMock()) is None


@pytest.mark.asyncio
async def test_the_named_version_is_fetched_from_mlflow_and_loaded(monkeypatch):
    monkeypatch.setenv("STUDENT_MODEL_VERSION", "4")
    monkeypatch.setenv("STUDENT_MODEL_NAME", "other-student")
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
    fetch = AsyncMock(return_value=b"graph")
    loaded = MagicMock(feature_version=3, threshold=0.4)
    from_bytes = MagicMock(return_value=loaded)
    monkeypatch.setattr(student_shadow, "fetch_registered_model", fetch)
    monkeypatch.setattr(student_shadow.StudentScorer, "from_bytes", from_bytes)
    session = object()

    assert await load_student_scorer(AsyncMock(return_value=session)) is loaded
    fetch.assert_awaited_once_with(session, "http://mlflow-server:5000", "other-student", "4")
    from_bytes.assert_called_once_with(b"graph", "other-student/4")


def test_shadow_logs_under_the_listener_logger():
    # A logger outside "listener." has no handler in the container, so its lines never reach the logs.
    assert student_shadow.logger.name == "listener.student_shadow"
