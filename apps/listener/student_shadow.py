"""
Shadow mode for the student model (#236): the model scores every feed listing in parallel with the rules,
and decides nothing.

The tick consumer only records the listing's rate (`StudentShadow.observe`, in memory) and queues the rest
without waiting. A worker of its own pool then gathers the features from the edge Redis, runs the model and
buffers the score, which the consumer ships in its next batch to `student_scores`. A full queue drops the
score (counted), a failure is logged and counted, and neither ever reaches the rules. Scores still queued when
the listener stops are lost; the rules' decisions and the recorded ticks are not.

The scored listings are the ones the replay's `zscore_dre_features` strategy gives features to (every listing
that reaches detection), with the same feature code, so a replay of the same ticks scores them identically.

Configuration: `STUDENT_MODEL_VERSION` (unset: no shadow mode), `STUDENT_MODEL_NAME` (default
brand-sniper-student) and `MLFLOW_TRACKING_URI`.
"""

import logging
import os
import time
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any

import aiohttp
from detection import WindowScore
from listener_telemetry import student_model_loaded, student_shadow_scores_total, student_shadow_scoring_seconds
from models import MarketTick
from redis.asyncio import Redis
from shared_utils import ListingRateTracker
from student_features import features_with_rate, record_listing
from student_scorer import StudentModelError, StudentScorer, fetch_registered_model
from task_supervisor import BoundedTaskPool

logger = logging.getLogger(__name__)

DEFAULT_MODEL_NAME = "brand-sniper-student"
DEFAULT_TRACKING_URI = "http://localhost:5000"
# Scores waiting for a worker. A listing takes well under a millisecond, so this only fills if Redis stalls.
SHADOW_QUEUE_SIZE = 1000

SessionFactory = Callable[[], Awaitable[aiohttp.ClientSession]]


class StudentShadow:
    """Scores listings with one loaded model, off the tick consumer, and buffers the scores for the next batch."""

    def __init__(self, scorer: StudentScorer, pool: BoundedTaskPool, cache: Redis) -> None:
        self.scorer = scorer
        self._pool = pool
        self._cache = cache
        self._rates = ListingRateTracker()
        self._scores: list[dict[str, Any]] = []

    def observe(self, tick: MarketTick, window_score: WindowScore | None) -> None:
        """Hands a listing that reached detection to the model. Never waits and never raises."""
        if tick.listing_id is None:
            return
        try:
            listings_last_hour = record_listing(tick, self._rates)
            queued = self._pool.try_submit(partial(self._score, tick, window_score, listings_last_hour))
        except Exception:
            logger.exception("[STUDENT] Could not queue %s for shadow scoring", tick.market_hash_name)
            student_shadow_scores_total.labels(outcome="failed").inc()
            return
        if not queued:
            student_shadow_scores_total.labels(outcome="dropped").inc()

    async def _score(self, tick: MarketTick, window_score: WindowScore | None, listings_last_hour: int) -> None:
        started = time.perf_counter()
        try:
            features = await features_with_rate(tick, window_score, self._cache, listings_last_hour)
            score = self.scorer.score(features)
        except Exception as error:
            student_shadow_scores_total.labels(outcome="failed").inc()
            logger.warning("[STUDENT] Shadow scoring failed for %s: %s", tick.market_hash_name, error)
            return
        elapsed = time.perf_counter() - started
        student_shadow_scoring_seconds.observe(elapsed)
        student_shadow_scores_total.labels(outcome="scored").inc()
        if score.would_approve:
            student_shadow_scores_total.labels(outcome="would_approve").inc()
            logger.info(
                "[STUDENT] Shadow approval (%s): %s at $%.2f, probability %.3f against threshold %.3f.",
                self.scorer.model_version,
                tick.market_hash_name,
                tick.price_usd,
                score.probability,
                score.threshold,
            )
        self._scores.append(
            {
                "listing_id": tick.listing_id,
                "model_version": self.scorer.model_version,
                "feature_version": self.scorer.feature_version,
                "probability": score.probability,
                "threshold": score.threshold,
                "would_approve": score.would_approve,
                "observed_at_ms": tick.observed_at_epoch_ms,
                "scoring_ms": elapsed * 1000,
            }
        )

    def drain(self) -> list[dict[str, Any]]:
        """The scores made since the last call, for the batch being written."""
        scores, self._scores = self._scores, []
        return scores


async def load_student_scorer(session_factory: SessionFactory) -> StudentScorer | None:
    """The model named by the environment, or None: shadow mode off, or the model cannot be used.

    A model that fails to load never stops the listener; it runs on the rules alone, as without one."""
    version = os.getenv("STUDENT_MODEL_VERSION", "").strip()
    if not version:
        logger.info("[STUDENT] No STUDENT_MODEL_VERSION: shadow scoring is off.")
        return None
    name = os.getenv("STUDENT_MODEL_NAME", DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", DEFAULT_TRACKING_URI)
    model_version = f"{name}/{version}"
    try:
        model_bytes = await fetch_registered_model(await session_factory(), tracking_uri, name, version)
        scorer = StudentScorer.from_bytes(model_bytes, model_version)
    except StudentModelError as error:
        logger.error("[STUDENT] Shadow scoring is off, the rules decide alone: %s", error)
        return None
    student_model_loaded.labels(model_version=model_version).set(1)
    logger.info(
        "[STUDENT] Shadow scoring with %s (feature version %d, threshold %.3f); the rules still decide.",
        model_version,
        scorer.feature_version,
        scorer.threshold,
    )
    return scorer
