import asyncio
import importlib.util
import io
import json
import math
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import detection
import pytest
import zscore
from backtest import database
from backtest.harness import run_replay
from backtest.sources import (
    POLL_GAP_MS,
    BaselineSchedule,
    BaselineSnapshot,
    PollClock,
    RecordedFeedEvent,
    RecordedSnapshot,
    ScheduledBuild,
    expand_event,
    load_fixture,
    merge_ordered,
    sanitize_feed_payload,
    write_fixture,
)
from backtest.strategy import (
    REASON_BELOW_FEE_FLOOR,
    REASON_BELOW_THRESHOLD,
    REASON_DRE_REJECTED,
    REASON_DUPLICATE,
    REASON_INSUFFICIENT_HISTORY,
    REASON_MACRO_ONLY,
    STRATEGIES,
    ZScoreDreFeaturesStrategy,
    ZScoreDreStrategy,
    ZScoreDreSweepStrategy,
)
from executor import ExecutionService
from models import FeedEvent, MarketTick
from rules_engine import REASON_MACRO_SIGMA, REASON_STICKER_PREMIUM, REASON_STICKERS_BELOW_BASE, REASON_SUPPORT_FLOOR
from shared_utils import FEATURE_NAMES, FEATURE_VERSION, ListingRateTracker, build_versioned_name, feature_vector
from student_features import listing_features
from task_supervisor import BoundedTaskPool

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "replay"
T0_MS = 1_790_000_000_000
ITEM = "AK-47 | Slate (Field-Tested)"
STICKER_ITEM = "M4A1-S | Nitro (Minimal Wear)"
VALUABLE_STICKER = "Titan | Katowice 2014"


def load_listener_main():
    module_path = Path(__file__).resolve().parents[1] / "main.py"
    spec = importlib.util.spec_from_file_location("listener_main_backtest_test", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load listener main module for parity tests")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


listener_main = load_listener_main()


def _snapshot(offset_s: int, price_cents: int, name: str = ITEM) -> RecordedSnapshot:
    return RecordedSnapshot(observed_at_ms=T0_MS + offset_s * 1000, market_hash_name=name, price_cents=price_cents)


def _sale(product_id: int, price_cents: int, name: str = ITEM, **fields) -> dict:
    return {"marketHashName": name, "salePrice": price_cents, "productId": product_id, "currency": "USD", **fields}


def _feed(offset_s: int, event_type: str, *sales: dict) -> RecordedFeedEvent:
    return RecordedFeedEvent(received_at_ms=T0_MS + offset_s * 1000, payload={"eventType": event_type, "sales": list(sales)})


def synthetic_stream() -> tuple[list, BaselineSnapshot]:
    """A small stream that reaches every Z-score/DRE outcome, including both sticker rules."""
    events = [_snapshot(index * 305, price) for index, price in enumerate([1000, 1010, 990, 1005, 995])]
    events += [_snapshot(index * 305, price, STICKER_ITEM) for index, price in enumerate([3000, 3020, 2980, 3010, 2990])]
    events += [
        _feed(1600, "listed", _sale(1, 1000)),  # below threshold
        _feed(1601, "listed", _sale(2, 1000)),  # another listing at the same price: below threshold too
        _feed(1601, "listed", _sale(1, 1000)),  # the first listing again: duplicate
        _feed(1602, "listed", _sale(4, 850)),  # outlier the DRE rejects
        _feed(1603, "listed", _sale(3, 400)),  # support floor
        _feed(1604, "sold", _sale(3, 400)),  # outcome only
        _feed(1605, "listed", _sale(6, 2870, STICKER_ITEM, stickers=[{"name": VALUABLE_STICKER}])),  # sticker premium
        _feed(1606, "listed", _sale(5, 2500, STICKER_ITEM, stickers=[{"name": VALUABLE_STICKER}])),  # stickers below base
        _feed(1607, "listed", _sale(7, 777, "Unknown Item")),  # no history, no baseline
    ]
    baselines = BaselineSnapshot(
        baselines={
            ITEM: {"support_floor_cents": 500, "latest_price_cents": 1000},
            STICKER_ITEM: {"support_floor_cents": 1000, "latest_price_cents": 2800},
        },
        sticker_prices={VALUABLE_STICKER: 500_000},
    )
    return events, baselines


def cold_start_stream() -> tuple[list, BaselineSnapshot]:
    """
    Right after a restart: one item is seen for the first time far under its baseline, so its window is too
    short and it is scored with the macro Z score; another has a full window and a listing under its floor.
    """
    cold_item = "AWP | Safari Mesh (Field-Tested)"
    events = [_snapshot(0, 400, cold_item)]
    events += [_snapshot(index * 305, price) for index, price in enumerate([1000, 1010, 990, 1005, 995])]
    events += [
        _feed(10, "listed", _sale(1, 380, cold_item)),  # window of 2: macro Z
        _feed(1600, "listed", _sale(2, 400)),  # full window: support floor
    ]
    baseline = {"support_floor_cents": 500, "latest_price_cents": 1000, "rolling_30d_avg_cents": 1000, "volatility_cents": 50}
    return events, BaselineSnapshot(baselines={ITEM: baseline, cold_item: baseline})


async def _replay(events, baselines, strategy=None, **kwargs) -> tuple[str, list[tuple[MarketTick, object]]]:
    decided: list[tuple[MarketTick, object]] = []
    log = io.StringIO()
    await run_replay(
        events,
        strategy or ZScoreDreStrategy(),
        baselines,
        log,
        source_label="test",
        on_timing=lambda tick, decision, _elapsed_ns: decided.append((tick, decision)),
        **kwargs,
    )
    return log.getvalue(), decided


def _rows(log_text: str, record_type: str = "decision") -> list[dict]:
    return [record for record in map(json.loads, log_text.splitlines()) if record["type"] == record_type]


# --- Sources ---


def test_sanitize_keeps_only_parser_fields_and_filters_sales():
    payload = {
        "eventType": "listed",
        "sales": [_sale(1, 1000, steamid="76561190000000001", link="steam://..."), _sale(2, 900, "Other"), "junk"],
    }

    sanitized = sanitize_feed_payload(payload, lambda sale: sale["marketHashName"] == ITEM)

    assert sanitized == {"eventType": "listed", "sales": [_sale(1, 1000)]}
    assert sanitize_feed_payload(payload, lambda sale: False) is None
    assert sanitize_feed_payload({"eventType": "listed", "sales": None}) is None


def test_feed_events_expand_through_the_live_parser():
    items = expand_event(_feed(0, "listed", _sale(1, 1234, version="Phase 2")))

    assert isinstance(items[0], FeedEvent)
    tick = items[1]
    assert isinstance(tick, MarketTick)
    assert (tick.market_hash_name, tick.price_cents, tick.listing_id, tick.timestamp) == (
        build_versioned_name(ITEM, "Phase 2"),
        1234,
        "1",
        T0_MS // 1000,
    )


def test_snapshots_expand_to_the_rest_tick():
    [tick] = expand_event(_snapshot(5, 199_999))

    assert isinstance(tick, MarketTick)
    assert (tick.price_cents, tick.timestamp, tick.event_type, tick.listing_id) == (199_999, T0_MS // 1000 + 5, None, None)


def test_poll_clock_stamps_each_poll_with_its_first_insert():
    clock = PollClock()

    stamps = [clock.stamp(ms) for ms in (0, 4_000, 19_000, 19_000 + POLL_GAP_MS + 1, 19_000 + POLL_GAP_MS + 3_000)]

    assert stamps == [0, 0, 0, 19_000 + POLL_GAP_MS + 1, 19_000 + POLL_GAP_MS + 1]


def test_fixture_round_trip_sorts_by_time_then_snapshots_first(tmp_path):
    feed = _feed(10, "listed", _sale(1, 1000))
    same_time_snapshot = _snapshot(10, 999)
    early = _snapshot(0, 1000)
    path = tmp_path / "events.jsonl"

    assert write_fixture(path, [feed, same_time_snapshot, early]) == 3
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    assert load_fixture(path) == [early, same_time_snapshot, feed]


@pytest.mark.parametrize(
    "line",
    ['{"kind": "feed", "received_at_ms": "soon", "payload": {}}', '{"kind": "trade"}', "[]", "not json"],
)
def test_fixture_rejects_malformed_lines(tmp_path, line):
    path = tmp_path / "events.jsonl"
    path.write_text(line + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="line 1"):
        load_fixture(path)


@pytest.mark.asyncio
async def test_merge_ordered_interleaves_streams_in_replay_order():
    async def stream(*events):
        for event in events:
            yield event

    feed_at_10 = _feed(10, "listed", _sale(1, 1000))
    merged = [
        event
        async for event in merge_ordered(
            stream(_feed(0, "sold"), feed_at_10), stream(_snapshot(5, 1), _snapshot(10, 2)), stream()
        )
    ]

    assert [event.time_ms for event in merged] == [T0_MS, T0_MS + 5000, T0_MS + 10_000, T0_MS + 10_000]
    assert merged[2] == _snapshot(10, 2)  # snapshot before the feed event of the same millisecond
    assert merged[3] == feed_at_10


def test_baseline_snapshot_round_trip_and_hash(tmp_path):
    snapshot = BaselineSnapshot(
        baselines={ITEM: {"latest_price_cents": 1}}, sticker_prices={"S": 5}, as_of="2026-09-24T00:00:00"
    )
    path = tmp_path / "baselines.json"
    snapshot.write(path)

    loaded = BaselineSnapshot.load(path)

    assert loaded == snapshot
    assert loaded.sha256() == snapshot.sha256()
    assert BaselineSnapshot().sha256() != snapshot.sha256()


def test_baseline_snapshot_rejects_a_file_without_baselines(tmp_path):
    path = tmp_path / "baselines.json"
    path.write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match="baselines"):
        BaselineSnapshot.load(path)


# --- Harness and strategy ---


@pytest.mark.asyncio
async def test_synthetic_stream_reaches_every_decision_reason():
    events, baselines = synthetic_stream()

    log_text, _ = await _replay(events, baselines)

    reasons = [(row["listing_id"], row["reason"], row["approve"]) for row in _rows(log_text)]
    assert reasons == [
        ("1", REASON_BELOW_THRESHOLD, False),
        ("2", REASON_BELOW_THRESHOLD, False),
        ("1", REASON_DUPLICATE, False),
        ("4", REASON_DRE_REJECTED, False),
        ("3", REASON_SUPPORT_FLOOR, True),
        ("6", REASON_STICKER_PREMIUM, True),
        ("5", REASON_STICKERS_BELOW_BASE, True),
        ("7", REASON_INSUFFICIENT_HISTORY, False),
    ]
    [summary] = _rows(log_text, "summary")
    assert summary["outcome_ticks"] == 1
    assert summary["feed_events"] == 9
    assert summary["duplicates"] == 1


@pytest.mark.asyncio
async def test_a_macro_z_score_is_logged_but_never_approved(monkeypatch):
    monkeypatch.setattr(zscore, "MACRO_ZSCORE_TRIGGERS", False)
    events, baselines = cold_start_stream()

    log_text, _ = await _replay(events, baselines)

    rows = {row["listing_id"]: row for row in _rows(log_text)}
    assert (rows["1"]["reason"], rows["1"]["approve"]) == (REASON_MACRO_ONLY, False)
    assert rows["1"]["features"]["z_source"] == "macro"
    assert rows["1"]["score"] < -2.0
    assert (rows["2"]["reason"], rows["2"]["approve"]) == (REASON_SUPPORT_FLOOR, True)
    assert json.loads(log_text.splitlines()[0])["config"]["macro_zscore_triggers"] is False


@pytest.mark.asyncio
async def test_a_macro_z_score_is_approved_when_enabled(monkeypatch):
    monkeypatch.setattr(zscore, "MACRO_ZSCORE_TRIGGERS", True)
    events, baselines = cold_start_stream()

    log_text, _ = await _replay(events, baselines)

    rows = {row["listing_id"]: row for row in _rows(log_text)}
    assert (rows["1"]["reason"], rows["1"]["approve"]) == (REASON_SUPPORT_FLOOR, True)


@pytest.mark.asyncio
async def test_approved_decisions_carry_the_fee_aware_estimate():
    events, baselines = synthetic_stream()

    log_text, _ = await _replay(events, baselines)

    [support_floor] = [row for row in _rows(log_text) if row["listing_id"] == "3"]
    # Resale at 1000 less the 8% seller fee (80), less the 400 buy price.
    assert support_floor["features"]["estimated_net_profit_cents"] == 520
    assert support_floor["features"]["z_source"] == "local"
    assert support_floor["score"] < -2.0


@pytest.mark.asyncio
async def test_snapshots_are_logged_only_when_approved():
    events = [_snapshot(index * 305, price) for index, price in enumerate([1000, 1010, 990, 1005, 995, 1000, 400])]
    baselines = BaselineSnapshot(baselines={ITEM: {"support_floor_cents": 500, "latest_price_cents": 1000}})

    log_text, decided = await _replay(events, baselines)

    assert len(decided) == 7
    assert [(row["price_cents"], row["reason"]) for row in _rows(log_text)] == [(400, REASON_SUPPORT_FLOOR)]


@pytest.mark.asyncio
async def test_unchanged_snapshots_are_not_scored_again_and_enter_the_window_once_an_hour():
    # Polls are 305 seconds apart, so the repeated 400 is past the dedup window every time. The twelfth
    # repeat is 3,660 seconds after the 400 entered the window, the first one past the hour.
    prices = [1000, 1010, 990, 1005, 995, 400] + [400] * 12 + [1000]
    events = [_snapshot(index * 305, price) for index, price in enumerate(prices)]
    baselines = BaselineSnapshot(baselines={ITEM: {"support_floor_cents": 500, "latest_price_cents": 1000}})

    log_text, decided = await _replay(events, baselines)

    [summary] = _rows(log_text, "summary")
    assert summary["unchanged_snapshots"] == 12
    assert summary["unchanged_snapshots_windowed"] == 1
    assert [(row["price_cents"], row["reason"]) for row in _rows(log_text)] == [(400, REASON_SUPPORT_FLOOR)]
    assert [tick.price_cents for tick, _ in decided] == [1000, 1010, 990, 1005, 995, 400, 1000]
    last_decision = decided[-1][1]
    assert last_decision.features["window_size"] == 8  # five prices, the 400, its hourly repeat, the 1000


def repeated_snapshot_stream() -> tuple[list, BaselineSnapshot]:
    """
    Two quiet items whose lowest asks repeat for hours, then listings below them. The second starts with
    repeats, so its window fills from every poll first; after that both are windowed once an hour.
    """
    other_item = "AWP | Safari Mesh (Field-Tested)"
    item_prices = [1000, 1010, 990, 1005] + [1000] * 40 + [1020] * 15
    other_prices = [2000] * 5 + [2010, 1990] + [2000] * 37 + [2020] * 15
    events = [_snapshot(index * 305, price) for index, price in enumerate(item_prices)]
    events += [_snapshot(index * 305, price, other_item) for index, price in enumerate(other_prices)]
    events += [
        _feed(18_100, "listed", _sale(1, 900)),
        _feed(18_101, "listed", _sale(2, 400)),
        _feed(18_102, "listed", _sale(3, 1500, other_item)),
    ]
    baselines = BaselineSnapshot(
        baselines={
            ITEM: {
                "support_floor_cents": 500,
                "latest_price_cents": 1000,
                "rolling_30d_avg_cents": 1000,
                "volatility_cents": 50,
            },
            other_item: {"support_floor_cents": 1700, "latest_price_cents": 2000},
        },
    )
    return events, baselines


@pytest.mark.asyncio
async def test_warmup_events_fill_the_window_but_are_not_logged():
    events, baselines = synthetic_stream()

    log_text, decided = await _replay(events, baselines, log_from_ms=T0_MS + 1603 * 1000)

    assert [row["listing_id"] for row in _rows(log_text)] == ["3", "6", "5", "7"]
    assert len(decided) == 17  # every non-duplicate listing and snapshot was still decided


@pytest.mark.asyncio
async def test_recorded_pace_sleeps_between_events():
    sleep = AsyncMock()
    events = [_snapshot(0, 1000), _snapshot(0, 1001, "Other"), _snapshot(10, 1002)]

    await run_replay(events, ZScoreDreStrategy(), BaselineSnapshot(), io.StringIO(), source_label="t", speed=2.0, sleep=sleep)

    assert [call.args[0] for call in sleep.await_args_list] == [5.0]


@pytest.mark.asyncio
async def test_replay_is_byte_identical_across_runs():
    events = load_fixture(FIXTURE_DIR / "events.jsonl")
    baselines = BaselineSnapshot.load(FIXTURE_DIR / "baselines.json")

    first, _ = await _replay(events, baselines)
    second, _ = await _replay(events, baselines)

    assert first == second
    header = json.loads(first.splitlines()[0])
    assert header["strategy"] == "zscore_dre"
    assert header["baseline_sha256"] == baselines.sha256()
    assert header["config"]["z_score_threshold"] == -2.0
    assert header["config"]["state_key"] == "venue_and_item"


def _verdicts(log_text: str) -> list[tuple]:
    return [(row["seq"], row["listing_id"], row["approve"], row["reason"], row["score"]) for row in _rows(log_text)]


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", ["synthetic", "fixture"])
async def test_sweep_strategy_makes_the_live_decisions(stream):
    if stream == "synthetic":
        events, baselines = synthetic_stream()
    else:
        events = load_fixture(FIXTURE_DIR / "events.jsonl")
        baselines = BaselineSnapshot.load(FIXTURE_DIR / "baselines.json")

    live, _ = await _replay(events, baselines)
    sweep, _ = await _replay(events, baselines, strategy=STRATEGIES["zscore_dre_sweep"]())

    assert _verdicts(sweep) == _verdicts(live)
    assert json.loads(sweep.splitlines()[0])["strategy"] == "zscore_dre_sweep"


@pytest.mark.asyncio
async def test_sweep_strategy_records_sticker_count_and_the_dre_verdict_below_the_threshold():
    events = [_snapshot(index * 305, price) for index, price in enumerate([1000, 1010, 990, 1005, 995])]
    events += [
        # z is about -1.25: below threshold, but 2.5 volatilities under the 30 day average.
        _feed(1600, "listed", _sale(1, 950)),
        _feed(1601, "listed", _sale(2, 1020)),  # below threshold and no DRE rule applies
        _feed(1602, "listed", _sale(3, 400)),  # approved: no DRE verdict to add
    ]
    baseline = {"support_floor_cents": 500, "latest_price_cents": 1000, "rolling_30d_avg_cents": 1200, "volatility_cents": 100}
    baselines = BaselineSnapshot(baselines={ITEM: baseline})

    log_text, _ = await _replay(events, baselines, strategy=ZScoreDreSweepStrategy())

    rows = {row["listing_id"]: row for row in _rows(log_text)}
    assert rows["1"]["reason"] == REASON_BELOW_THRESHOLD
    assert rows["1"]["features"]["dre_reason"] == REASON_MACRO_SIGMA
    assert rows["2"]["features"]["dre_reason"] is None
    assert rows["3"]["approve"] is True
    assert "dre_reason" not in rows["3"]["features"]
    assert {row["features"]["sticker_count"] for row in rows.values()} == {0}


def fee_floor_stream() -> tuple[list, BaselineSnapshot]:
    """Listings under the support floor, two of them priced so that resale at 950 less the 8% fee (874) loses."""
    events = [_snapshot(index * 305, price) for index, price in enumerate([1000, 1010, 990, 1005, 995])]
    events += [
        _feed(1600, "listed", _sale(1, 990)),  # below threshold; the DRE approves, the floor would not
        _feed(1601, "listed", _sale(2, 900)),  # an outlier the DRE approves, but it nets -$0.26
        _feed(1602, "listed", _sale(3, 400)),  # an outlier that nets $4.74: approved
    ]
    baseline = {"support_floor_cents": 995, "latest_price_cents": 950}
    return events, BaselineSnapshot(baselines={ITEM: baseline})


@pytest.mark.asyncio
async def test_the_fee_floor_rejects_an_approval_that_nets_nothing(monkeypatch):
    monkeypatch.setattr(detection, "FEE_FLOOR", True)
    events, baselines = fee_floor_stream()

    log_text, _ = await _replay(events, baselines, strategy=ZScoreDreSweepStrategy())

    rows = {row["listing_id"]: row for row in _rows(log_text)}
    assert (rows["1"]["reason"], rows["1"]["features"]["dre_reason"]) == (REASON_BELOW_THRESHOLD, REASON_SUPPORT_FLOOR)
    assert rows["1"]["features"]["below_fee_floor"] is True
    assert (rows["2"]["reason"], rows["2"]["approve"]) == (REASON_BELOW_FEE_FLOOR, False)
    assert rows["2"]["features"]["estimated_net_profit_cents"] == -26
    assert rows["2"]["features"]["dre_rule"] == REASON_SUPPORT_FLOOR
    assert (rows["3"]["reason"], rows["3"]["approve"]) == (REASON_SUPPORT_FLOOR, True)
    assert json.loads(log_text.splitlines()[0])["config"]["fee_floor"] is True


@pytest.mark.asyncio
async def test_without_the_fee_floor_the_replay_approves_what_it_used_to(monkeypatch):
    monkeypatch.setattr(detection, "FEE_FLOOR", False)
    events, baselines = fee_floor_stream()

    log_text, _ = await _replay(events, baselines, strategy=ZScoreDreSweepStrategy())

    rows = {row["listing_id"]: row for row in _rows(log_text)}
    assert rows["1"]["features"]["below_fee_floor"] is False
    assert (rows["2"]["reason"], rows["2"]["approve"]) == (REASON_SUPPORT_FLOOR, True)
    assert json.loads(log_text.splitlines()[0])["config"]["fee_floor"] is False


@pytest.mark.asyncio
async def test_live_strategy_log_carries_no_sweep_inputs():
    events, baselines = synthetic_stream()

    log_text, _ = await _replay(events, baselines)

    assert not any("sticker_count" in row["features"] or "dre_reason" in row["features"] for row in _rows(log_text))


# --- Parity with the live consumer ---


class RecordingExecutor(ExecutionService):
    """Records every approval, before the buy-once check: the replay counts approvals, not purchases."""

    def __init__(self) -> None:
        super().__init__()
        self.trades: list[dict] = []

    async def execute(self, **trade) -> None:
        self.trades.append(trade)

    async def place_order(self, purchase) -> None:
        raise AssertionError("approvals are recorded in execute, never placed")


async def _live_run(monkeypatch, events, baselines) -> tuple[list[tuple], list[tuple]]:
    """Drive the real tick_consumer over the stream; return (DRE submissions, executed trades)."""
    from backtest.store import InMemoryEdgeStore

    store = InMemoryEdgeStore()
    store.load_baselines(baselines.baselines, baselines.sticker_prices)
    monkeypatch.setattr(listener_main.Redis, "from_url", lambda *_args, **_kwargs: store)

    async def discard_flush(source, buffer, *, batch_id, store, batch_pool, feed_event_buffer=None):
        buffer.clear()
        if feed_event_buffer is not None:
            feed_event_buffer.clear()

    monkeypatch.setattr(listener_main, "flush_batch_buffer", discard_flush)

    queue: asyncio.Queue = asyncio.Queue()
    for event in events:
        for item in expand_event(event):
            queue.put_nowait(item)
    queue.put_nowait(None)

    submissions: list[tuple] = []
    executor = RecordingExecutor()
    async with BoundedTaskPool("anomaly", workers=4, queue_size=128, shutdown_timeout=5) as pool:
        submit = pool.submit

        async def record_submit(job):
            tick, z_score = job.args[0], job.args[1]
            submissions.append((tick.market_hash_name, tick.listing_id, tick.price_cents, round(z_score, 6)))
            await submit(job)

        monkeypatch.setattr(pool, "submit", record_submit)
        await listener_main.tick_consumer(queue, "skinport", pool, AsyncMock(), AsyncMock(), executor)

    trades = [
        (
            trade["market_hash_name"],
            trade["listing_id"],
            trade["purchase_price_cents"],
            round(trade["z_score"], 6),
            trade["estimated_profit_cents"],
        )
        for trade in executor.trades
    ]
    return sorted(submissions, key=repr), sorted(trades, key=repr)


def _replay_run(decided) -> tuple[list[tuple], list[tuple]]:
    dre_reached = [
        (tick.market_hash_name, tick.listing_id, tick.price_cents, round(decision.score, 6))
        for tick, decision in decided
        if decision.approve or decision.reason in (REASON_DRE_REJECTED, REASON_BELOW_FEE_FLOOR)
    ]
    approved = [
        (
            tick.market_hash_name,
            tick.listing_id,
            tick.price_cents,
            round(decision.score, 6),
            decision.features["estimated_net_profit_cents"],
        )
        for tick, decision in decided
        if decision.approve
    ]
    return sorted(dre_reached, key=repr), sorted(approved, key=repr)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stream", "macro_triggers", "fee_floor", "expected_trades"),
    [
        # Cold, the recorded fixture's approvals are all macro Z scores: none without them, 18 with them, and one
        # of those 18 nets nothing after the seller fee.
        ("recorded_fixture", False, True, 0),
        ("recorded_fixture", True, True, 17),
        ("recorded_fixture", True, False, 18),
        ("synthetic", False, True, 3),
        ("repeated_snapshots", False, True, 3),
        ("cold_start", False, True, 1),
        ("cold_start", True, True, 3),
    ],
)
async def test_replay_decisions_match_the_live_consumer(monkeypatch, stream, macro_triggers, fee_floor, expected_trades):
    monkeypatch.setattr(zscore, "MACRO_ZSCORE_TRIGGERS", macro_triggers)
    monkeypatch.setattr(detection, "FEE_FLOOR", fee_floor)
    if stream == "recorded_fixture":
        events = load_fixture(FIXTURE_DIR / "events.jsonl")
        baselines = BaselineSnapshot.load(FIXTURE_DIR / "baselines.json")
    elif stream == "synthetic":
        events, baselines = synthetic_stream()
    elif stream == "repeated_snapshots":
        events, baselines = repeated_snapshot_stream()
    else:
        events, baselines = cold_start_stream()

    live_submissions, live_trades = await _live_run(monkeypatch, events, baselines)
    _, decided = await _replay(events, baselines)
    replay_submissions, replay_trades = _replay_run(decided)

    assert len(live_trades) == expected_trades
    assert replay_submissions == live_submissions
    assert replay_trades == live_trades


# --- Database source ---


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for row in self._rows:
            yield row


class FakeConnection:
    def __init__(self, rows_by_query):
        self._rows_by_query = rows_by_query

    async def stream(self, query, params):
        return FakeResult(self._rows_by_query[query.text])

    async def execute(self, query, params=None):
        return FakeResult(self._rows_by_query[query.text])


class FakeEngine:
    def __init__(self, rows_by_query):
        self._rows_by_query = rows_by_query

    @asynccontextmanager
    async def connect(self):
        yield FakeConnection(self._rows_by_query)


def _at(offset_s: float) -> datetime:
    return datetime(2026, 9, 24, 22, 0, 0) + timedelta(seconds=offset_s)


@pytest.mark.asyncio
async def test_database_stream_merges_feed_events_and_poll_stamped_snapshots():
    engine = FakeEngine(
        {
            database._FEED_EVENTS_QUERY.text: [(_at(2.5), {"eventType": "listed", "sales": []})],
            database._SNAPSHOTS_QUERY.text: [(_at(0), ITEM, 1000), (_at(4), "Other", 2000), (_at(400), ITEM, 1001)],
        }
    )

    events = [event async for event in database.stream_recorded_events(_at(0), _at(500), engine=engine)]

    start_ms = database._epoch_ms(_at(0))
    assert [(type(event).__name__, event.time_ms - start_ms) for event in events] == [
        ("RecordedSnapshot", 0),
        ("RecordedSnapshot", 0),  # same poll as the first row
        ("RecordedFeedEvent", 2500),
        ("RecordedSnapshot", 400_000),
    ]


@pytest.mark.asyncio
async def test_build_snapshot_matches_what_the_listener_loads():
    engine = FakeEngine(
        {
            database._BUILD_ROWS_QUERY.text: [
                (ITEM, 900, 1000, 1100, 110, 1.5, 2.5),
                (f"Sticker | {VALUABLE_STICKER}", 400_000, 500_000, 480_000, 4800, 0.0, 12.0),
            ]
        }
    )

    snapshot = await database.load_build_snapshot(7, _at(60).isoformat(), engine)

    assert snapshot.baselines[ITEM] == {
        "support_floor_cents": 900,
        "latest_price_cents": 1000,
        "rolling_30d_avg_cents": 1100,
        "volatility_cents": 110,
        "drift_percent": 1.5,
        "avg_volume_30d": 2.5,
        "coefficient_of_variation": 0.1,
    }
    # Keyed the way a listing names the applied sticker, as the backend serves it.
    assert snapshot.sticker_prices == {VALUABLE_STICKER: 500_000}
    assert snapshot.as_of == _at(60).isoformat()


@pytest.mark.asyncio
async def test_baseline_schedule_lists_builds_and_loads_them_on_demand():
    engine = FakeEngine(
        {
            database._BUILDS_QUERY.text: [(3, _at(0)), (4, _at(3600))],
            database._BUILD_ROWS_QUERY.text: [(ITEM, 900, 1000, 1100, 110, 1.5, 2.5)],
        }
    )

    schedule = await database.load_baseline_schedule(_at(0), _at(7200), engine=engine)

    assert [(build.build_id, build.built_at) for build in schedule.builds] == [
        (3, _at(0).isoformat()),
        (4, _at(3600).isoformat()),
    ]
    assert schedule.builds[1].effective_from_ms == database._epoch_ms(_at(3600))
    assert (await schedule.load(4)).as_of == _at(3600).isoformat()


def _schedule(*builds: tuple[int, int, BaselineSnapshot]) -> BaselineSchedule:
    """Builds as (build_id, offset_s from T0, snapshot)."""
    snapshots = {build_id: snapshot for build_id, _, snapshot in builds}

    async def load(build_id: int) -> BaselineSnapshot:
        return snapshots[build_id]

    return BaselineSchedule(
        builds=[
            ScheduledBuild(build_id=build_id, built_at=f"t+{offset_s}", effective_from_ms=T0_MS + offset_s * 1000)
            for build_id, offset_s, _ in builds
        ],
        load=load,
    )


def test_schedule_picks_the_newest_build_at_or_before_a_time():
    empty = BaselineSnapshot()
    schedule = _schedule((1, 100, empty), (2, 200, empty))

    assert schedule.active_index(T0_MS) == 0  # before any build: the first one (look-ahead)
    assert schedule.active_index(T0_MS + 200_000) == 1
    assert schedule.active_index(T0_MS + 150_000) == 0


@pytest.mark.asyncio
async def test_replay_switches_builds_as_time_passes_and_logs_each_switch():
    cheap_floor = BaselineSnapshot(baselines={ITEM: {"support_floor_cents": 500, "latest_price_cents": 1000}})
    high_floor = BaselineSnapshot(baselines={ITEM: {"support_floor_cents": 900, "latest_price_cents": 1000}})
    # Build 2 appears between two similar dips, so only the second one is under its higher floor.
    schedule = _schedule((1, 0, cheap_floor), (2, 1650, high_floor))
    history = [_snapshot(offset, 1000 + offset % 7) for offset in range(0, 1600, 60)]
    events = [*history, _feed(1601, "listed", _sale(1, 850)), _feed(1701, "listed", _sale(2, 845))]

    log_text, _ = await _replay(events, schedule)
    records = [json.loads(line) for line in log_text.splitlines()]

    header = records[0]
    assert (header["format"], header["baseline_mode"], header["baseline_builds"]) == (2, "schedule", [1, 2])
    switches = [record for record in records if record["type"] == "baseline"]
    assert [(record["build_id"], record["look_ahead"]) for record in switches] == [(1, False), (2, False)]
    assert switches[1]["from_ms"] == T0_MS + 1701 * 1000
    assert switches[1]["sha256"] == high_floor.sha256()
    decisions = {record["listing_id"]: record for record in records if record["type"] == "decision"}
    assert decisions["1"]["approve"] is False
    assert decisions["2"]["approve"] is True


@pytest.mark.asyncio
async def test_replay_before_the_first_build_marks_the_look_ahead():
    schedule = _schedule((1, 5000, BaselineSnapshot()))

    log_text, _ = await _replay([_snapshot(0, 1000)], schedule)

    switch = next(json.loads(line) for line in log_text.splitlines() if json.loads(line)["type"] == "baseline")
    assert switch["look_ahead"] is True


@pytest.mark.asyncio
async def test_replay_rejects_an_empty_schedule():
    with pytest.raises(ValueError, match="no builds"):
        await _replay([], _schedule())


def test_epoch_ms_is_exact_for_naive_utc():
    assert database._epoch_ms(datetime(2026, 9, 24, 21, 58, 53, 318000)) == 1_790_287_133_318


# --- Student features (#234): live, replay and training compute the same features ---


def _feature_streams():
    return {
        "synthetic": synthetic_stream(),
        "fixture": (load_fixture(FIXTURE_DIR / "events.jsonl"), BaselineSnapshot.load(FIXTURE_DIR / "baselines.json")),
    }


async def _replay_log(events, baselines, strategy) -> str:
    log = io.StringIO()
    await run_replay(events, strategy, baselines, log, source_label="test")
    return log.getvalue()


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", ["synthetic", "fixture"])
async def test_features_strategy_makes_the_live_decisions(stream):
    events, baselines = _feature_streams()[stream]

    live = await _replay_log(events, baselines, ZScoreDreStrategy())
    with_features = await _replay_log(events, baselines, STRATEGIES["zscore_dre_features"]())

    assert _verdicts(with_features) == _verdicts(live)
    header = json.loads(with_features.splitlines()[0])
    assert header["strategy"] == "zscore_dre_features"
    assert header["config"]["feature_version"] == FEATURE_VERSION


@pytest.mark.asyncio
async def test_every_scored_listing_carries_student_features_and_snapshots_none():
    events, baselines = synthetic_stream()

    rows = _rows(await _replay_log(events, baselines, ZScoreDreFeaturesStrategy()))

    scored_listings = [row for row in rows if row["listing_id"] is not None and row["reason"] != "duplicate"]
    assert scored_listings
    assert all(set(row["features"]["student"]) == set(FEATURE_NAMES) for row in scored_listings)
    assert all("student" not in row["features"] for row in rows if row["listing_id"] is None)
    # The listing of an item with no history and no baseline is still logged, with missing features.
    unknown = next(row for row in rows if row["market_hash_name"] == "Unknown Item")
    assert unknown["features"]["student"]["z_score"] is None
    assert unknown["features"]["student"]["price_to_30d_avg"] is None


@pytest.mark.asyncio
async def test_sticker_value_and_order_flow_features():
    events = [_snapshot(index * 305, price) for index, price in enumerate([1000, 1010, 990, 1005, 995])]
    events += [
        _feed(1600, "listed", _sale(1, 1000)),
        _feed(1900, "listed", _sale(2, 1500, stickers=[{"name": VALUABLE_STICKER}])),
        _feed(1600 + 3600, "listed", _sale(3, 1200)),
    ]
    baselines = BaselineSnapshot(
        baselines={ITEM: {"support_floor_cents": 500, "latest_price_cents": 1000}},
        sticker_prices={VALUABLE_STICKER: 500_000},
    )

    rows = {
        row["listing_id"]: row["features"]["student"]
        for row in _rows(await _replay_log(events, baselines, ZScoreDreFeaturesStrategy()))
    }

    assert [rows[listing]["listings_per_hour_item"] for listing in ("1", "2", "3")] == [0, 1, 1]
    assert rows["1"]["sticker_value_ratio"] == 0
    assert rows["2"]["sticker_value_ratio"] == 500


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", ["synthetic", "fixture"])
async def test_live_replay_and_training_compute_the_same_features(monkeypatch, stream):
    events, baselines = _feature_streams()[stream]

    # Live: the real tick consumer, with the features computed from its own edge state right where it scores.
    live_features: dict[str, dict] = {}
    live_rates = ListingRateTracker()
    score_window = detection.score_window

    async def scoring_with_features(tick, cache):
        window_score = await score_window(tick, cache)
        if tick.listing_id is not None:
            live_features[tick.listing_id] = await listing_features(tick, window_score, cache, live_rates)
        return window_score

    monkeypatch.setattr(detection, "score_window", scoring_with_features)
    await _live_run(monkeypatch, events, baselines)
    monkeypatch.undo()

    # Replay: the decision log, as the training flow reads it back.
    rows = _rows(await _replay_log(events, baselines, ZScoreDreFeaturesStrategy()))
    logged = {row["listing_id"]: row["features"]["student"] for row in rows if "student" in row["features"]}

    assert logged
    assert set(logged) == set(live_features)
    for listing_id, features in logged.items():
        training_row = feature_vector(features)
        live_row = feature_vector(live_features[listing_id])
        for name, trained, live in zip(FEATURE_NAMES, training_row, live_row, strict=True):
            assert (math.isnan(trained) and math.isnan(live)) or trained == pytest.approx(live, abs=1e-6), (listing_id, name)
