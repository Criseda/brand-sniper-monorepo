import asyncio
import importlib.util
import io
import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from backtest import database
from backtest.harness import run_replay
from backtest.sources import (
    POLL_GAP_MS,
    BaselineSnapshot,
    PollClock,
    RecordedFeedEvent,
    RecordedSnapshot,
    expand_event,
    load_fixture,
    merge_ordered,
    sanitize_feed_payload,
    write_fixture,
)
from backtest.strategy import (
    REASON_BELOW_THRESHOLD,
    REASON_DRE_REJECTED,
    REASON_DUPLICATE,
    REASON_INSUFFICIENT_HISTORY,
    ZScoreDreStrategy,
)
from executor import ExecutionService
from models import FeedEvent, MarketTick
from rules_engine import REASON_STICKER_PREMIUM, REASON_STICKERS_BELOW_BASE, REASON_SUPPORT_FLOOR
from shared_utils import build_versioned_name
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
        _feed(1601, "listed", _sale(2, 1000)),  # duplicate of the previous price
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


async def _replay(events, baselines, **kwargs) -> tuple[str, list[tuple[MarketTick, object]]]:
    decided: list[tuple[MarketTick, object]] = []
    log = io.StringIO()
    await run_replay(
        events,
        ZScoreDreStrategy(),
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

    reasons = {row["listing_id"]: (row["reason"], row["approve"]) for row in _rows(log_text)}
    assert reasons == {
        "1": (REASON_BELOW_THRESHOLD, False),
        "2": (REASON_DUPLICATE, False),
        "3": (REASON_SUPPORT_FLOOR, True),
        "4": (REASON_DRE_REJECTED, False),
        "5": (REASON_STICKERS_BELOW_BASE, True),
        "6": (REASON_STICKER_PREMIUM, True),
        "7": (REASON_INSUFFICIENT_HISTORY, False),
    }
    [summary] = _rows(log_text, "summary")
    assert summary["outcome_ticks"] == 1
    assert summary["feed_events"] == 8


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
async def test_warmup_events_fill_the_window_but_are_not_logged():
    events, baselines = synthetic_stream()

    log_text, decided = await _replay(events, baselines, log_from_ms=T0_MS + 1603 * 1000)

    assert [row["listing_id"] for row in _rows(log_text)] == ["3", "6", "5", "7"]
    assert len(decided) == 16  # every non-duplicate listing and snapshot was still decided


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


# --- Parity with the live consumer ---


class RecordingExecutor(ExecutionService):
    def __init__(self) -> None:
        self.trades: list[dict] = []

    async def execute(self, **trade) -> None:
        self.trades.append(trade)


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
        if decision.approve or decision.reason == REASON_DRE_REJECTED
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
@pytest.mark.parametrize("stream", ["recorded_fixture", "synthetic"])
async def test_replay_decisions_match_the_live_consumer(monkeypatch, stream):
    if stream == "recorded_fixture":
        events = load_fixture(FIXTURE_DIR / "events.jsonl")
        baselines = BaselineSnapshot.load(FIXTURE_DIR / "baselines.json")
    else:
        events, baselines = synthetic_stream()

    live_submissions, live_trades = await _live_run(monkeypatch, events, baselines)
    _, decided = await _replay(events, baselines)
    replay_submissions, replay_trades = _replay_run(decided)

    assert live_trades, "the parity stream must produce trades, or the test proves nothing"
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

    async def execute(self, query):
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
async def test_current_baselines_match_the_edge_sync():
    engine = FakeEngine(
        {
            database._BASELINES_QUERY.text: [
                (ITEM, "Weapon", 900, 1000, 1100, 110, 1.5, _at(0)),
                (VALUABLE_STICKER, "Sticker", 400_000, 500_000, 480_000, 0, 0.0, _at(60)),
            ]
        }
    )

    snapshot = await database.load_current_baselines(engine)

    assert snapshot.baselines[ITEM] == {
        "support_floor_cents": 900,
        "latest_price_cents": 1000,
        "rolling_30d_avg_cents": 1100,
        "volatility_cents": 110,
        "drift_percent": 1.5,
        "coefficient_of_variation": 0.1,
    }
    assert snapshot.sticker_prices == {VALUABLE_STICKER: 500_000}
    assert snapshot.as_of == _at(60).isoformat()


def test_epoch_ms_is_exact_for_naive_utc():
    assert database._epoch_ms(datetime(2026, 9, 24, 21, 58, 53, 318000)) == 1_790_287_133_318
