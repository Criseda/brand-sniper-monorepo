from collections.abc import AsyncIterator
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import detection
import pytest
from backtest.store import InMemoryEdgeStore
from listener_telemetry import (
    anomalies_confirmed_total,
    anomalies_detected_total,
    anomalies_rejected_total,
    dedup_cache_evictions_total,
    dedup_cache_size,
)
from models import MarketTick, TickKind
from redis.asyncio import Redis

ITEM = "AK-47 | Slate (Field-Tested)"


def _tick(price_usd: float, timestamp: int = 1_790_000_000, venue: str = "skinport", **fields) -> MarketTick:
    kind = TickKind.of_feed_event(fields["event_type"]) if "event_type" in fields else TickKind.REST_SNAPSHOT
    return MarketTick(venue=venue, kind=kind, market_hash_name=ITEM, price_usd=price_usd, timestamp=timestamp, **fields)


def _store_with_baseline(**baseline) -> InMemoryEdgeStore:
    store = InMemoryEdgeStore()
    if baseline:
        store.load_baselines({ITEM: baseline}, {})
    return store


async def _fill_window(store: InMemoryEdgeStore, prices_cents: list[int], start: int = 1_790_000_000) -> None:
    for offset, price_cents in enumerate(prices_cents):
        await detection.push_to_window(_tick(price_cents / 100, timestamp=start + offset), cast(Redis, store))


@pytest.mark.asyncio
async def test_window_keeps_only_the_newest_prices():
    store = InMemoryEdgeStore()
    total = detection.SLIDING_WINDOW_SIZE + 5
    await _fill_window(store, [1000 + index for index in range(total)])

    members = await store.zrange(detection.price_window_key("skinport", ITEM), 0, -1)

    assert len(members) == detection.SLIDING_WINDOW_SIZE
    assert [int(member.split(":")[1]) for member in members] == list(range(1005, 1000 + total))


@pytest.mark.asyncio
async def test_outlier_is_submitted_to_the_dre():
    store = InMemoryEdgeStore()
    await _fill_window(store, [1000, 1010, 990, 1005, 995])
    pool = MagicMock()
    pool.submit = AsyncMock()
    outlier = _tick(5.00, timestamp=1_790_000_100)

    await detection.update_window_and_detect(outlier, store, pool, AsyncMock())

    job = pool.submit.await_args.args[0]
    assert job.func is detection.evaluate_and_execute
    assert job.args[0] is outlier
    assert job.args[1] < -2.0  # z-score
    assert job.args[5] == "local"


@pytest.mark.asyncio
async def test_flagged_outlier_is_counted_by_source_and_tick_kind():
    store = InMemoryEdgeStore()
    await _fill_window(store, [1000, 1010, 990, 1005, 995])
    pool = MagicMock()
    pool.submit = AsyncMock()
    counter = anomalies_detected_total.labels(source="local", tick_kind="listing")
    before = counter._value.get()

    listing = _tick(5.00, timestamp=1_790_000_100, event_type="listed", listing_id="1")
    await detection.update_window_and_detect(listing, store, pool, AsyncMock())

    assert counter._value.get() == before + 1


@pytest.mark.asyncio
async def test_price_inside_the_band_is_not_submitted():
    store = InMemoryEdgeStore()
    await _fill_window(store, [1000, 1010, 990, 1005, 995])
    pool = MagicMock()
    pool.submit = AsyncMock()

    await detection.update_window_and_detect(_tick(10.00, timestamp=1_790_000_100), store, pool, AsyncMock())

    pool.submit.assert_not_awaited()


@pytest.mark.asyncio
async def test_score_window_needs_history_without_a_baseline():
    store = InMemoryEdgeStore()
    await _fill_window(store, [1000, 1010])

    assert await detection.score_window(_tick(10.00), store) is None


@pytest.mark.asyncio
async def test_score_window_falls_back_to_the_macro_baseline():
    store = _store_with_baseline(rolling_30d_avg_cents=2000, volatility_cents=100, coefficient_of_variation=0.05)
    await _fill_window(store, [1500])

    score = await detection.score_window(_tick(15.00), store)

    assert score is not None
    assert (score.z_score, score.source, score.window_size) == (-5.0, "macro", 1)
    assert score.baseline == {"rolling_30d_avg_cents": 2000, "volatility_cents": 100, "coefficient_of_variation": 0.05}


def _listed(price_usd: float, timestamp: int = 1_790_000_000, listing_id: str = "1") -> MarketTick:
    return _tick(price_usd, timestamp=timestamp, event_type="listed", listing_id=listing_id)


def test_duplicate_is_the_same_price_inside_the_dedup_window():
    cache = detection.DedupCache()
    first = _listed(10.00)
    detection.update_dedup_cache(first, cache)

    assert detection.is_duplicate(_listed(10.00, timestamp=first.timestamp + detection.DEDUP_WINDOW_SECONDS - 1), cache)
    assert not detection.is_duplicate(_listed(10.00, timestamp=first.timestamp + detection.DEDUP_WINDOW_SECONDS), cache)
    assert not detection.is_duplicate(_listed(10.01, timestamp=first.timestamp + 1), cache)


def test_unchanged_rest_snapshot_is_recognised_however_old():
    cache = detection.DedupCache()
    first = _tick(10.00)
    detection.update_dedup_cache(first, cache)

    # Polls are 305 seconds or more apart, longer than the dedup window, so these are not duplicates.
    for poll in range(1, 4):
        repeat = _tick(10.00, timestamp=first.timestamp + poll * 305)
        assert not detection.is_duplicate(repeat, cache)
        assert detection.is_unchanged_snapshot(repeat, cache)
        detection.update_dedup_cache(repeat, cache)
    assert detection.is_unchanged_snapshot(_tick(10.00, timestamp=first.timestamp + 86_400), cache)


def test_rest_snapshot_at_a_new_price_is_not_unchanged():
    cache = detection.DedupCache()
    assert not detection.is_unchanged_snapshot(_tick(10.00), cache)
    detection.update_dedup_cache(_tick(10.00), cache)

    changed = _tick(9.50, timestamp=1_790_000_305)
    assert not detection.is_unchanged_snapshot(changed, cache)
    detection.update_dedup_cache(changed, cache)

    # Back to the earlier price: the lowest ask changed again, so it is scored again.
    assert not detection.is_unchanged_snapshot(_tick(10.00, timestamp=1_790_000_610), cache)


def test_listing_is_never_an_unchanged_snapshot_and_keeps_the_snapshot_price():
    cache = detection.DedupCache()
    detection.update_dedup_cache(_tick(10.00), cache)
    detection.update_dedup_cache(_listed(12.00, timestamp=1_790_000_100), cache)

    entry = cache.get(_tick(10.00))
    assert entry is not None and entry.snapshot_price_cents == 1000
    assert detection.is_unchanged_snapshot(_tick(10.00, timestamp=1_790_000_305), cache)
    assert not detection.is_unchanged_snapshot(_listed(10.00, timestamp=1_790_000_500, listing_id="2"), cache)


def test_dedup_cache_evicts_the_least_recently_used_item(monkeypatch):
    monkeypatch.setattr(detection, "DEDUP_CACHE_MAX_SIZE", 2)
    cache = detection.DedupCache()
    for name in ("a", "b", "a", "c"):
        tick = MarketTick(venue="skinport", kind=TickKind.REST_SNAPSHOT, market_hash_name=name, price_usd=1.0, timestamp=1)
        detection.update_dedup_cache(tick, cache)

    assert cache.items("skinport") == ["a", "c"]


def _named(name: str, venue: str) -> MarketTick:
    return MarketTick(venue=venue, kind=TickKind.REST_SNAPSHOT, market_hash_name=name, price_usd=1.0, timestamp=1)


def test_each_venue_has_its_own_dedup_cap_and_counts_its_evictions(monkeypatch):
    monkeypatch.setattr(detection, "DEDUP_CACHE_MAX_SIZE", 2)
    evictions = dedup_cache_evictions_total.labels(venue="csfloat")
    before = evictions._value.get()
    cache = detection.DedupCache()
    for name in ("a", "b"):
        detection.update_dedup_cache(_named(name, "skinport"), cache)
    for name in ("a", "b", "c", "d"):
        detection.update_dedup_cache(_named(name, "csfloat"), cache)

    # The second venue filling its cache never pushes out the first venue's items.
    assert cache.items("skinport") == ["a", "b"]
    assert cache.items("csfloat") == ["c", "d"]
    assert evictions._value.get() == before + 2
    assert dedup_cache_size.labels(venue="csfloat")._value.get() == 2
    assert cache.size("waxpeer") == 0 and cache.items("waxpeer") == []


def test_two_venues_keep_separate_dedup_state():
    cache = detection.DedupCache()
    detection.update_dedup_cache(_tick(10.00, venue="skinport"), cache)

    # The same item at the same price on another venue is neither a duplicate nor an unchanged snapshot.
    other_venue = _tick(10.00, timestamp=1_790_000_001, venue="csfloat")
    assert not detection.is_duplicate(other_venue, cache)
    assert not detection.is_unchanged_snapshot(other_venue, cache)
    detection.update_dedup_cache(other_venue, cache)

    # Each venue's snapshot rule compares with its own previous lowest ask.
    detection.update_dedup_cache(_tick(12.00, timestamp=1_790_000_305, venue="csfloat"), cache)
    assert detection.is_unchanged_snapshot(_tick(10.00, timestamp=1_790_000_610, venue="skinport"), cache)
    assert not detection.is_unchanged_snapshot(_tick(10.00, timestamp=1_790_000_610, venue="csfloat"), cache)


@pytest.mark.asyncio
async def test_two_venues_keep_separate_price_windows():
    store = InMemoryEdgeStore()
    await _fill_window(store, [1000, 1010, 990, 1005, 995])
    for offset, price_cents in enumerate([500, 505, 495, 500, 500]):
        await detection.push_to_window(_tick(price_cents / 100, timestamp=1_790_000_000 + offset, venue="csfloat"), store)

    skinport_score = await detection.score_window(_tick(10.00), store)
    csfloat_score = await detection.score_window(_tick(5.00, venue="csfloat"), store)

    # Each venue is scored against its own prices only (about $10 and about $5).
    assert skinport_score is not None and 990 < skinport_score.mean_cents < 1010
    assert csfloat_score is not None and 490 < csfloat_score.mean_cents < 510
    assert await store.zcard(detection.price_window_key("skinport", ITEM)) == 5
    assert await store.zcard(detection.price_window_key("csfloat", ITEM)) == 5


class _MigrationStore(InMemoryEdgeStore):
    """The edge store plus the key commands the legacy window migration uses."""

    async def scan_iter(self, match: str, count: int) -> AsyncIterator[bytes]:
        prefix = match.removesuffix("*")
        for key in [key for key in self._sorted_sets if key.startswith(prefix)]:
            yield key.encode("utf-8")

    async def renamenx(self, source: str, destination: str) -> bool:
        if destination in self._sorted_sets:
            return False
        self._sorted_sets[destination] = self._sorted_sets.pop(source)
        return True

    async def delete(self, key: str) -> int:
        return int(self._sorted_sets.pop(key, None) is not None)


@pytest.mark.asyncio
async def test_legacy_windows_move_to_skinport_keys_and_a_newer_window_wins():
    store = _MigrationStore()
    await store.zadd("market:ticks:AK-47 | Slate (Field-Tested)", {"1790000000:1000": 1_790_000_000})
    await store.zadd("market:ticks:Item: With Colon", {"1790000000:700": 1_790_000_000})
    await store.zadd("market:ticks:Newer", {"1790000000:300": 1_790_000_000})
    await store.zadd(detection.price_window_key("skinport", "Newer"), {"1790000600:310": 1_790_000_600})

    assert await detection.migrate_legacy_price_windows(cast(Redis, store)) == 3

    assert await store.zrange(detection.price_window_key("skinport", ITEM), 0, -1) == ["1790000000:1000"]
    assert await store.zrange(detection.price_window_key("skinport", "Item: With Colon"), 0, -1) == ["1790000000:700"]
    assert await store.zrange(detection.price_window_key("skinport", "Newer"), 0, -1) == ["1790000600:310"]
    assert await store.zcard("market:ticks:Newer") == 0
    # A second start finds nothing left to move.
    assert await detection.migrate_legacy_price_windows(cast(Redis, store)) == 0


@pytest.mark.asyncio
async def test_approved_trade_records_the_bought_listing(monkeypatch):
    monkeypatch.setattr(detection, "dre_approval_reason", AsyncMock(return_value="support_floor"))
    executor = AsyncMock()
    tick = MarketTick(
        venue="skinport",
        kind=TickKind.LISTED,
        market_hash_name="Item",
        price_usd=10.0,
        timestamp=1_790_000_000,
        event_type="listed",
        listing_id="60823173",
        float_value=0.46,
    )

    await detection.evaluate_and_execute(tick, -3.0, MagicMock(), executor, {"latest_price_cents": 1500})

    executor.execute.assert_awaited_once_with(
        market_hash_name="Item",
        purchase_price_cents=1000,
        # Resale at 1500 less the 8% Skinport seller fee (120), less the 1000 buy price.
        estimated_profit_cents=380,
        profit_estimate_basis="net_of_seller_fee",
        z_score=-3.0,
        listing_id="60823173",
        float_value=0.46,
    )


@pytest.mark.asyncio
async def test_approved_trade_without_baseline_price_records_no_estimate(monkeypatch):
    monkeypatch.setattr(detection, "dre_approval_reason", AsyncMock(return_value="support_floor"))
    executor = AsyncMock()

    await detection.evaluate_and_execute(_tick(10.0), -3.0, MagicMock(), executor, {"rolling_30d_avg_cents": 1500})

    assert executor.execute.await_args.kwargs["estimated_profit_cents"] is None


@pytest.mark.asyncio
async def test_approved_trade_fetches_the_baseline_when_none_was_passed(monkeypatch):
    monkeypatch.setattr(detection, "dre_approval_reason", AsyncMock(return_value="support_floor"))
    store = _store_with_baseline(latest_price_cents=1500)
    executor = AsyncMock()

    await detection.evaluate_and_execute(_tick(10.0), -3.0, store, executor)

    assert executor.execute.await_args.kwargs["estimated_profit_cents"] == 380


@pytest.mark.asyncio
async def test_rejected_opportunity_is_not_executed(monkeypatch):
    monkeypatch.setattr(detection, "dre_approval_reason", AsyncMock(return_value=None))
    executor = AsyncMock()

    await detection.evaluate_and_execute(_tick(10.0), -3.0, MagicMock(), executor, {"latest_price_cents": 1500})

    executor.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_approved_trade_on_venue_without_fee_schedule_fails_loudly(monkeypatch):
    from shared_utils import UnknownVenueError

    monkeypatch.setattr(detection, "dre_approval_reason", AsyncMock(return_value="support_floor"))
    executor = AsyncMock()
    tick = MarketTick(
        venue="unlisted-venue", kind=TickKind.REST_SNAPSHOT, market_hash_name="Item", price_usd=10.0, timestamp=1_790_000_000
    )

    with pytest.raises(UnknownVenueError):
        await detection.evaluate_and_execute(tick, -3.0, MagicMock(), executor, {"latest_price_cents": 1500})
    executor.execute.assert_not_awaited()


@pytest.mark.parametrize(
    ("fields", "expected"),
    [({}, "rest_snapshot"), ({"event_type": "listed", "listing_id": "1"}, "listing")],
)
def test_tick_kind_label_tells_rest_snapshots_from_listings(fields, expected):
    assert detection.tick_kind_label(_tick(10.0, **fields)) == expected


@pytest.mark.asyncio
async def test_approval_is_counted_and_logged_with_its_rule(monkeypatch, caplog):
    monkeypatch.setattr(detection, "dre_approval_reason", AsyncMock(return_value="macro_sigma"))
    counter = anomalies_confirmed_total.labels(source="macro", tick_kind="rest_snapshot", reason="macro_sigma")
    before = counter._value.get()

    with caplog.at_level("INFO", logger="listener.main"):
        await detection.evaluate_and_execute(_tick(10.0), -3.0, MagicMock(), AsyncMock(), {}, "macro")

    assert counter._value.get() == before + 1
    assert "(macro, rest_snapshot, rule macro_sigma)" in caplog.text


@pytest.mark.asyncio
async def test_rejection_is_counted_by_source_and_tick_kind(monkeypatch):
    monkeypatch.setattr(detection, "dre_approval_reason", AsyncMock(return_value=None))
    counter = anomalies_rejected_total.labels(source="hybrid", tick_kind="listing")
    before = counter._value.get()

    listing = _tick(10.0, event_type="listed", listing_id="1")
    await detection.evaluate_and_execute(listing, -3.0, MagicMock(), AsyncMock(), {}, "hybrid")

    assert counter._value.get() == before + 1


def test_every_anomaly_counter_series_exists_before_the_first_anomaly():
    detection.initialise_anomaly_counters()

    def label_values(counter, label: str) -> set[str]:
        return {sample.labels[label] for sample in counter.collect()[0].samples if sample.name.endswith("_total")}

    reasons = {"support_floor", "macro_sigma", "stickers_below_base", "sticker_premium"}
    assert label_values(anomalies_confirmed_total, "reason") == reasons
    assert label_values(anomalies_rejected_total, "tick_kind") == {"rest_snapshot", "listing"}
    assert label_values(anomalies_detected_total, "source") == {"local", "hybrid", "macro"}
