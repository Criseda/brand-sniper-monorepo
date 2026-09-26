from typing import cast
from unittest.mock import AsyncMock, MagicMock

import detection
import pytest
from backtest.store import InMemoryEdgeStore
from models import MarketTick
from redis.asyncio import Redis

ITEM = "AK-47 | Slate (Field-Tested)"


def _tick(price_usd: float, timestamp: int = 1_790_000_000, **fields) -> MarketTick:
    return MarketTick(venue="skinport", market_hash_name=ITEM, price_usd=price_usd, timestamp=timestamp, **fields)


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

    members = await store.zrange(f"market:ticks:{ITEM}", 0, -1)

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


def test_duplicate_is_the_same_price_inside_the_dedup_window():
    cache: detection.DedupCache = detection.OrderedDict()
    first = _tick(10.00)
    detection.update_dedup_cache(first, cache)

    assert detection.is_duplicate(_tick(10.00, timestamp=first.timestamp + detection.DEDUP_WINDOW_SECONDS - 1), cache)
    assert not detection.is_duplicate(_tick(10.00, timestamp=first.timestamp + detection.DEDUP_WINDOW_SECONDS), cache)
    assert not detection.is_duplicate(_tick(10.01, timestamp=first.timestamp + 1), cache)


def test_dedup_cache_evicts_the_least_recently_used_item(monkeypatch):
    monkeypatch.setattr(detection, "DEDUP_CACHE_MAX_SIZE", 2)
    cache: detection.DedupCache = detection.OrderedDict()
    for name in ("a", "b", "a", "c"):
        detection.update_dedup_cache(MarketTick(venue="skinport", market_hash_name=name, price_usd=1.0, timestamp=1), cache)

    assert list(cache) == ["a", "c"]


@pytest.mark.asyncio
async def test_approved_trade_records_the_bought_listing(monkeypatch):
    monkeypatch.setattr(detection, "evaluate_opportunity", AsyncMock(return_value=True))
    executor = AsyncMock()
    tick = MarketTick(
        venue="skinport",
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
    monkeypatch.setattr(detection, "evaluate_opportunity", AsyncMock(return_value=True))
    executor = AsyncMock()

    await detection.evaluate_and_execute(_tick(10.0), -3.0, MagicMock(), executor, {"rolling_30d_avg_cents": 1500})

    assert executor.execute.await_args.kwargs["estimated_profit_cents"] is None


@pytest.mark.asyncio
async def test_approved_trade_fetches_the_baseline_when_none_was_passed(monkeypatch):
    monkeypatch.setattr(detection, "evaluate_opportunity", AsyncMock(return_value=True))
    store = _store_with_baseline(latest_price_cents=1500)
    executor = AsyncMock()

    await detection.evaluate_and_execute(_tick(10.0), -3.0, store, executor)

    assert executor.execute.await_args.kwargs["estimated_profit_cents"] == 380


@pytest.mark.asyncio
async def test_rejected_opportunity_is_not_executed(monkeypatch):
    monkeypatch.setattr(detection, "evaluate_opportunity", AsyncMock(return_value=False))
    executor = AsyncMock()

    await detection.evaluate_and_execute(_tick(10.0), -3.0, MagicMock(), executor, {"latest_price_cents": 1500})

    executor.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_approved_trade_on_venue_without_fee_schedule_fails_loudly(monkeypatch):
    from shared_utils import UnknownVenueError

    monkeypatch.setattr(detection, "evaluate_opportunity", AsyncMock(return_value=True))
    executor = AsyncMock()
    tick = MarketTick(venue="unlisted-venue", market_hash_name="Item", price_usd=10.0, timestamp=1_790_000_000)

    with pytest.raises(UnknownVenueError):
        await detection.evaluate_and_execute(tick, -3.0, MagicMock(), executor, {"latest_price_cents": 1500})
    executor.execute.assert_not_awaited()
