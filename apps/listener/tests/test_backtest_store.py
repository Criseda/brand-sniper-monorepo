import json

import pytest
from backtest.store import InMemoryEdgeStore


@pytest.mark.asyncio
async def test_sorted_set_orders_by_score_then_member_bytes():
    store = InMemoryEdgeStore()
    await store.zadd("window", {"b": 2, "a": 2, "c": 1})

    assert await store.zrange("window", 0, -1) == ["c", "a", "b"]


@pytest.mark.asyncio
async def test_zadd_counts_only_new_members_and_updates_scores():
    store = InMemoryEdgeStore()

    assert await store.zadd("window", {"a": 1, "b": 2}) == 2
    assert await store.zadd("window", {"a": 3}) == 0
    assert await store.zrange("window", 0, -1) == ["b", "a"]
    assert await store.zcard("window") == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("start", "stop", "expected"),
    [
        (0, -1, ["m0", "m1", "m2", "m3"]),
        (1, 2, ["m1", "m2"]),
        (-2, -1, ["m2", "m3"]),
        (2, 99, ["m2", "m3"]),
        (-99, 0, ["m0"]),
        (3, 1, []),
    ],
)
async def test_zrange_resolves_redis_rank_bounds(start, stop, expected):
    store = InMemoryEdgeStore()
    await store.zadd("window", {f"m{index}": index for index in range(4)})

    assert await store.zrange("window", start, stop) == expected


@pytest.mark.asyncio
async def test_zremrangebyrank_trims_the_lowest_ranks():
    store = InMemoryEdgeStore()
    await store.zadd("window", {f"m{index}": index for index in range(5)})

    assert await store.zremrangebyrank("window", 0, 1) == 2
    assert await store.zrange("window", 0, -1) == ["m2", "m3", "m4"]


@pytest.mark.asyncio
async def test_removing_every_member_deletes_the_key():
    store = InMemoryEdgeStore()
    await store.zadd("window", {"a": 1})

    assert await store.zremrangebyrank("window", 0, -1) == 1
    assert await store.zcard("window") == 0
    assert await store.zrange("window", 0, -1) == []


@pytest.mark.asyncio
async def test_loaded_baselines_and_sticker_prices_read_like_the_edge_redis():
    store = InMemoryEdgeStore()
    store.load_baselines({"Item": {"latest_price_cents": 1500}}, {"Sticker": 12000})

    assert json.loads(await store.hget("baselines:skinport", "Item")) == {"latest_price_cents": 1500}
    assert await store.hget("baselines:skinport", "Other") is None
    assert await store.hget("sticker_prices:skinport", "Sticker") == "12000"
    assert await store.hget("sticker_prices:skinport", "Other") is None

    # A new build replaces the old one: items it no longer has are gone.
    store.load_baselines({"Newer": {"latest_price_cents": 1}}, {})
    assert await store.hget("baselines:skinport", "Item") is None
    assert await store.hget("sticker_prices:skinport", "Sticker") is None
    assert await store.set("key", "value") is True
    assert await store.get("key") == "value"
    await store.aclose()
