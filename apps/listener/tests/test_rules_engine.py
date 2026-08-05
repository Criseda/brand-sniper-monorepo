import json
from dataclasses import dataclass

import pytest
from rules_engine import evaluate_opportunity


# Mock classes for our input data
@dataclass
class MockMarketTick:
    market_hash_name: str
    price_cents: int
    stickers: list[dict[str, str]]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tick", "expected"),
    [
        # Item priced below support floor is immediately approved, regardless of stickers
        pytest.param(
            MockMarketTick(
                market_hash_name="AK-47 | Redline (Field-Tested)", price_cents=1000, stickers=[]
            ),  # $10.00 (below $15.00 floor)
            True,
            id="deep_discount_no_stickers",
        ),
        # Item priced above support floor with no stickers is rejected
        pytest.param(
            MockMarketTick(
                market_hash_name="AK-47 | Redline (Field-Tested)", price_cents=1700, stickers=[]
            ),  # $17.00 (above $15.00 floor)
            False,
            id="overpriced_no_stickers",
        ),
        # Item priced above floor, but with a highly valuable sticker at < 3% SP, is approved
        pytest.param(
            MockMarketTick(
                market_hash_name="AK-47 | Redline (Field-Tested)",
                price_cents=6600,  # $66.00 (Base $16 + $50 premium)
                stickers=[{"name": "Titan | Katowice 2014", "wear": "0.0"}],
            ),
            True,
            id="excellent_sticker_snipe",
        ),
        # A highly valuable sticker item priced too high (> 3% SP) is rejected
        pytest.param(
            MockMarketTick(
                market_hash_name="AK-47 | Redline (Field-Tested)",
                price_cents=116600,  # $1166.00 (Base $16 + $1150 premium)
                stickers=[{"name": "Titan | Katowice 2014", "wear": "0.0"}],
            ),
            False,
            id="bad_sticker_snipe",
        ),
        # Cheap stickers (total value < $100) do not trigger the sticker premium logic
        pytest.param(
            MockMarketTick(
                market_hash_name="AK-47 | Redline (Field-Tested)",
                price_cents=1700,  # $17.00
                stickers=[{"name": "Cheap Sticker", "wear": "0.0"}],
            ),
            False,
            id="cheap_sticker_ignored",
        ),
        # Safe handling when macro baseline is missing from Redis
        pytest.param(
            MockMarketTick(market_hash_name="AWP | Dragon Lore (Factory New)", price_cents=1000, stickers=[]),
            False,
            id="missing_baseline",
        ),
    ],
)
async def test_evaluate_opportunity(mock_redis, tick, expected):
    assert await evaluate_opportunity(tick, mock_redis) is expected


# ── Layer 3: Volatility-aware macro floor (illiquidity trap fix) ─────────────


@pytest.fixture
def mock_redis_with_volatility(mock_redis):
    baseline = {
        "support_floor_cents": 1500,
        "latest_price_cents": 1600,
        "rolling_30d_avg_cents": 1600,
        "volatility_cents": 50,
    }
    mock_redis.data["baseline:AK-47 | Redline (Field-Tested)"] = json.dumps(baseline)
    return mock_redis


@pytest.mark.asyncio
async def test_volatility_macro_floor_approves_2sigma_drop(mock_redis_with_volatility):
    """Price 2+ sigma below rolling 30d avg should be approved via Layer 3."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1200,  # 30d avg=1600, vol=50  =>  (1600-1200)/50 = 8.0 sigma
        stickers=[],
    )
    assert await evaluate_opportunity(tick, mock_redis_with_volatility) is True


@pytest.mark.asyncio
async def test_volatility_macro_floor_rejects_small_drop(mock_redis_with_volatility):
    """Price < 2 sigma below rolling 30d avg should NOT trigger Layer 3 alone."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1550,  # (1600-1550)/50 = 1.0 sigma  (< 2.0)
        stickers=[],
    )
    assert await evaluate_opportunity(tick, mock_redis_with_volatility) is False


@pytest.mark.asyncio
async def test_volatility_macro_floor_skipped_when_fields_missing(mock_redis):
    """Baseline without volatility fields must not crash — falls through to sticker logic."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1200,  # below support_floor (1500) so still approved
        stickers=[],
    )
    assert await evaluate_opportunity(tick, mock_redis) is True


# ── Optional baseline dict param ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_baseline_passed_directly_works(mock_redis):
    """Passing baseline dict directly should avoid a Redis fetch and return the same result."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1000,
        stickers=[],
    )
    baseline = {"support_floor_cents": 1500, "latest_price_cents": 1600}
    # Even without the baseline in Redis, passing it directly should work
    mock_redis.data.pop("baseline:AK-47 | Redline (Field-Tested)", None)
    assert await evaluate_opportunity(tick, mock_redis, baseline=baseline) is True


# ── Sticker premium edge paths ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_sticker_not_in_price_map_is_skipped(mock_redis):
    """Stickers whose price is missing from Redis add zero to the valuation."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1700,
        stickers=[{"name": "Mystery Sticker"}, {"name": "Another Mystery"}],
    )
    assert await evaluate_opportunity(tick, mock_redis) is False


@pytest.mark.asyncio
async def test_non_numeric_sticker_price_is_ignored(mock_redis):
    """A corrupt (non-numeric) sticker price must not crash the evaluation."""
    mock_redis.data["sticker_prices"]["Broken Sticker"] = "not-a-number"

    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1700,
        stickers=[{"name": "Broken Sticker"}],
    )
    assert await evaluate_opportunity(tick, mock_redis) is False


@pytest.mark.asyncio
async def test_free_stickers_below_base_price_approved(mock_redis):
    """Valuable stickers acquired at/below base price should be approved."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1501,  # above support floor (1500), below latest price (1600)
        stickers=[{"name": "Titan | Katowice 2014"}],
    )
    assert await evaluate_opportunity(tick, mock_redis) is True


@pytest.mark.asyncio
async def test_tick_without_stickers_attribute_is_safe(mock_redis):
    class BareTick:
        market_hash_name = "AK-47 | Redline (Field-Tested)"
        price_cents = 1700

    assert await evaluate_opportunity(BareTick(), mock_redis) is False
