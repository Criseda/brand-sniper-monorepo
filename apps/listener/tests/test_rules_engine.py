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


class MockRedis:
    def __init__(self):
        self.data = {}

    async def set(self, key, value):
        self.data[key] = value

    async def get(self, key):
        return self.data.get(key)

    async def hget(self, name, key):
        hash_data = self.data.get(name, {})
        return hash_data.get(key)

    async def hset(self, name, key, value):
        if name not in self.data:
            self.data[name] = {}
        self.data[name][key] = value


@pytest.fixture
def mock_redis():
    r = MockRedis()
    # Baseline for AK-47 Redline
    baseline = {"support_floor_cents": 1500, "latest_price_cents": 1600}
    r.data["baseline:AK-47 | Redline (Field-Tested)"] = json.dumps(baseline)

    # Sticker prices
    r.data["sticker_prices"] = {"Titan | Katowice 2014": "500000", "iBUYPOWER | Cologne 2014": "15000", "Cheap Sticker": "50"}
    return r


@pytest.mark.asyncio
async def test_deep_discount_no_stickers(mock_redis):
    """Test that an item priced below support floor is immediately approved, regardless of stickers."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1000,  # $10.00 (below $15.00 floor)
        stickers=[],
    )
    assert await evaluate_opportunity(tick, mock_redis) is True


@pytest.mark.asyncio
async def test_overpriced_no_stickers(mock_redis):
    """Test that an item priced above support floor with no stickers is rejected."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1700,  # $17.00 (above $15.00 floor)
        stickers=[],
    )
    assert await evaluate_opportunity(tick, mock_redis) is False


@pytest.mark.asyncio
async def test_excellent_sticker_snipe(mock_redis):
    """Test that an item priced above floor, but with a highly valuable sticker at < 3% SP, is approved."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=6600,  # $66.00 (Base $16 + $50 premium)
        stickers=[{"name": "Titan | Katowice 2014", "wear": "0.0"}],
    )
    assert await evaluate_opportunity(tick, mock_redis) is True


@pytest.mark.asyncio
async def test_bad_sticker_snipe(mock_redis):
    """Test that a highly valuable sticker item priced too high (> 3% SP) is rejected."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=116600,  # $1166.00 (Base $16 + $1150 premium)
        stickers=[{"name": "Titan | Katowice 2014", "wear": "0.0"}],
    )
    assert await evaluate_opportunity(tick, mock_redis) is False


@pytest.mark.asyncio
async def test_cheap_sticker_ignored(mock_redis):
    """Test that cheap stickers (total value < $100) do not trigger the sticker premium logic."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1700,  # $17.00
        stickers=[{"name": "Cheap Sticker", "wear": "0.0"}],
    )
    assert await evaluate_opportunity(tick, mock_redis) is False


@pytest.mark.asyncio
async def test_missing_baseline(mock_redis):
    """Test safe handling when macro baseline is missing from Redis."""
    tick = MockMarketTick(market_hash_name="AWP | Dragon Lore (Factory New)", price_cents=1000, stickers=[])
    assert await evaluate_opportunity(tick, mock_redis) is False
