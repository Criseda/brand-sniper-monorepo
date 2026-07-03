import json
import pytest
from dataclasses import dataclass
from typing import List, Dict, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rules_engine import evaluate_opportunity

# Mock classes for our input data
@dataclass
class MockMarketTick:
    market_hash_name: str
    price_cents: int
    stickers: List[Dict[str, str]]

class MockRedis:
    def __init__(self):
        self.data = {}
        
    def set(self, key, value):
        self.data[key] = value
        
    def get(self, key):
        return self.data.get(key)
        
    def hget(self, name, key):
        hash_data = self.data.get(name, {})
        return hash_data.get(key)
        
    def hset(self, name, key, value):
        if name not in self.data:
            self.data[name] = {}
        self.data[name][key] = value

@pytest.fixture
def mock_redis():
    r = MockRedis()
    # Baseline for AK-47 Redline
    baseline = {
        "support_floor_cents": 1500,
        "latest_price_cents": 1600
    }
    r.set("baseline:AK-47 | Redline (Field-Tested)", json.dumps(baseline))
    
    # Sticker prices
    r.hset("sticker_prices", "Titan | Katowice 2014", "500000") # $5000.00
    r.hset("sticker_prices", "iBUYPOWER | Cologne 2014", "15000") # $150.00
    r.hset("sticker_prices", "Cheap Sticker", "50") # $0.50
    return r

def test_deep_discount_no_stickers(mock_redis):
    """Test that an item priced below support floor is immediately approved, regardless of stickers."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1000, # $10.00 (below $15.00 floor)
        stickers=[]
    )
    assert evaluate_opportunity(tick, mock_redis) is True

def test_overpriced_no_stickers(mock_redis):
    """Test that an item priced above support floor with no stickers is rejected."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1700, # $17.00 (above $15.00 floor)
        stickers=[]
    )
    assert evaluate_opportunity(tick, mock_redis) is False

def test_excellent_sticker_snipe(mock_redis):
    """Test that an item priced above floor, but with a highly valuable sticker at < 3% SP, is approved."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=6600, # $66.00 (Base $16 + $50 premium)
        stickers=[{"name": "Titan | Katowice 2014", "wear": "0.0"}]
    )
    # Base price = 1600. Premium = 6600 - 1600 = 5000.
    # SP% = 5000 / 500000 = 1.0% (<= 3.0%)
    assert evaluate_opportunity(tick, mock_redis) is True

def test_bad_sticker_snipe(mock_redis):
    """Test that a highly valuable sticker item priced too high (> 3% SP) is rejected."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=116600, # $1166.00 (Base $16 + $1150 premium)
        stickers=[{"name": "Titan | Katowice 2014", "wear": "0.0"}]
    )
    # Base price = 1600. Premium = 116600 - 1600 = 115000.
    # SP% = 115000 / 500000 = 23.0% (> 3.0%)
    assert evaluate_opportunity(tick, mock_redis) is False

def test_cheap_sticker_ignored(mock_redis):
    """Test that cheap stickers (total value < $100) do not trigger the sticker premium logic."""
    tick = MockMarketTick(
        market_hash_name="AK-47 | Redline (Field-Tested)",
        price_cents=1700, # $17.00
        stickers=[{"name": "Cheap Sticker", "wear": "0.0"}]
    )
    # Base price = 1600. Premium = 100.
    # Total sticker value = 50 (< 10000 threshold). Should fallback to base discount check, which fails.
    assert evaluate_opportunity(tick, mock_redis) is False

def test_missing_baseline(mock_redis):
    """Test safe handling when macro baseline is missing from Redis."""
    tick = MockMarketTick(
        market_hash_name="AWP | Dragon Lore (Factory New)",
        price_cents=1000, 
        stickers=[]
    )
    assert evaluate_opportunity(tick, mock_redis) is False
