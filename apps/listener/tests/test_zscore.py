import importlib.util
from pathlib import Path

import pytest
from models import MarketTick


def _load_listener_main():
    """Import apps/listener/main.py as an isolated module to avoid conflicts."""
    module_path = Path(__file__).resolve().parent.parent / "main.py"
    spec = importlib.util.spec_from_file_location("listener_main_under_test", module_path)
    mod = importlib.util.module_from_spec(spec)
    # Prevent accidental sys.modules pollution
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def listener_main():
    """Load the real listener main module once per module and pin its constants."""
    mod = _load_listener_main()
    mod.MIN_HISTORY_POINTS = 4
    mod.MIN_STD_DEV_FACTOR = 0.04
    mod.Z_SCORE_THRESHOLD = -2.5
    mod.Z_SCORE_STICKER_THRESHOLD = -1.0
    mod.MIN_SAVINGS_CENTS = 75
    mod.MACRO_ZSCORE_FALLBACK = True
    mod.MACRO_PRIOR_WEIGHT = 5.0
    return mod


def _tick(price_usd: float, stickers: list[dict] | None = None) -> MarketTick:
    return MarketTick(market_hash_name="Test Item", price_usd=price_usd, stickers=stickers or [])


# ============================================================================
# calculate_z_score  —  Layer 1 (macro fallback), Layer 2 (Bayesian hybrid)
# ============================================================================


def test_returns_none_when_few_prices_and_no_macro(listener_main):
    prices = [100, 102]  # 2 < MIN_HISTORY_POINTS = 4
    result = listener_main.calculate_z_score(prices)
    assert result is None


def test_macro_fallback_when_few_prices(listener_main):
    prices = [100, 102]
    result = listener_main.calculate_z_score(prices, macro_rolling_avg_cents=101, macro_volatility_cents=5, macro_cv=0.05)
    assert result is not None
    z_score, mean_cents, source = result
    # current=102, mean=101, min_vol=max(5, 101*0.01)=5  =>  z=(102-101)/5 = 0.2
    assert z_score == pytest.approx(0.2, abs=1e-9)
    assert mean_cents == 101.0
    assert source == "macro"


def test_macro_fallback_uses_min_vol_floor(listener_main):
    prices = [100]
    result = listener_main.calculate_z_score(prices, macro_rolling_avg_cents=10000, macro_volatility_cents=1, macro_cv=0.0001)
    assert result is not None
    z_score, mean_cents, source = result
    # current=100, mean=10000, min_vol=max(1, 10000*0.01)=100  =>  z=(100-10000)/100 = -99
    assert z_score == pytest.approx(-99.0, abs=1e-9)
    assert source == "macro"


def test_local_z_score_no_macro_data(listener_main):
    prices = [100, 102, 101, 99, 103]
    result = listener_main.calculate_z_score(prices)
    assert result is not None
    z_score, mean_cents, source = result
    # historical = [100, 102, 101, 99],  current = 103
    # mean = 100.5,  variance = 5/3 ≈ 1.667,  std = 1.291
    # min_std = 100.5 * 0.04 = 4.02
    # effective_std = max(1.291, 4.02) = 4.02
    # z = (103 - 100.5) / 4.02 ≈ 0.622
    assert mean_cents == pytest.approx(100.5, abs=0.01)
    assert z_score == pytest.approx(0.622, abs=0.01)
    assert source == "local"


def test_hybrid_z_score_with_macro_prior(listener_main):
    prices = [100, 102, 101, 99, 103]
    result = listener_main.calculate_z_score(prices, macro_rolling_avg_cents=100, macro_volatility_cents=5, macro_cv=0.05)
    assert result is not None
    z_score, mean_cents, source = result
    # historical = [100, 102, 101, 99],  current = 103,  n = 4
    # mean = 100.5,  variance = 1.667,  local_std = 1.291
    # macro_std_estimate = 100.5 * 0.05 = 5.025
    # blended = (4 * 1.667 + 5.0 * 5.025²) / (4 + 5.0) = 132.92 / 9 = 14.769
    # effective_std = sqrt(14.769) ≈ 3.843
    # z = (103 - 100.5) / 3.843 ≈ 0.651
    assert mean_cents == pytest.approx(100.5, abs=0.01)
    assert z_score == pytest.approx(0.651, abs=0.01)
    assert source == "hybrid"


def test_local_fallback_when_macro_cv_missing(listener_main):
    prices = [100, 102, 101, 99, 103]
    # macro_avg and macro_vol present but macro_cv is None
    result = listener_main.calculate_z_score(prices, macro_rolling_avg_cents=100, macro_volatility_cents=5, macro_cv=None)
    assert result is not None
    _, _, source = result
    assert source == "local"


def test_identical_prices_have_zero_z_score(listener_main):
    prices = [100, 100, 100, 100, 100]
    result = listener_main.calculate_z_score(prices)
    assert result is not None
    z_score, mean_cents, source = result
    # variance = 0, min_std = 100 * 0.04 = 4, z = (100-100)/4 = 0
    assert mean_cents == 100.0
    assert z_score == pytest.approx(0.0, abs=1e-9)
    assert source == "local"


def test_zero_variance_still_returns_zscore_via_min_std(listener_main):
    prices = [500, 500, 500, 500, 400]
    result = listener_main.calculate_z_score(prices)
    assert result is not None
    z_score, mean_cents, source = result
    # historical = [500, 500, 500, 500],  current = 400,  mean = 500
    # variance = 0,  min_std = 500 * 0.04 = 20
    # z = (400 - 500) / 20 = -5.0
    assert mean_cents == 500.0
    assert z_score == pytest.approx(-5.0, abs=0.01)
    assert source == "local"


# ============================================================================
# should_trigger_anomaly
# ============================================================================


def test_triggers_when_z_below_threshold_no_stickers(listener_main):
    tick = _tick(5.00)  # $5, price_cents = 500
    assert listener_main.should_trigger_anomaly(-3.0, 600, tick) is True


def test_does_not_trigger_when_z_above_threshold(listener_main):
    tick = _tick(5.00)
    assert listener_main.should_trigger_anomaly(-1.5, 600, tick) is False


def test_triggers_with_sticker_relaxed_threshold(listener_main):
    tick = _tick(5.00, stickers=[{"name": "Titan"}])
    # With stickers, threshold is -1.0 (relaxed), so -1.5 is below it -> triggers
    assert listener_main.should_trigger_anomaly(-1.5, 600, tick) is True


def test_rejects_when_savings_below_floor_no_stickers(listener_main):
    tick = _tick(5.80)  # price_cents = 580
    # mean = 600, savings = 20 < MIN_SAVINGS_CENTS = 75
    assert listener_main.should_trigger_anomaly(-3.0, 600, tick) is False


def test_no_savings_floor_for_stickered_items(listener_main):
    tick = _tick(5.80, stickers=[{"name": "Titan"}])
    # Even with tiny savings, stickers skip the floor check
    assert listener_main.should_trigger_anomaly(-3.0, 600, tick) is True


def test_source_param_does_not_alter_outcome(listener_main):
    tick = _tick(5.00)
    assert listener_main.should_trigger_anomaly(-3.0, 600, tick, source="local") is True
    assert listener_main.should_trigger_anomaly(-3.0, 600, tick, source="hybrid") is True
    assert listener_main.should_trigger_anomaly(-3.0, 600, tick, source="macro") is True
    assert listener_main.should_trigger_anomaly(-1.0, 600, tick, source="local") is False
    assert listener_main.should_trigger_anomaly(-1.0, 600, tick, source="macro") is False
