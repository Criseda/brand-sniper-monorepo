import pytest
import zscore
from models import MarketTick
from zscore import calculate_z_score, should_trigger_anomaly

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _pin_detection_constants(monkeypatch):
    """Pin tunable constants so detection tests are deterministic."""
    monkeypatch.setattr(zscore, "MIN_HISTORY_POINTS", 4)
    monkeypatch.setattr(zscore, "MIN_STD_DEV_FACTOR", 0.04)
    monkeypatch.setattr(zscore, "Z_SCORE_THRESHOLD", -2.0)
    monkeypatch.setattr(zscore, "Z_SCORE_STICKER_THRESHOLD", -1.0)
    monkeypatch.setattr(zscore, "MIN_SAVINGS_CENTS", 50)
    monkeypatch.setattr(zscore, "MACRO_ZSCORE_FALLBACK", True)
    monkeypatch.setattr(zscore, "MACRO_PRIOR_WEIGHT", 5.0)


def _tick(price_usd: float, stickers: list[dict] | None = None) -> MarketTick:
    return MarketTick(market_hash_name="Test Item", price_usd=price_usd, stickers=stickers or [])


# ============================================================================
# calculate_z_score  —  Layer 1 (macro fallback), Layer 2 (Bayesian hybrid)
# ============================================================================


@pytest.mark.parametrize(
    ("prices", "macro_avg", "macro_vol", "macro_cv", "expected"),
    [
        # 2 < MIN_HISTORY_POINTS = 4, no macro data -> None
        pytest.param([100, 102], None, None, None, None, id="returns_none_when_few_prices_and_no_macro"),
        # current=102, mean=101, min_vol=max(5, 101*0.01)=5  =>  z=(102-101)/5 = 0.2
        pytest.param(
            [100, 102], 101, 5, 0.05, (pytest.approx(0.2, abs=1e-9), 101.0, "macro"), id="macro_fallback_when_few_prices"
        ),
        # current=100, mean=10000, min_vol=max(1, 10000*0.01)=100  =>  z=(100-10000)/100 = -99
        pytest.param(
            [100], 10000, 1, 0.0001, (pytest.approx(-99.0, abs=1e-9), 10000.0, "macro"), id="macro_fallback_uses_min_vol_floor"
        ),
        # historical = [100, 102, 101, 99], current = 103
        # mean = 100.5, std = 1.291, min_std = 100.5 * 0.04 = 4.02
        # z = (103 - 100.5) / 4.02 ≈ 0.622
        pytest.param(
            [100, 102, 101, 99, 103],
            None,
            None,
            None,
            (pytest.approx(0.622, abs=0.01), pytest.approx(100.5, abs=0.01), "local"),
            id="local_z_score_no_macro_data",
        ),
        # historical = [100, 102, 101, 99], current = 103, n = 4
        # blended = (4 * 1.667 + 5.0 * 5.025²) / (4 + 5.0) = 14.769, std ≈ 3.843
        # z = (103 - 100.5) / 3.843 ≈ 0.651
        pytest.param(
            [100, 102, 101, 99, 103],
            100,
            5,
            0.05,
            (pytest.approx(0.651, abs=0.01), pytest.approx(100.5, abs=0.01), "hybrid"),
            id="hybrid_z_score_with_macro_prior",
        ),
        # macro_avg and macro_vol present but macro_cv is None -> local
        pytest.param([100, 102, 101, 99, 103], 100, 5, None, (None, None, "local"), id="local_fallback_when_macro_cv_missing"),
        # variance = 0, min_std = 100 * 0.04 = 4, z = (100-100)/4 = 0
        pytest.param(
            [100, 100, 100, 100, 100],
            None,
            None,
            None,
            (pytest.approx(0.0, abs=1e-9), 100.0, "local"),
            id="identical_prices_have_zero_z_score",
        ),
        # historical = [500, 500, 500, 500], current = 400, mean = 500
        # variance = 0, min_std = 500 * 0.04 = 20, z = (400 - 500) / 20 = -5.0
        pytest.param(
            [500, 500, 500, 500, 400],
            None,
            None,
            None,
            (pytest.approx(-5.0, abs=0.01), 500.0, "local"),
            id="zero_variance_still_returns_zscore_via_min_std",
        ),
    ],
)
def test_calculate_z_score(prices, macro_avg, macro_vol, macro_cv, expected):
    kwargs = {}
    if macro_avg is not None:
        kwargs["macro_rolling_avg_cents"] = macro_avg
    if macro_vol is not None:
        kwargs["macro_volatility_cents"] = macro_vol
    if macro_cv is not None:
        kwargs["macro_cv"] = macro_cv

    result = calculate_z_score(prices, **kwargs)

    if expected is None:
        assert result is None
        return
    assert result is not None
    z_score, mean_cents, source = result
    expected_z, expected_mean, expected_source = expected
    if expected_z is not None:
        assert z_score == expected_z
    if expected_mean is not None:
        assert mean_cents == expected_mean
    if expected_source is not None:
        assert source == expected_source


# ============================================================================
# should_trigger_anomaly
# ============================================================================


@pytest.mark.parametrize(
    ("price_usd", "z_score", "mean_cents", "stickers", "expected"),
    [
        # $5, price_cents = 500, no stickers
        pytest.param(5.00, -3.0, 600, None, True, id="triggers_when_z_below_threshold_no_stickers"),
        pytest.param(5.00, -1.5, 600, None, False, id="does_not_trigger_when_z_above_threshold"),
        # With stickers, threshold is -1.0 (relaxed), so -1.5 is below it -> triggers
        pytest.param(5.00, -1.5, 600, [{"name": "Titan"}], True, id="triggers_with_sticker_relaxed_threshold"),
        # price_cents = 580, mean = 600, savings = 20 < MIN_SAVINGS_CENTS = 50
        pytest.param(5.80, -3.0, 600, None, False, id="rejects_when_savings_below_floor_no_stickers"),
        # Even with tiny savings, stickers skip the floor check
        pytest.param(5.80, -3.0, 600, [{"name": "Titan"}], True, id="no_savings_floor_for_stickered_items"),
    ],
)
def test_should_trigger_anomaly(price_usd, z_score, mean_cents, stickers, expected):
    tick = _tick(price_usd, stickers)
    assert should_trigger_anomaly(z_score, mean_cents, tick) is expected


def test_source_param_does_not_alter_outcome():
    tick = _tick(5.00)
    assert should_trigger_anomaly(-3.0, 600, tick, source="local") is True
    assert should_trigger_anomaly(-3.0, 600, tick, source="hybrid") is True
    assert should_trigger_anomaly(-3.0, 600, tick, source="macro") is True
    assert should_trigger_anomaly(-1.0, 600, tick, source="local") is False
    assert should_trigger_anomaly(-1.0, 600, tick, source="macro") is False
