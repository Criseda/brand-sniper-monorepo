import math

import pytest
from shared_utils.baselines import (
    MIN_DAILY_POINTS,
    VOLATILITY_FROM_DAILY_MEDIANS,
    VOLATILITY_FROM_SALES_SPREAD,
    SalesHistory,
    SalesWindow,
    applied_sticker_name,
    build_item_baseline,
    edge_baseline_meta_key,
    edge_baselines_key,
    edge_sticker_prices_key,
    latest_price_cents,
    sales_spread_volatility_cents,
)


def window(median=None, minimum=None, volume=0, maximum=None, avg=None):
    return {"min": minimum, "max": maximum, "avg": avg, "median": median, "volume": volume}


def entry(name="AK-47 | Redline (Field-Tested)", version=None, currency="USD", day=None, week=None, month=None, quarter=None):
    return {
        "market_hash_name": name,
        "version": version,
        "currency": currency,
        "last_24_hours": day or window(),
        "last_7_days": week or window(),
        "last_30_days": month or window(),
        "last_90_days": quarter or window(),
    }


# Skinport's sales history for AK-47 | Redline (Field-Tested) on 2026-09-26.
REDLINE = entry(
    day=window(27.39, 23.94, 14, 71.04, 31.03),
    week=window(28.21, 22.93, 80, 81.33, 32.4),
    month=window(29.29, 21.55, 321, 456.06, 41.78),
    quarter=window(31.26, 21.55, 966, 938.97, 46.95),
)


def test_edge_keys_are_per_venue():
    assert edge_baselines_key("skinport") == "baselines:skinport"
    assert edge_sticker_prices_key("skinport") == "sticker_prices:skinport"
    assert edge_baseline_meta_key("csfloat") == "baseline_meta:csfloat"


def test_applied_sticker_name_drops_the_item_prefix():
    assert applied_sticker_name("Sticker | Crown (Foil)") == "Crown (Foil)"
    assert applied_sticker_name("Sticker Slab | BIG (Gold) | Stockholm 2021") is None
    assert applied_sticker_name("AK-47 | Redline (Field-Tested)") is None


def test_sales_window_parses_dollars_to_cents_and_tolerates_bad_values():
    assert SalesWindow.from_skinport(window(29.29, 21.55, 321, 456.06, 41.78)) == SalesWindow(2155, 45606, 4178, 2929, 321)
    assert SalesWindow.from_skinport(None) == SalesWindow(None, None, None, None, 0)
    assert SalesWindow.from_skinport({"median": "12", "volume": -1}) == SalesWindow(None, None, None, None, 0)


def test_sales_history_uses_the_versioned_name_and_requires_usd():
    doppler = SalesHistory.from_skinport(entry(name="★ Karambit | Doppler (Factory New)", version="Phase 4"))
    assert doppler is not None
    assert doppler.market_hash_name == "★ Karambit | Doppler (Phase 4) (Factory New)"
    assert SalesHistory.from_skinport(entry(currency="EUR")) is None
    assert SalesHistory.from_skinport(entry(name="")) is None


def test_spread_volatility_follows_the_lowest_of_n_sales():
    expected = round((2929 - 2155) / math.sqrt(2 * math.log(321)))
    assert sales_spread_volatility_cents(2929, 2155, 321) == expected == 228
    # A minimum above the median (bad data) is not a negative spread.
    assert sales_spread_volatility_cents(100, 120, 10) == 0
    with pytest.raises(ValueError):
        sales_spread_volatility_cents(100, 90, 1)


def test_redline_baseline_ignores_the_pattern_premium_maximum():
    history = SalesHistory.from_skinport(REDLINE)
    assert history is not None

    baseline = build_item_baseline(history)

    assert baseline is not None
    assert baseline.latest_price_cents == 2739  # 24 hour median, 14 sales that day
    assert baseline.rolling_30d_avg_cents == 2929  # median, not the premium inflated mean of 41.78
    assert baseline.rolling_90d_avg_cents == 3126
    assert baseline.volatility_cents == 228
    assert baseline.support_floor_cents == round(2929 - 1.2816 * 228)
    assert baseline.avg_volume_30d == pytest.approx(10.7)
    assert baseline.drift_percent == pytest.approx((2929 - 3126) / 3126 * 100)
    assert baseline.volatility_method == VOLATILITY_FROM_SALES_SPREAD
    assert (baseline.median_24h_cents, baseline.volume_24h, baseline.min_30d_cents, baseline.volume_90d) == (
        2739,
        14,
        2155,
        966,
    )


def test_edge_payload_matches_the_shared_document_shape():
    history = SalesHistory.from_skinport(REDLINE)
    assert history is not None
    baseline = build_item_baseline(history)
    assert baseline is not None

    assert baseline.edge_payload() == {
        "support_floor_cents": baseline.support_floor_cents,
        "latest_price_cents": 2739,
        "rolling_30d_avg_cents": 2929,
        "volatility_cents": 228,
        "drift_percent": baseline.drift_percent,
        "coefficient_of_variation": round(228 / 2929, 4),
    }


@pytest.mark.parametrize(
    ("day", "week", "expected"),
    [
        (window(1.00, 0.90, 5), window(1.20, 0.90, 10), 100),  # enough sales today
        (window(1.00, 0.90, 4), window(1.20, 0.90, 3), 120),  # thin day: 7 day median
        (window(1.00, 0.90, 4), window(1.20, 0.90, 2), 150),  # thin week too: 30 day median
    ],
)
def test_latest_price_falls_back_by_sales_volume(day, week, expected):
    history = SalesHistory.from_skinport(entry(day=day, week=week, month=window(1.50, 0.90, 30)))
    assert history is not None
    assert latest_price_cents(history) == expected


def test_latest_price_without_a_30_day_median_is_an_error():
    history = SalesHistory.from_skinport(entry())
    assert history is not None
    with pytest.raises(ValueError):
        latest_price_cents(history)


def test_items_with_too_few_sales_get_no_baseline():
    thin = SalesHistory.from_skinport(entry(month=window(0.75, 0.72, 4), quarter=window(0.75, 0.72, 20)))
    unsold = SalesHistory.from_skinport(entry())
    assert thin is not None and unsold is not None
    assert build_item_baseline(thin) is None
    assert build_item_baseline(unsold) is None


def test_missing_90_day_median_falls_back_to_30_days():
    history = SalesHistory.from_skinport(entry(month=window(0.10, 0.09, 5)))
    assert history is not None
    baseline = build_item_baseline(history)
    assert baseline is not None
    assert baseline.rolling_90d_avg_cents == 10
    assert baseline.drift_percent == 0


def test_volatility_never_drops_below_one_percent_of_the_median():
    # Every sale at the same price: no spread at all.
    history = SalesHistory.from_skinport(entry(month=window(5.00, 5.00, 40)))
    assert history is not None
    baseline = build_item_baseline(history)
    assert baseline is not None
    assert baseline.volatility_cents == 5
    assert baseline.support_floor_cents == round(500 - 1.2816 * 5)


def test_enough_daily_medians_switch_to_their_spread_and_tenth_percentile():
    history = SalesHistory.from_skinport(REDLINE)
    assert history is not None
    daily = [2800 + 10 * offset for offset in range(MIN_DAILY_POINTS)]

    baseline = build_item_baseline(history, daily)

    assert baseline is not None
    assert baseline.volatility_method == VOLATILITY_FROM_DAILY_MEDIANS
    assert baseline.volatility_cents == 42  # stdev of 2800..2930 in steps of 10
    assert baseline.support_floor_cents == 2813  # 10th percentile, inclusive


def test_too_few_daily_medians_keep_the_sales_spread_estimate():
    history = SalesHistory.from_skinport(REDLINE)
    assert history is not None
    baseline = build_item_baseline(history, [2900] * (MIN_DAILY_POINTS - 1))
    assert baseline is not None
    assert baseline.volatility_method == VOLATILITY_FROM_SALES_SPREAD
