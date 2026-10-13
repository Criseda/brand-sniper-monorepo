import math

import pytest
from shared_utils import FEATURE_NAMES, FeatureInputs, ListingRateTracker, compute_features, feature_vector
from shared_utils.features import Z_SOURCE_CODES, float_tier_boundary_distance

BASELINE = {
    "support_floor_cents": 800,
    "latest_price_cents": 1000,
    "rolling_30d_avg_cents": 1250,
    "volatility_cents": 50,
    "coefficient_of_variation": 0.04,
    "avg_volume_30d": 12.5,
}


def _inputs(**overrides) -> FeatureInputs:
    values = {
        "price_cents": 900,
        "z_score": -2.5,
        "mean_cents": 1200.0,
        "z_source": "hybrid",
        "baseline": BASELINE,
        "float_value": 0.1612,
        "sticker_value_cents": 250,
        "listings_last_hour": 3,
    }
    values.update(overrides)
    return FeatureInputs(**values)


def test_every_feature_from_full_inputs():
    features = compute_features(_inputs())

    assert list(features) == list(FEATURE_NAMES)
    assert features["price_to_window_mean"] == pytest.approx(0.75)
    assert features["z_score"] == -2.5
    assert features["z_source"] == Z_SOURCE_CODES["hybrid"]
    assert features["price_to_30d_avg"] == pytest.approx(0.72)
    assert features["price_to_support_floor"] == pytest.approx(1.125)
    assert features["macro_cv"] == 0.04
    assert features["avg_volume_30d"] == 12.5
    assert features["float_value"] == 0.1612
    assert features["float_tier_boundary_distance"] == pytest.approx(0.0112)
    assert features["sticker_value_ratio"] == pytest.approx(0.25)
    assert features["listings_per_hour_item"] == 3


def test_missing_inputs_give_missing_features_not_zeros():
    features = compute_features(
        _inputs(z_score=None, mean_cents=None, z_source=None, baseline=None, float_value=None, sticker_value_cents=0)
    )

    missing = {name for name, value in features.items() if value is None}
    assert missing == set(FEATURE_NAMES) - {"listings_per_hour_item"}


def test_a_zero_baseline_price_is_missing_rather_than_a_division_error():
    features = compute_features(_inputs(baseline={**BASELINE, "support_floor_cents": 0, "latest_price_cents": 0}))

    assert features["price_to_support_floor"] is None
    assert features["sticker_value_ratio"] is None


@pytest.mark.parametrize(
    ("float_value", "expected"),
    [(0.0, 0.0), (0.069, 0.069), (0.07, 0.0), (0.1501, 0.0001), (0.38, 0.0), (0.4499, 0.0699), (0.99, 0.54)],
)
def test_float_distance_to_the_lower_bound_of_its_wear_tier(float_value, expected):
    assert float_tier_boundary_distance(float_value) == pytest.approx(expected)


def test_feature_vector_keeps_the_column_order_and_marks_missing_values_nan():
    features = compute_features(_inputs(float_value=None))

    vector = feature_vector(features)

    assert len(vector) == len(FEATURE_NAMES)
    assert vector[FEATURE_NAMES.index("z_score")] == -2.5
    assert math.isnan(vector[FEATURE_NAMES.index("float_value")])
    assert feature_vector({}, names=("a",))[0] != feature_vector({}, names=("a",))[0]  # NaN


def test_listing_rate_counts_the_items_listings_in_the_hour_before():
    rates = ListingRateTracker()

    assert rates.record("skinport", "A", 0) == 0
    assert rates.record("skinport", "A", 1800) == 1
    assert rates.record("skinport", "B", 1800) == 0  # Another item
    assert rates.record("waxpeer", "A", 1800) == 0  # Another venue
    assert rates.record("skinport", "A", 3599) == 2
    # The listing at 0 is an hour old now and drops out.
    assert rates.record("skinport", "A", 3600) == 2
