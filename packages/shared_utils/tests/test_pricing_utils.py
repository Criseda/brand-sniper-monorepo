import pytest
from shared_utils.pricing_utils import detect_downtrend, resolve_recent_median, to_cents


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (10.50, 1050),
        (10.555, 1056),
        (0.01, 1),
        (None, None),
        (0.0, 0),
    ],
    ids=["basic", "rounding", "small_value", "none", "zero"],
)
def test_to_cents(value, expected):
    assert to_cents(value) == expected


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        pytest.param(
            {
                "last_24_hours": {"median": 12.50, "volume": 10},
                "last_7_days": {"median": 11.00, "volume": 50},
            },
            12.50,
            id="returns_24h_when_volume_exists",
        ),
        pytest.param(
            {
                "last_24_hours": {"median": 12.50, "volume": 0},
                "last_7_days": {"median": 11.00, "volume": 50},
            },
            11.00,
            id="falls_through_to_7d_when_24h_volume_zero",
        ),
        pytest.param(
            {
                "last_24_hours": {"median": 12.50, "volume": 0},
                "last_7_days": {"median": 11.00, "volume": 0},
                "last_30_days": {"median": 10.50, "volume": 200},
            },
            10.50,
            id="falls_through_to_30d",
        ),
        pytest.param(
            {"last_90_days": {"median": 9.00}},
            9.00,
            id="falls_through_to_90d_no_volume_check",
        ),
        pytest.param({}, None, id="empty_dict"),
        pytest.param({"last_24_hours": {}, "last_7_days": {}}, None, id="all_missing"),
        pytest.param({"unknown": "data"}, None, id="missing_keys_are_safe"),
    ],
)
def test_resolve_recent_median(entry, expected):
    assert resolve_recent_median(entry) == expected


@pytest.mark.parametrize(
    ("entry", "expected_detected", "expected_severity"),
    [
        pytest.param(
            {
                "last_7_days": {"median": 12.00, "volume": 50},
                "last_30_days": {"median": 10.00, "volume": 200},
            },
            False,
            0.0,
            id="no_downtrend",
        ),
        pytest.param(
            {
                "last_7_days": {"median": 9.00, "volume": 50},
                "last_30_days": {"median": 12.00, "volume": 200},
            },
            True,
            pytest.approx(0.25),  # (12-9)/12
            id="medium_term_downtrend",
        ),
        pytest.param(
            {
                "last_24_hours": {"median": 8.00, "volume": 10},
                "last_7_days": {"median": 10.00, "volume": 50},
            },
            True,
            pytest.approx(0.20),  # (10-8)/10
            id="short_term_panic",
        ),
        pytest.param(
            {
                "last_24_hours": {"median": 8.00, "volume": 10},
                "last_7_days": {"median": 9.00, "volume": 50},
                "last_30_days": {"median": 12.00, "volume": 200},
            },
            True,
            # medium-term: (12-9)/12 = 0.25, short-term: (9-8)/9 = 0.111...
            pytest.approx(0.3611, abs=0.001),
            id="combined_downtrend",
        ),
        pytest.param({}, False, 0.0, id="empty_dict"),
    ],
)
def test_detect_downtrend(entry, expected_detected, expected_severity):
    detected, severity = detect_downtrend(entry)
    assert detected is expected_detected
    assert severity == expected_severity


def test_to_cents_rejects_non_numeric():
    with pytest.raises(TypeError):
        to_cents("not-a-number")


@pytest.mark.parametrize("bad_entry", [None, "string", 42, ["list"]], ids=["none", "string", "int", "list"])
def test_resolve_recent_median_rejects_non_dict(bad_entry):
    with pytest.raises(TypeError):
        resolve_recent_median(bad_entry)


@pytest.mark.parametrize("bad_entry", [None, "string", 42], ids=["none", "string", "int"])
def test_detect_downtrend_rejects_non_dict(bad_entry):
    with pytest.raises(TypeError):
        detect_downtrend(bad_entry)


def test_resolve_recent_median_skips_non_numeric_median():
    entry = {
        "last_24_hours": {"median": "NaN-ish", "volume": 10},
        "last_7_days": {"median": 11.00, "volume": 50},
    }
    assert resolve_recent_median(entry) == 11.00


def test_resolve_recent_median_returns_none_when_all_medians_malformed():
    entry = {"last_90_days": {"median": {"bad": "shape"}}}
    assert resolve_recent_median(entry) is None


def test_detect_downtrend_ignores_malformed_median():
    entry = {
        "last_24_hours": {"median": "boom", "volume": 10},
        "last_7_days": {"median": 9.00, "volume": 50},
        "last_30_days": {"median": 12.00, "volume": 200},
    }
    detected, severity = detect_downtrend(entry)
    assert detected is True
    assert severity == pytest.approx(0.25)
