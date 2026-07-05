import pytest
from shared_utils.pricing_utils import to_cents, resolve_recent_median, detect_downtrend


class TestToCents:
    def test_basic(self):
        assert to_cents(10.50) == 1050

    def test_rounding(self):
        assert to_cents(10.555) == 1056

    def test_small_value(self):
        assert to_cents(0.01) == 1

    def test_none(self):
        assert to_cents(None) is None

    def test_zero(self):
        assert to_cents(0.0) == 0


class TestResolveRecentMedian:
    def test_returns_24h_when_volume_exists(self):
        entry = {
            "last_24_hours": {"median": 12.50, "volume": 10},
            "last_7_days": {"median": 11.00, "volume": 50},
        }
        assert resolve_recent_median(entry) == 12.50

    def test_falls_through_to_7d_when_24h_volume_zero(self):
        entry = {
            "last_24_hours": {"median": 12.50, "volume": 0},
            "last_7_days": {"median": 11.00, "volume": 50},
        }
        assert resolve_recent_median(entry) == 11.00

    def test_falls_through_to_30d(self):
        entry = {
            "last_24_hours": {"median": 12.50, "volume": 0},
            "last_7_days": {"median": 11.00, "volume": 0},
            "last_30_days": {"median": 10.50, "volume": 200},
        }
        assert resolve_recent_median(entry) == 10.50

    def test_falls_through_to_90d_no_volume_check(self):
        entry = {
            "last_90_days": {"median": 9.00},
        }
        assert resolve_recent_median(entry) == 9.00

    def test_empty_dict(self):
        assert resolve_recent_median({}) is None

    def test_all_missing(self):
        assert resolve_recent_median({"last_24_hours": {}, "last_7_days": {}}) is None

    def test_missing_keys_are_safe(self):
        assert resolve_recent_median({"unknown": "data"}) is None


class TestDetectDowntrend:
    def test_no_downtrend(self):
        entry = {
            "last_7_days": {"median": 12.00, "volume": 50},
            "last_30_days": {"median": 10.00, "volume": 200},
        }
        detected, severity = detect_downtrend(entry)
        assert detected is False
        assert severity == 0.0

    def test_medium_term_downtrend(self):
        entry = {
            "last_7_days": {"median": 9.00, "volume": 50},
            "last_30_days": {"median": 12.00, "volume": 200},
        }
        detected, severity = detect_downtrend(entry)
        assert detected is True
        assert severity == pytest.approx(0.25)  # (12-9)/12

    def test_short_term_panic(self):
        entry = {
            "last_24_hours": {"median": 8.00, "volume": 10},
            "last_7_days": {"median": 10.00, "volume": 50},
        }
        detected, severity = detect_downtrend(entry)
        assert detected is True
        assert severity == pytest.approx(0.20)  # (10-8)/10

    def test_combined_downtrend(self):
        entry = {
            "last_24_hours": {"median": 8.00, "volume": 10},
            "last_7_days": {"median": 9.00, "volume": 50},
            "last_30_days": {"median": 12.00, "volume": 200},
        }
        detected, severity = detect_downtrend(entry)
        assert detected is True
        # medium-term: (12-9)/12 = 0.25, short-term: (9-8)/9 = 0.111...
        assert severity == pytest.approx(0.3611, abs=0.001)

    def test_empty_dict(self):
        detected, severity = detect_downtrend({})
        assert detected is False
        assert severity == 0.0
