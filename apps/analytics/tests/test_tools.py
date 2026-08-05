import json

import pytest
import tools
from tools import _classify_float, _wear_tier, fetch_live_market_floor, search_macro_trends


class _FakeResp:
    def __init__(self, payload, status=200):
        self.status = status
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return json.dumps(self.payload).encode()


# ---------------------------------------------------------------------------
# _classify_float tiers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("float_value", "expected_quality", "expected_multiplier"),
    [
        (-0.1, "Standard", 1.0),
        (1.5, "Standard", 1.0),
        (0.02, "Excellent", 1.3),
        (0.09, "Decent", 1.05),
        (0.12, "Good", 1.1),
        (0.18, "Decent", 1.03),
        (0.92, "Notable", 1.15),
    ],
)
def test_classify_float_tiers(float_value, expected_quality, expected_multiplier):
    quality, multiplier, _ = _classify_float(float_value)
    assert quality == expected_quality
    assert multiplier == expected_multiplier


def test_wear_tier_unknown_for_out_of_range_float():
    assert _wear_tier(1.5) == "Unknown"


# ---------------------------------------------------------------------------
# fetch_live_market_floor
# ---------------------------------------------------------------------------


def test_fetch_live_market_floor_success(monkeypatch):
    payload = {"cash_equivalent_avg_cents": 1500, "real_time_skinport_median_cents": 1400, "is_liquid": True}
    monkeypatch.setattr(tools.urllib.request, "urlopen", lambda url, timeout=5: _FakeResp(payload))

    out = json.loads(fetch_live_market_floor("AK-47 | Redline (Field-Tested)"))

    assert out["market_hash_name"] == "AK-47 | Redline (Field-Tested)"
    assert out["live_floor_cents"] == 1500
    assert out["recent_sales_cents"] == [1400]
    assert out["liquidity"] == "HIGH"


def test_fetch_live_market_floor_falls_back_to_median(monkeypatch):
    payload = {"real_time_skinport_median_cents": 1400, "is_liquid": False}
    monkeypatch.setattr(tools.urllib.request, "urlopen", lambda url, timeout=5: _FakeResp(payload))

    out = json.loads(fetch_live_market_floor("AK-47 | Redline (Field-Tested)"))

    assert out["live_floor_cents"] == 1400
    assert out["liquidity"] == "LOW"


def test_fetch_live_market_floor_non_200_returns_error_payload(monkeypatch):
    monkeypatch.setattr(tools.urllib.request, "urlopen", lambda url, timeout=5: _FakeResp({}, status=500))

    out = json.loads(fetch_live_market_floor("AK-47 | Redline (Field-Tested)"))

    assert out["live_floor_cents"] is None
    assert out["liquidity"] == "UNKNOWN"


def test_fetch_live_market_floor_network_error_returns_error_payload(monkeypatch):
    def boom(url, timeout=5):
        raise OSError("connection refused")

    monkeypatch.setattr(tools.urllib.request, "urlopen", boom)

    out = json.loads(fetch_live_market_floor("AK-47 | Redline (Field-Tested)"))

    assert out["live_floor_cents"] is None
    assert "ERROR" in out["message"]


# ---------------------------------------------------------------------------
# search_macro_trends
# ---------------------------------------------------------------------------


def test_search_macro_trends_success(monkeypatch):
    payload = {"trends": ["market crash detected"]}
    monkeypatch.setattr(tools.urllib.request, "urlopen", lambda req, timeout=5: _FakeResp(payload))

    out = search_macro_trends("market crash")

    assert out == json.dumps(payload)


def test_search_macro_trends_network_error_returns_default_message(monkeypatch):
    def boom(req, timeout=5):
        raise OSError("connection refused")

    monkeypatch.setattr(tools.urllib.request, "urlopen", boom)

    assert search_macro_trends("market crash") == "No major macroeconomic news detected."


def test_search_macro_trends_empty_payload_returns_default_message(monkeypatch):
    monkeypatch.setattr(tools.urllib.request, "urlopen", lambda req, timeout=5: _FakeResp({}, status=500))

    assert search_macro_trends("market crash") == "No major macroeconomic news detected."
