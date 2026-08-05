import asyncio
import json
from unittest.mock import AsyncMock

import aiohttp
import pytest
import tools
from tools import (
    _classify_float,
    _wear_tier,
    fetch_live_market_floor,
    search_macro_trends,
    verify_float_value,
)


class _FakeResp:
    def __init__(self, payload, status=200):
        self.status = status
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def text(self):
        return json.dumps(self.payload)


class _FakeSession:
    closed = False

    def __init__(self, resp):
        self.resp = resp

    def get(self, url, timeout=None):
        return self.resp

    def post(self, url, json=None, timeout=None):
        return self.resp


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


def _fake_session_getter(payload, status=200):
    return AsyncMock(return_value=_FakeSession(_FakeResp(payload, status=status)))


@pytest.mark.asyncio
async def test_fetch_live_market_floor_success(monkeypatch):
    payload = {"cash_equivalent_avg_cents": 1500, "real_time_skinport_median_cents": 1400, "is_liquid": True}
    monkeypatch.setattr(tools, "_get_http_session", _fake_session_getter(payload))

    out = json.loads(await fetch_live_market_floor("AK-47 | Redline (Field-Tested)"))

    assert out["market_hash_name"] == "AK-47 | Redline (Field-Tested)"
    assert out["live_floor_cents"] == 1500
    assert out["recent_sales_cents"] == [1400]
    assert out["liquidity"] == "HIGH"


@pytest.mark.asyncio
async def test_fetch_live_market_floor_falls_back_to_median(monkeypatch):
    payload = {"real_time_skinport_median_cents": 1400, "is_liquid": False}
    monkeypatch.setattr(tools, "_get_http_session", _fake_session_getter(payload))

    out = json.loads(await fetch_live_market_floor("AK-47 | Redline (Field-Tested)"))

    assert out["live_floor_cents"] == 1400
    assert out["liquidity"] == "LOW"


@pytest.mark.asyncio
async def test_fetch_live_market_floor_non_200_returns_error_payload(monkeypatch):
    monkeypatch.setattr(tools, "_get_http_session", _fake_session_getter({}, status=500))

    out = json.loads(await fetch_live_market_floor("AK-47 | Redline (Field-Tested)"))

    assert out["live_floor_cents"] is None
    assert out["liquidity"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_fetch_live_market_floor_network_error_returns_error_payload(monkeypatch):
    async def boom():
        raise aiohttp.ClientError("connection refused")

    monkeypatch.setattr(tools, "_get_http_session", boom)

    out = json.loads(await fetch_live_market_floor("AK-47 | Redline (Field-Tested)"))

    assert out["live_floor_cents"] is None
    assert "ERROR" in out["message"]


# ---------------------------------------------------------------------------
# search_macro_trends
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_macro_trends_success(monkeypatch):
    payload = {"trends": ["market crash detected"]}
    monkeypatch.setattr(tools, "_get_http_session", _fake_session_getter(payload))

    out = await search_macro_trends("market crash")

    assert out == json.dumps(payload)


@pytest.mark.asyncio
async def test_search_macro_trends_network_error_returns_default_message(monkeypatch):
    async def boom():
        raise aiohttp.ClientError("connection refused")

    monkeypatch.setattr(tools, "_get_http_session", boom)

    assert await search_macro_trends("market crash") == "No major macroeconomic news detected."


@pytest.mark.asyncio
async def test_search_macro_trends_empty_payload_returns_default_message(monkeypatch):
    monkeypatch.setattr(tools, "_get_http_session", _fake_session_getter({}, status=500))

    assert await search_macro_trends("market crash") == "No major macroeconomic news detected."


# ---------------------------------------------------------------------------
# session lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(tools.AVAILABLE_FUNCTIONS))
def test_all_tools_are_awaitable(name):
    assert asyncio.iscoroutinefunction(tools.AVAILABLE_FUNCTIONS[name])


@pytest.mark.parametrize(
    ("item_name", "float_value", "expected"),
    [
        pytest.param(
            "AK-47 | Redline (Field-Tested)",
            0.001,
            {"float_quality": "Exceptional", "premium_multiplier": 1.5, "wear_tier": "Factory New"},
            id="factory_new_exceptional",
        ),
        pytest.param(
            "AWP | Asiimov (Field-Tested)",
            0.96,
            {"float_quality": "Exceptional", "premium_multiplier": 1.3, "wear_tier": "Battle-Scarred"},
            id="battle_scarred_exceptional",
        ),
        pytest.param(
            "M4A4 | Howl (Factory New)",
            0.30,
            {"float_quality": "Standard", "premium_multiplier": 1.0, "wear_tier": "Field-Tested"},
            id="field_tested_standard",
        ),
        pytest.param(
            "AK-47 | Redline (Factory New)",
            0.05,
            {"float_quality": "Good", "premium_multiplier": 1.1},
            id="factory_new_good",
        ),
        pytest.param(
            "AK-47 | Redline (Minimal Wear)",
            0.075,
            {"float_quality": "Good", "premium_multiplier": 1.15, "wear_tier": "Minimal Wear"},
            id="minimal_wear_good",
        ),
    ],
)
@pytest.mark.asyncio
async def test_verify_float_value(item_name, float_value, expected):
    result = json.loads(await verify_float_value(item_name, float_value))
    for key, value in expected.items():
        assert result[key] == value


@pytest.mark.asyncio
async def test_get_http_session_reuses_existing_instance(monkeypatch):
    fake = _FakeSession(_FakeResp({}))
    monkeypatch.setattr(tools, "_http_session", fake)

    session = await tools._get_http_session()

    assert session is fake
    monkeypatch.setattr(tools, "_http_session", None)


@pytest.mark.asyncio
async def test_get_http_session_creates_new_when_none(monkeypatch):
    monkeypatch.setattr(tools, "_http_session", None)

    session = await tools._get_http_session()

    assert isinstance(session, aiohttp.ClientSession)
    assert not session.closed
    await tools.close_http_session()
    assert tools._http_session is None


@pytest.mark.asyncio
async def test_get_http_session_recreates_when_closed(monkeypatch):
    fake = _FakeSession(_FakeResp({}))
    fake.closed = True
    monkeypatch.setattr(tools, "_http_session", fake)

    session = await tools._get_http_session()

    assert isinstance(session, aiohttp.ClientSession)
    assert not session.closed
    await tools.close_http_session()
    assert tools._http_session is None


@pytest.mark.asyncio
async def test_close_http_session_closes_and_resets(monkeypatch):
    fake = _FakeSession(_FakeResp({}))
    fake.closed = False
    fake.close = AsyncMock()
    monkeypatch.setattr(tools, "_http_session", fake)

    await tools.close_http_session()

    assert fake.close.await_count == 1
    assert tools._http_session is None
    monkeypatch.setattr(tools, "_http_session", None)


@pytest.mark.asyncio
async def test_close_http_session_is_noop_when_none(monkeypatch):
    monkeypatch.setattr(tools, "_http_session", None)

    await tools.close_http_session()

    assert tools._http_session is None
