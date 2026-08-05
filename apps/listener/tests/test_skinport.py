import json
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
import scrapers.skinport as skinport
from models import MarketTick
from scrapers.skinport import SkinportScraper


class _OkResponse:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def json(self):
        return self.payload


class _StatusResponse:
    def __init__(self, status):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


class _Session:
    def __init__(self, responses, default_payload=None):
        self.responses = list(responses)
        self.default_payload = default_payload if default_payload is not None else []
        self.get_calls = []
        self.closed = False

    def get(self, url, params=None, **kwargs):
        self.get_calls.append((url, params))
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        return _OkResponse(self.default_payload)

    async def close(self):
        self.closed = True


# ---------------------------------------------------------------------------
# Auth header and session management
# ---------------------------------------------------------------------------


def test_build_auth_header_without_credentials(monkeypatch):
    monkeypatch.delenv("SKINPORT_CLIENT_ID", raising=False)
    monkeypatch.delenv("SKINPORT_CLIENT_SECRET", raising=False)

    scraper = SkinportScraper()

    assert scraper._build_auth_header() == ""


def test_build_auth_header_with_credentials(monkeypatch):
    monkeypatch.setenv("SKINPORT_CLIENT_ID", "test_client")
    monkeypatch.setenv("SKINPORT_CLIENT_SECRET", "test_secret")

    scraper = SkinportScraper()

    header = scraper._build_auth_header()

    assert header.startswith("Basic ")
    assert header == "Basic dGVzdF9jbGllbnQ6dGVzdF9zZWNyZXQ="


@pytest.mark.asyncio
async def test_get_session_creates_and_reuses():
    scraper = SkinportScraper()

    first = await scraper._get_session()
    second = await scraper._get_session()

    assert first is second
    assert first.headers.get("Accept-Encoding") == "br"
    await scraper.close()


@pytest.mark.asyncio
async def test_close_resets_session():
    scraper = SkinportScraper()
    session = await scraper._get_session()

    await scraper.close()

    assert session.closed
    assert scraper._session is None


@pytest.mark.asyncio
async def test_sleep_yields_without_delay():
    await skinport._sleep(0)


# ---------------------------------------------------------------------------
# poll_market_stream
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_poll_yields_market_ticks_from_200_response(mocker):
    scraper = SkinportScraper()
    payload = [
        {"market_hash_name": "AK-47 | Redline", "min_price": 15.5},
        {"market_hash_name": "★ Butterfly Knife | Doppler", "min_price": 700.0, "version": "Phase 3"},
        {"market_hash_name": "No Price Item", "min_price": None},
    ]
    session = _Session([_OkResponse(payload)], default_payload=[{"market_hash_name": "Fallback | Item", "min_price": 5.0}])
    scraper._session = session
    mocker.patch("scrapers.skinport._sleep", new_callable=AsyncMock)

    stream = scraper.poll_market_stream()
    first = await anext(stream)
    second = await anext(stream)
    await stream.aclose()

    assert isinstance(first, MarketTick)
    assert first.market_hash_name == "AK-47 | Redline"
    assert first.price_usd == 15.5
    assert second.market_hash_name == "★ Butterfly Knife | Doppler (Phase 3)"
    assert session.get_calls[0][1] == {"app_id": 730, "currency": "USD", "tradable": 0}


@pytest.mark.asyncio
async def test_poll_continues_after_401(mocker):
    scraper = SkinportScraper()
    item = {"market_hash_name": "AK-47 | Redline", "min_price": 10.0}
    session = _Session([_StatusResponse(401), _OkResponse([item])])
    scraper._session = session
    mocker.patch("scrapers.skinport._sleep", new_callable=AsyncMock)

    stream = scraper.poll_market_stream()
    await stream.__anext__()
    await stream.aclose()

    assert len(session.get_calls) == 2


@pytest.mark.asyncio
async def test_poll_continues_after_429(mocker):
    scraper = SkinportScraper()
    item = {"market_hash_name": "AK-47 | Redline", "min_price": 10.0}
    session = _Session([_StatusResponse(429), _OkResponse([item])])
    scraper._session = session
    mocker.patch("scrapers.skinport._sleep", new_callable=AsyncMock)

    stream = scraper.poll_market_stream()
    await stream.__anext__()
    await stream.aclose()

    assert len(session.get_calls) == 2


@pytest.mark.asyncio
async def test_poll_continues_after_unexpected_status(mocker):
    scraper = SkinportScraper()
    item = {"market_hash_name": "AK-47 | Redline", "min_price": 10.0}
    session = _Session([_StatusResponse(418), _OkResponse([item])])
    scraper._session = session
    mocker.patch("scrapers.skinport._sleep", new_callable=AsyncMock)

    stream = scraper.poll_market_stream()
    await stream.__anext__()
    await stream.aclose()

    assert len(session.get_calls) == 2


@pytest.mark.asyncio
async def test_poll_continues_after_transport_error(mocker):
    scraper = SkinportScraper()
    item = {"market_hash_name": "AK-47 | Redline", "min_price": 10.0}
    session = _Session([aiohttp.ClientConnectionError("connection reset"), _OkResponse([item])])
    scraper._session = session
    mocker.patch("scrapers.skinport._sleep", new_callable=AsyncMock)

    stream = scraper.poll_market_stream()
    await stream.__anext__()
    await stream.aclose()

    assert len(session.get_calls) == 2


# ---------------------------------------------------------------------------
# listen_websocket_stream message parsing
# ---------------------------------------------------------------------------


def _pubsub_for(messages):
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    pubsub.unsubscribe = AsyncMock()

    async def listen():
        for message in messages:
            yield message

    pubsub.listen = MagicMock(return_value=listen())
    return pubsub


def _redis_for(pubsub):
    cache = MagicMock()
    cache.aclose = AsyncMock()
    cache.pubsub.return_value = pubsub
    return cache


@pytest.mark.asyncio
async def test_websocket_yields_parsed_sales(monkeypatch):
    scraper = SkinportScraper()
    pubsub = _pubsub_for(
        [
            {"type": "subscribe"},
            {
                "type": "message",
                "data": json.dumps(
                    {
                        "sales": [
                            {
                                "marketHashName": "AK-47 | Redline",
                                "salePrice": 155000,
                                "wear": 0.31,
                                "stickers": [{"name": "Titan | Katowice 2014"}],
                                "version": "Factory New",
                            }
                        ]
                    }
                ),
            },
        ]
    )
    cache = _redis_for(pubsub)
    monkeypatch.setenv("EDGE_REDIS_URL", "redis://localhost:6380")
    with patch("scrapers.skinport.Redis.from_url", return_value=cache):
        stream = scraper.listen_websocket_stream()
        tick = await anext(stream)
        await stream.aclose()

    assert tick.market_hash_name == "AK-47 | Redline (Factory New)"
    assert tick.price_usd == 1550.0
    assert tick.float_value == 0.31
    assert tick.stickers == [{"name": "Titan | Katowice 2014"}]


@pytest.mark.asyncio
async def test_websocket_skips_sales_without_required_fields(monkeypatch):
    scraper = SkinportScraper()
    pubsub = _pubsub_for(
        [
            {
                "type": "message",
                "data": json.dumps(
                    {
                        "sales": [
                            {"marketHashName": "No Price"},
                            {"salePrice": 10000},
                            {"marketHashName": "Good One", "salePrice": 10000},
                        ]
                    }
                ),
            }
        ]
    )
    cache = _redis_for(pubsub)
    monkeypatch.setenv("EDGE_REDIS_URL", "redis://localhost:6380")
    with patch("scrapers.skinport.Redis.from_url", return_value=cache):
        stream = scraper.listen_websocket_stream()
        tick = await anext(stream)
        await stream.aclose()

    assert tick.market_hash_name == "Good One"


@pytest.mark.asyncio
async def test_websocket_survives_malformed_message(monkeypatch):
    scraper = SkinportScraper()
    pubsub = _pubsub_for(
        [
            {"type": "message", "data": "{not valid json"},
            {"type": "message", "data": json.dumps({"sales": [{"marketHashName": "Good One", "salePrice": 10000}]})},
        ]
    )
    cache = _redis_for(pubsub)
    monkeypatch.setenv("EDGE_REDIS_URL", "redis://localhost:6380")
    with patch("scrapers.skinport.Redis.from_url", return_value=cache):
        stream = scraper.listen_websocket_stream()
        tick = await anext(stream)
        await stream.aclose()

    assert tick.market_hash_name == "Good One"
