import json
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
import scrapers.skinport as skinport
from models import FeedEvent, MarketTick
from redis.exceptions import ConnectionError as RedisConnectionError
from scrapers.skinport import NEXT_ITEMS_REQUEST_KEY, NextRequestStore, SkinportScraper, parse_retry_after

NOW = 1_790_000_000.0


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
    def __init__(self, status, headers=None):
        self.status = status
        self.headers = headers or {}

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


class _MemoryStore:
    """Stands in for the edge Redis NextRequestStore."""

    def __init__(self, request_at=None):
        self.request_at = request_at
        self.closed = False

    async def load(self):
        return self.request_at

    async def save(self, request_at, now):
        self.request_at = request_at

    async def aclose(self):
        self.closed = True


@pytest.fixture(autouse=True)
def _memory_store(monkeypatch):
    """Polls never touch a real Redis; each scraper gets a fresh in-memory next request store."""
    monkeypatch.setattr(SkinportScraper, "_open_next_request_store", lambda self: _MemoryStore())


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_items_requests_send_no_credentials_even_when_configured(monkeypatch):
    monkeypatch.setenv("SKINPORT_CLIENT_ID", "test_client")
    monkeypatch.setenv("SKINPORT_CLIENT_SECRET", "test_secret")
    scraper = SkinportScraper()

    session = await scraper._get_session()

    assert "Authorization" not in session.headers
    await scraper.close()


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
    # Tradable listings only: tradable=0 would return only the trade locked ones.
    assert session.get_calls[0][1] == {"app_id": 730, "currency": "USD", "tradable": 1}


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


def _scraper_with(responses, store=None, payload=None) -> tuple[SkinportScraper, _Session]:
    scraper = SkinportScraper()
    session = _Session(responses, default_payload=payload)
    scraper._session = session
    if store is not None:
        scraper._next_request_store = store
    return scraper, session


@pytest.mark.asyncio
async def test_429_waits_exactly_as_long_as_retry_after_says(mocker):
    item = {"market_hash_name": "AK-47 | Redline", "min_price": 10.0}
    store = _MemoryStore()
    scraper, session = _scraper_with([_StatusResponse(429, {"Retry-After": "1650"}), _OkResponse([item])], store)
    mocker.patch("scrapers.skinport._now", return_value=NOW)
    sleep = mocker.patch("scrapers.skinport._sleep", new_callable=AsyncMock)

    stream = scraper.poll_market_stream()
    await anext(stream)
    await stream.aclose()

    # Retry-After plus the margin, and nothing sooner.
    assert [call.args[0] for call in sleep.await_args_list] == [1655.0]
    assert len(session.get_calls) == 2
    assert store.request_at == NOW + 305


@pytest.mark.asyncio
async def test_429_with_an_http_date_waits_until_that_time(mocker):
    item = {"market_hash_name": "AK-47 | Redline", "min_price": 10.0}
    # NOW is 2026-09-21 14:13:20 UTC; the lockout ends 600 seconds later.
    scraper, _ = _scraper_with([_StatusResponse(429, {"Retry-After": "Mon, 21 Sep 2026 14:23:20 GMT"}), _OkResponse([item])])
    mocker.patch("scrapers.skinport._now", return_value=NOW)
    sleep = mocker.patch("scrapers.skinport._sleep", new_callable=AsyncMock)

    stream = scraper.poll_market_stream()
    await anext(stream)
    await stream.aclose()

    assert [call.args[0] for call in sleep.await_args_list] == [605.0]


@pytest.mark.asyncio
async def test_429_without_retry_after_doubles_the_backoff(mocker, caplog):
    item = {"market_hash_name": "AK-47 | Redline", "min_price": 10.0}
    responses = [_StatusResponse(429), _StatusResponse(429, {"Retry-After": "soon"}), _OkResponse([item])]
    scraper, _ = _scraper_with(responses)
    mocker.patch("scrapers.skinport._now", return_value=NOW)
    sleep = mocker.patch("scrapers.skinport._sleep", new_callable=AsyncMock)

    with caplog.at_level("WARNING", logger="listener.skinport"):
        stream = scraper.poll_market_stream()
        await anext(stream)
        await stream.aclose()

    assert [call.args[0] for call in sleep.await_args_list] == [610.0, 1200.0]
    assert "Rate limited (HTTP 429, Retry-After None). Next request in 610 seconds." in caplog.text


@pytest.mark.asyncio
async def test_a_restart_waits_for_the_stored_request_time_before_its_first_request(mocker):
    item = {"market_hash_name": "AK-47 | Redline", "min_price": 10.0}
    # The previous process polled 105 seconds ago, or is inside a lockout: either way it stored the time.
    scraper, session = _scraper_with([_OkResponse([item])], _MemoryStore(request_at=NOW + 200))
    mocker.patch("scrapers.skinport._now", return_value=NOW)
    requests_before_sleep = []

    async def record_sleep(seconds):
        requests_before_sleep.append((seconds, len(session.get_calls)))

    mocker.patch("scrapers.skinport._sleep", side_effect=record_sleep)

    stream = scraper.poll_market_stream()
    await anext(stream)
    await stream.aclose()

    assert requests_before_sleep == [(200.0, 0)]


@pytest.mark.asyncio
async def test_the_next_request_time_is_stored_before_the_ticks_are_handed_on(mocker):
    store = _MemoryStore()
    items = [{"market_hash_name": "AK-47 | Redline", "min_price": 10.0}]
    scraper, _ = _scraper_with([_OkResponse(items)], store)
    mocker.patch("scrapers.skinport._now", return_value=NOW)
    mocker.patch("scrapers.skinport._sleep", new_callable=AsyncMock)

    stream = scraper.poll_market_stream()
    await anext(stream)

    assert store.request_at == NOW + 305
    await stream.aclose()


@pytest.mark.asyncio
async def test_a_malformed_item_is_skipped_and_the_poll_goes_on(mocker, caplog):
    payload = [
        {"market_hash_name": "No Listings", "min_price": None},
        {"market_hash_name": "Broken", "min_price": "not a price"},
        {"min_price": 3.0},
        {"market_hash_name": "AK-47 | Redline", "min_price": 10.0},
    ]
    scraper, _ = _scraper_with([_OkResponse(payload)])
    mocker.patch("scrapers.skinport._sleep", new_callable=AsyncMock)

    with caplog.at_level("WARNING", logger="listener.skinport"):
        stream = scraper.poll_market_stream()
        tick = await anext(stream)
        await stream.aclose()

    assert tick.market_hash_name == "AK-47 | Redline"
    assert "Skipping malformed item 'Broken'" in caplog.text


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("1650", 1650.0),
        (" 30 ", 30.0),
        ("Mon, 21 Sep 2026 14:23:20 GMT", 600.0),
        ("Mon, 21 Sep 2026 14:23:20 -0000", 600.0),
        ("Mon, 21 Sep 2026 13:46:40 GMT", 0.0),  # already past: request now
        ("7201", None),  # longer than any lockout seen: treated as unusable
        ("soon", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_retry_after(header, expected):
    assert parse_retry_after(header, NOW) == expected


@pytest.mark.asyncio
async def test_next_request_store_round_trips_through_redis():
    cache = MagicMock()
    cache.set = AsyncMock()
    cache.get = AsyncMock(return_value="1790000305.000")
    cache.aclose = AsyncMock()
    store = NextRequestStore(cache)

    await store.save(NOW + 305, NOW)

    cache.set.assert_awaited_once_with(NEXT_ITEMS_REQUEST_KEY, "1790000305.000", ex=365)
    assert await store.load() == NOW + 305
    await store.aclose()
    cache.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_next_request_store_carries_on_without_redis(caplog):
    cache = MagicMock()
    cache.get = AsyncMock(side_effect=RedisConnectionError("refused"))
    cache.set = AsyncMock(side_effect=RedisConnectionError("refused"))
    store = NextRequestStore(cache)

    with caplog.at_level("WARNING", logger="listener.skinport"):
        assert await store.load() is None
        await store.save(NOW + 305, NOW)

    assert "Could not read the next /v1/items request time" in caplog.text
    assert "Could not store the next /v1/items request time" in caplog.text


@pytest.mark.asyncio
async def test_next_request_store_ignores_a_garbled_value():
    cache = MagicMock()
    cache.get = AsyncMock(return_value="garbled")

    assert await NextRequestStore(cache).load() is None


@pytest.mark.asyncio
async def test_poll_opens_the_edge_redis_store_and_close_releases_it(monkeypatch):
    monkeypatch.undo()  # use the real _open_next_request_store
    cache = MagicMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()
    cache.aclose = AsyncMock()
    monkeypatch.setenv("EDGE_REDIS_URL", "redis://edge:6379")
    monkeypatch.setattr("scrapers.skinport.Redis.from_url", lambda *args, **kwargs: cache)
    monkeypatch.setattr("scrapers.skinport._sleep", AsyncMock())
    scraper, _ = _scraper_with([_OkResponse([{"market_hash_name": "AK-47 | Redline", "min_price": 10.0}])])

    stream = scraper.poll_market_stream()
    await anext(stream)
    await stream.aclose()
    await scraper.close()

    cache.set.assert_awaited_once()
    cache.aclose.assert_awaited_once()
    assert scraper._next_request_store is None


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


def _envelope(payload: dict, received_at_ms: int = 1_790_000_000_000) -> str:
    return json.dumps({"receivedAt": received_at_ms, "payload": payload})


async def _drain(stream, count: int) -> list:
    items = [await anext(stream) for _ in range(count)]
    await stream.aclose()
    return items


@pytest.mark.asyncio
async def test_websocket_yields_raw_event_then_parsed_sales(monkeypatch):
    scraper = SkinportScraper()
    payload = {
        "eventType": "listed",
        "sales": [
            {
                "marketHashName": "AK-47 | Redline",
                "salePrice": 155000,
                "wear": 0.31,
                "stickers": [{"name": "Titan | Katowice 2014"}],
                "version": "Factory New",
            }
        ],
    }
    pubsub = _pubsub_for([{"type": "subscribe"}, {"type": "message", "data": _envelope(payload)}])
    cache = _redis_for(pubsub)
    monkeypatch.setenv("EDGE_REDIS_URL", "redis://localhost:6380")
    with patch("scrapers.skinport.Redis.from_url", return_value=cache):
        feed_event, tick = await _drain(scraper.listen_websocket_stream(), 2)

    pubsub.subscribe.assert_awaited_once_with("skinport:sale_feed")
    assert isinstance(feed_event, FeedEvent)
    assert feed_event.event_type == "listed"
    assert feed_event.payload == payload
    assert tick.market_hash_name == "AK-47 | Redline (Factory New)"
    assert tick.price_usd == 1550.0
    assert tick.float_value == 0.31
    assert tick.stickers == [{"name": "Titan | Katowice 2014"}]
    assert tick.event_type == "listed"


@pytest.mark.asyncio
async def test_websocket_skips_sales_without_required_fields(monkeypatch):
    scraper = SkinportScraper()
    payload = {
        "eventType": "listed",
        "sales": [
            {"marketHashName": "No Price"},
            {"salePrice": 10000},
            {"marketHashName": "Zero Price", "salePrice": 0},
            {"marketHashName": "Good One", "salePrice": 10000},
        ],
    }
    pubsub = _pubsub_for([{"type": "message", "data": _envelope(payload)}])
    cache = _redis_for(pubsub)
    monkeypatch.setenv("EDGE_REDIS_URL", "redis://localhost:6380")
    with patch("scrapers.skinport.Redis.from_url", return_value=cache):
        feed_event, tick = await _drain(scraper.listen_websocket_stream(), 2)

    # The raw event keeps every sale, including the ones that could not become ticks.
    assert len(feed_event.payload["sales"]) == 4
    assert tick.market_hash_name == "Good One"


@pytest.mark.asyncio
async def test_websocket_survives_malformed_message(monkeypatch):
    scraper = SkinportScraper()
    good_payload = {"eventType": "sold", "sales": [{"marketHashName": "Good One", "salePrice": 10000}]}
    pubsub = _pubsub_for(
        [
            {"type": "message", "data": "{not valid json"},
            {"type": "message", "data": json.dumps({"sales": []})},
            {"type": "message", "data": _envelope(good_payload)},
        ]
    )
    cache = _redis_for(pubsub)
    monkeypatch.setenv("EDGE_REDIS_URL", "redis://localhost:6380")
    with patch("scrapers.skinport.Redis.from_url", return_value=cache):
        feed_event, tick = await _drain(scraper.listen_websocket_stream(), 2)

    assert feed_event.event_type == "sold"
    assert tick.market_hash_name == "Good One"
