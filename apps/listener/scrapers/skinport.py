import asyncio
import json
import math
import os
import time
from collections.abc import AsyncGenerator
from datetime import UTC
from email.utils import parsedate_to_datetime
from pathlib import Path

import aiohttp
from models import MAX_EVENT_TYPE_LENGTH, MAX_LISTING_URL_LENGTH, FeedEvent, MarketTick, TickKind
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from scrapers.base import BaseScraper
from shared_utils import build_versioned_name, get_logger

logger = get_logger("listener.skinport")

# Redis Pub/Sub channel the Node.js sidecar publishes every saleFeed event to.
SALE_FEED_CHANNEL = "skinport:sale_feed"
SKINPORT_ITEM_URL = "https://skinport.com/item"
VENUE = "skinport"

# `tradable=1` is the lowest ask among listings that can be traded now. `tradable=0` is not "all listings":
# it returns only trade locked ones, priced below the rest by the lock (docs/skinport_feed.md, #275).
ITEMS_QUERY: dict[str, str | int] = {"app_id": 730, "currency": "USD", "tradable": 1}

# /v1/items is cached for 5 minutes, so one request per cache period is all that is useful.
POLL_INTERVAL_SECONDS = 305
# After a 429 without a usable Retry-After, the wait doubles up to this cap.
MAX_FALLBACK_BACKOFF_SECONDS = 1200
# Added to Retry-After, so the next request does not land in the last second of the lockout.
RETRY_AFTER_MARGIN_SECONDS = 5
# A longer Retry-After is treated as unusable; the lockouts seen so far lasted about an hour.
MAX_RETRY_AFTER_SECONDS = 7200
# Edge Redis key with the Unix time the next /v1/items request may go out. It outlives the listener process,
# so a restart does not send an early request (docs/skinport_feed.md, #282).
NEXT_ITEMS_REQUEST_KEY = "skinport:items:next_request_at"


async def _sleep(seconds: float) -> None:
    """Testable seam over asyncio.sleep for cooldown/backoff waits."""
    await asyncio.sleep(seconds)


def _now() -> float:
    """Testable seam over the wall clock; the next request time is shared across listener processes."""
    return time.time()


def _edge_redis_from_env() -> Redis:
    edge_redis_url = os.getenv("EDGE_REDIS_URL")
    redis_password = os.getenv("REDIS_PASSWORD")
    if edge_redis_url:
        return Redis.from_url(edge_redis_url, username="default", password=redis_password, decode_responses=True)
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6380))
    return Redis(host=redis_host, port=redis_port, username="default", password=redis_password, decode_responses=True)


def parse_retry_after(value: str | None, now: float) -> float | None:
    """Seconds to wait from a Retry-After header (a delay in seconds or an HTTP date); None when unusable."""
    if value is None:
        return None
    value = value.strip()
    if value.isdigit():
        seconds = float(value)
    else:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        seconds = max(0.0, retry_at.timestamp() - now)
    return seconds if seconds <= MAX_RETRY_AFTER_SECONDS else None


class NextRequestStore:
    """
    Keeps the time the next /v1/items request may go out in the edge Redis.

    A listener that restarts reads it back and waits out the rest of the poll interval or of a rate limit
    lockout, instead of polling at once. Polling carries on without it when Redis is unavailable.
    """

    def __init__(self, cache: Redis) -> None:
        self._cache = cache

    async def load(self) -> float | None:
        try:
            raw = await self._cache.get(NEXT_ITEMS_REQUEST_KEY)
        except RedisError as err:
            logger.warning("[SKINPORT] Could not read the next /v1/items request time from Redis: %s", err)
            return None
        try:
            return float(raw) if raw is not None else None
        except ValueError:
            return None

    async def save(self, request_at: float, now: float) -> None:
        # Expires shortly after the time it holds, so a stale value never delays a later start.
        expires_in = max(1, math.ceil(request_at - now)) + 60
        try:
            await self._cache.set(NEXT_ITEMS_REQUEST_KEY, f"{request_at:.3f}", ex=expires_in)
        except RedisError as err:
            logger.warning("[SKINPORT] Could not store the next /v1/items request time in Redis: %s", err)

    async def aclose(self) -> None:
        await self._cache.aclose()


def _as_number(value: object) -> float | None:
    """The value as a float when it is a real JSON number (bools excluded), else None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _optional_non_negative_int(value: object) -> int | None:
    """Pattern/finish as an int, or None when missing or out of range (the price is still kept)."""
    number = _as_number(value)
    return int(number) if number is not None and number >= 0 else None


def _optional_wear(value: object) -> float | None:
    """Float value in [0, 1], or None when missing or out of range (the price is still kept)."""
    number = _as_number(value)
    return number if number is not None and 0 <= number <= 1 else None


def _listing_url(sale: dict) -> str | None:
    """Deep link to the listing when the feed carries a sale ID, else to the item page.

    The public saleFeed currently sends saleId as null, so in practice this is the item page,
    which lists every open offer for that item.
    """
    slug = sale.get("url")
    if not slug:
        return None
    sale_id = sale.get("saleId")
    url = f"{SKINPORT_ITEM_URL}/{slug}/{sale_id}" if sale_id else f"{SKINPORT_ITEM_URL}/{slug}"
    # A truncated URL would be a broken link; drop it instead.
    return url if len(url) <= MAX_LISTING_URL_LENGTH else None


def _sale_to_tick(sale: dict, event_type: str, received_at_ms: int) -> MarketTick | None:
    """Normalize one entry of a saleFeed `sales` array; None when it lacks a name or a valid price."""
    market_hash_name = sale.get("marketHashName")
    sale_price = sale.get("salePrice")
    if not market_hash_name or sale_price is None:
        return None

    product_id = sale.get("productId")
    try:
        return MarketTick(
            venue=VENUE,
            kind=TickKind.of_feed_event(event_type),
            market_hash_name=build_versioned_name(market_hash_name, sale.get("version")),
            # salePrice is in USD cents when currency is USD
            price_usd=float(sale_price) / 100.0,
            timestamp=received_at_ms // 1000,
            float_value=_optional_wear(sale.get("wear")),
            stickers=sale.get("stickers") or [],
            event_type=event_type,
            # productId is the only identifier populated on both listed and sold events, so it is the key
            # that joins a listing to its outcome. saleId is documented as set on sold events
            # (https://docs.skinport.com/websocket/sale-feed) but is null on both in practice.
            listing_id=str(product_id) if product_id is not None else None,
            pattern=_optional_non_negative_int(sale.get("pattern")),
            paint_index=_optional_non_negative_int(sale.get("finish")),
            listing_url=_listing_url(sale),
        )
    except (ValidationError, ValueError, TypeError) as err:
        logger.warning("[SKINPORT] Skipping malformed %s sale for '%s': %s", event_type, market_hash_name, err)
        return None


def parse_sale_feed_message(message: str | bytes) -> tuple[FeedEvent, list[MarketTick]]:
    """
    Parse one sidecar envelope `{"receivedAt": <epoch ms>, "payload": <raw saleFeed event>}`.

    Returns the raw event (always, so nothing the venue sent is lost) plus one normalized tick
    per well-formed sale. Raises ValueError when the envelope itself is unusable.
    """
    envelope = json.loads(message)
    if not isinstance(envelope, dict) or not isinstance(envelope.get("payload"), dict):
        raise ValueError("Sale feed envelope must be an object with an object 'payload'")

    payload: dict = envelope["payload"]
    received_at_ms = envelope.get("receivedAt")
    if not isinstance(received_at_ms, int) or received_at_ms <= 0:
        raise ValueError("Sale feed envelope must carry a positive integer 'receivedAt'")

    # Clamped so an unexpected, overlong type is still recorded; the payload keeps the original value.
    event_type = str(payload.get("eventType") or "unknown")[:MAX_EVENT_TYPE_LENGTH]
    feed_event = FeedEvent(event_type=event_type, received_at_ms=received_at_ms, payload=payload)

    sales = payload.get("sales")
    ticks: list[MarketTick] = []
    for sale in sales if isinstance(sales, list) else []:
        if not isinstance(sale, dict):
            continue
        tick = _sale_to_tick(sale, event_type, received_at_ms)
        if tick is not None:
            ticks.append(tick)
    return feed_event, ticks


class SkinportScraper(BaseScraper):
    """
    Production Ingestion Engine for Skinport utilizing Basic Auth and mandatory Brotli compression.

    REST: https://docs.skinport.com/items. WebSocket: https://docs.skinport.com/websocket/sale-feed.
    Observed feed quirks: docs/skinport_feed.md.
    """

    def __init__(self):
        super().__init__(platform_name=VENUE)
        self.api_url = "https://api.skinport.com/v1/items"

        # Sidecar script path for the Node.js WebSocket relay
        self.sidecar_script_path = Path(__file__).parent / "skinport_websocket" / "sidecar.js"

        # Shared session for API requests (lazy init)
        self._session: aiohttp.ClientSession | None = None
        # When the next /v1/items request may go out, kept in the edge Redis (lazy init)
        self._next_request_store: NextRequestStore | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Returns a shared aiohttp session, creating it lazily if needed."""
        if self._session is None or self._session.closed:
            # No Authorization header: /v1/items is public, and authenticated requests share the account's
            # rate limit (they were refused for over an hour on 2026-09-26 while anonymous ones went through).
            headers = {"Accept": "application/json", "Accept-Encoding": "br", "User-Agent": "BrandSniperEdgeTelemetry/1.0"}
            self._session = aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=15, connect=5))
        return self._session

    async def close(self):
        """Closes the shared session and the next request store cleanly."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        if self._next_request_store is not None:
            await self._next_request_store.aclose()
            self._next_request_store = None

    def _open_next_request_store(self) -> NextRequestStore:
        return NextRequestStore(_edge_redis_from_env())

    async def poll_market_stream(self) -> AsyncGenerator[MarketTick, None]:
        """
        Polls Skinport's lowest tradable ask per item, passing Brotli-decompressed ticks down the pipe.

        One request per POLL_INTERVAL_SECONDS, and after a 429 exactly as long as Retry-After says. The time
        the next request may go out is kept in the edge Redis, so a restart never polls early (#282).
        """
        session = await self._get_session()
        if self._next_request_store is None:
            self._next_request_store = self._open_next_request_store()
        store = self._next_request_store

        next_request_at = await store.load() or 0.0
        fallback_backoff_seconds = POLL_INTERVAL_SECONDS
        while True:
            delay_seconds = next_request_at - _now()
            if delay_seconds > 0:
                logger.info("[SKINPORT] Next /v1/items request in %d seconds.", math.ceil(delay_seconds))
                await _sleep(delay_seconds)

            raw_items: list[dict] = []
            wait_seconds = POLL_INTERVAL_SECONDS
            try:
                logger.info("Querying asset directory stream (Rate Limit: 8 requests per 5 mins)...")
                async with session.get(self.api_url, params=ITEMS_QUERY) as response:
                    if response.status == 200:
                        # aiohttp automatically uses the loaded 'brotli' library to transparently unpack the data
                        raw_items = await response.json()
                        logger.info("Successfully decompressed %d market entries.", len(raw_items))
                        fallback_backoff_seconds = POLL_INTERVAL_SECONDS

                    elif response.status == 429:
                        retry_after_header = response.headers.get("Retry-After")
                        retry_after_seconds = parse_retry_after(retry_after_header, _now())
                        if retry_after_seconds is None:
                            fallback_backoff_seconds = min(MAX_FALLBACK_BACKOFF_SECONDS, fallback_backoff_seconds * 2)
                            wait_seconds = fallback_backoff_seconds
                        else:
                            wait_seconds = math.ceil(retry_after_seconds) + RETRY_AFTER_MARGIN_SECONDS
                        logger.warning(
                            "[SKINPORT] Rate limited (HTTP 429, Retry-After %s). Next request in %d seconds.",
                            retry_after_header,
                            wait_seconds,
                        )
                    else:
                        logger.warning("Marketplace responded with unexpected HTTP status code: %s", response.status)

            except (TimeoutError, aiohttp.ClientError, ValueError) as e:
                logger.error("Telemetry connection dropout encountered: %s", e)

            # Stored before the ticks are handed on: draining a poll can take minutes, and a restart in that
            # time must not send an early request.
            next_request_at = _now() + wait_seconds
            await store.save(next_request_at, _now())

            for item in raw_items:
                # We track 'min_price' as our entry signal parameter
                if item.get("min_price") is None:
                    continue
                try:
                    tick = MarketTick(
                        venue=VENUE,
                        kind=TickKind.REST_SNAPSHOT,
                        market_hash_name=build_versioned_name(item["market_hash_name"], item.get("version")),
                        price_usd=float(item["min_price"]),
                    )
                except (ValidationError, ValueError, TypeError, KeyError) as err:
                    logger.warning("[SKINPORT] Skipping malformed item '%s': %s", item.get("market_hash_name"), err)
                    continue
                yield tick

    async def listen_websocket_stream(self) -> AsyncGenerator[MarketTick | FeedEvent, None]:
        """
        Subscribes to the local Redis Pub/Sub channel relayed by the Node.js WebSocket sidecar.
        For every saleFeed event it yields the raw FeedEvent first, then one MarketTick per sale.
        """
        cache = _edge_redis_from_env()
        pubsub = cache.pubsub()
        await pubsub.subscribe(SALE_FEED_CHANNEL)
        logger.info("Subscribed to Redis channel '%s'", SALE_FEED_CHANNEL)

        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                try:
                    feed_event, ticks = parse_sale_feed_message(message["data"])
                except (ValueError, TypeError, ValidationError) as parse_err:
                    logger.error("Error parsing sidecar sale feed message: %s", parse_err)
                    continue

                yield feed_event
                for tick in ticks:
                    yield tick
        finally:
            await pubsub.unsubscribe(SALE_FEED_CHANNEL)
            await cache.aclose()
