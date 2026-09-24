import asyncio
import base64
import json
import os
from collections.abc import AsyncGenerator
from pathlib import Path

import aiohttp
from models import MAX_EVENT_TYPE_LENGTH, MAX_LISTING_URL_LENGTH, FeedEvent, MarketTick
from pydantic import ValidationError
from redis.asyncio import Redis
from scrapers.base import BaseScraper
from shared_utils import build_versioned_name, get_logger

logger = get_logger("listener.skinport")

# Redis Pub/Sub channel the Node.js sidecar publishes every saleFeed event to.
SALE_FEED_CHANNEL = "skinport:sale_feed"
SKINPORT_ITEM_URL = "https://skinport.com/item"
VENUE = "skinport"


async def _sleep(seconds: float) -> None:
    """Testable seam over asyncio.sleep for cooldown/backoff waits."""
    await asyncio.sleep(seconds)


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

        # Pull secure platform credentials out of environment variables
        self.client_id = os.getenv("SKINPORT_CLIENT_ID")
        self.client_secret = os.getenv("SKINPORT_CLIENT_SECRET")

        # Sidecar script path for the Node.js WebSocket relay
        self.sidecar_script_path = Path(__file__).parent / "skinport_websocket" / "sidecar.js"

        # Shared session for API requests (lazy init)
        self._session: aiohttp.ClientSession | None = None

    def _build_auth_header(self) -> str:
        """Constructs a compliant HTTP Basic Authentication header string using Base64 encoding."""
        if not self.client_id or not self.client_secret:
            # Fallback gracefully to unauthenticated public access if credentials aren't set yet
            return ""

        raw_credentials = f"{self.client_id}:{self.client_secret}"
        encoded_bytes = base64.b64encode(raw_credentials.encode("utf-8"))
        return f"Basic {encoded_bytes.decode('utf-8')}"

    async def _get_session(self) -> aiohttp.ClientSession:
        """Returns a shared aiohttp session, creating it lazily if needed."""
        if self._session is None or self._session.closed:
            headers = {"Accept": "application/json", "Accept-Encoding": "br", "User-Agent": "BrandSniperEdgeTelemetry/1.0"}
            auth_string = self._build_auth_header()
            if auth_string:
                headers["Authorization"] = auth_string
            self._session = aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=15, connect=5))
        return self._session

    async def close(self):
        """Closes the shared session cleanly."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def poll_market_stream(self) -> AsyncGenerator[MarketTick, None]:
        """Polls Skinport REST feeds, passing authenticated, Brotli-decompressed tokens down the pipe."""
        session = await self._get_session()
        auth_string = self._build_auth_header()
        if auth_string:
            logger.info("Basic Authentication token successfully compiled and injected.")

        backoff_seconds = 305
        while True:
            try:
                # Target CS2 inventory items denominated in USD
                params: dict[str, str | int] = {"app_id": 730, "currency": "USD", "tradable": 0}

                logger.info("Querying asset directory stream (Rate Limit: 8 requests per 5 mins)...")
                async with session.get(self.api_url, params=params) as response:
                    if response.status == 200:
                        # aiohttp automatically uses the loaded 'brotli' library to transparently unpack the data
                        raw_items = await response.json()
                        logger.info("Successfully decompressed %d market entries.", len(raw_items))
                        backoff_seconds = 305  # Reset on success

                        for item in raw_items:
                            # We track 'min_price' as our entry signal parameter
                            if item.get("min_price") is not None:
                                market_hash_name = item["market_hash_name"]
                                version = item.get("version")
                                market_hash_name = build_versioned_name(market_hash_name, version)
                                yield MarketTick(
                                    venue=VENUE, market_hash_name=market_hash_name, price_usd=float(item["min_price"])
                                )

                    elif response.status == 401:
                        logger.error(
                            "API Rejected Credentials! Check your client ID and secret variables inside your .env file."
                        )
                        backoff_seconds = 305
                    elif response.status == 429:
                        backoff_seconds = min(1200, backoff_seconds * 2)
                        logger.warning(
                            "High-velocity rate limits encountered. Backing off production loop... Retrying in %d seconds.",
                            backoff_seconds,
                        )
                    else:
                        logger.warning("Marketplace responded with unexpected HTTP status code: %s", response.status)
                        backoff_seconds = 305

            except (TimeoutError, aiohttp.ClientError, ValueError) as e:
                logger.error("Telemetry connection dropout encountered: %s", e)
                backoff_seconds = 305

            # Respect the 5-minute cache instruction or back off if rate limited
            logger.info("Entering calculated cooldown cycle for %d seconds...", backoff_seconds)
            await _sleep(backoff_seconds)

    async def listen_websocket_stream(self) -> AsyncGenerator[MarketTick | FeedEvent, None]:
        """
        Subscribes to the local Redis Pub/Sub channel relayed by the Node.js WebSocket sidecar.
        For every saleFeed event it yields the raw FeedEvent first, then one MarketTick per sale.
        """
        edge_redis_url = os.getenv("EDGE_REDIS_URL")
        redis_password = os.getenv("REDIS_PASSWORD")
        if edge_redis_url:
            cache = Redis.from_url(edge_redis_url, username="default", password=redis_password, decode_responses=True)
        else:
            redis_host = os.getenv("REDIS_HOST", "localhost")
            redis_port = int(os.getenv("REDIS_PORT", 6380))
            cache = Redis(host=redis_host, port=redis_port, username="default", password=redis_password, decode_responses=True)
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
