import asyncio
import base64
import json
import os
import time
from collections.abc import AsyncGenerator
from pathlib import Path

import aiohttp
from models import MarketTick
from redis.asyncio import Redis
from shared_utils import build_versioned_name, get_logger, parse_version_from_name, resolve_recent_median

from scrapers.base import BaseScraper

logger = get_logger("listener.skinport")


class SkinportScraper(BaseScraper):
    """Production Ingestion Engine for Skinport utilizing Basic Auth and mandatory Brotli compression."""

    def __init__(self):
        super().__init__(platform_name="skinport")
        self.api_url = "https://api.skinport.com/v1/items"

        # Pull secure platform credentials out of environment variables
        self.client_id = os.getenv("SKINPORT_CLIENT_ID")
        self.client_secret = os.getenv("SKINPORT_CLIENT_SECRET")

        # Local cache for sales history to prevent API rate limit exhaustion
        # Map: (base_name, version) -> (inserted_timestamp, entry_dict)
        self.history_cache = {}
        self.cache_ttl = 600  # 10 minutes cache TTL
        self.history_api_cooldown_until = 0.0

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
                params = {"app_id": 730, "currency": "USD", "tradable": 0}

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
                                yield MarketTick(market_hash_name=market_hash_name, price_usd=float(item["min_price"]))

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

            except Exception as e:
                logger.error("Telemetry connection dropout encountered: %s", e)
                backoff_seconds = 305

            # Respect the 5-minute cache instruction or back off if rate limited
            logger.info("Entering calculated cooldown cycle for %d seconds...", backoff_seconds)
            await asyncio.sleep(backoff_seconds)

    async def verify_anomaly_with_history(self, market_hash_name: str, price_usd: float) -> bool:
        """
        Queries `/v1/sales/history` using the base name, filters for the matching version,
        and determines if the price represents a genuine discount (e.g. <= 5% discount at edge)
        adjusted for active downtrends to prevent alerting on structural market shifts.

        .. note::
           This method is currently unused in real-time execution. Anomaly verification is offloaded
           to O(1) Edge Redis baseline checks to preserve <5ms hot-path latency and avoid rate-limiting.
        """
        now = time.time()
        if now < self.history_api_cooldown_until:
            # During cooldown, skip verification (return False) to avoid dispatching unverified alerts
            return False

        try:
            base_name, version = parse_version_from_name(market_hash_name)

            cache_key = (base_name, version)
            target_entry = None

            # Check cache
            if cache_key in self.history_cache:
                ts, cached_entry = self.history_cache[cache_key]
                if now - ts < self.cache_ttl:
                    logger.info("Using cached sales history for '%s' (version: %s)", base_name, version)
                    target_entry = cached_entry

            if not target_entry:
                url = "https://api.skinport.com/v1/sales/history"
                params = {"app_id": 730, "currency": "USD", "market_hash_name": base_name}
                session = await self._get_session()

                async with session.get(
                    url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=5, connect=2),
                ) as response:
                    if response.status != 200:
                        logger.warning("Non-200 status fetching history for '%s': %s", base_name, response.status)
                        if response.status == 429:
                            logger.warning(
                                "Rate limit hit (429) fetching history for '%s'. Cooldown active for 60 seconds.",
                                base_name,
                            )
                            self.history_api_cooldown_until = now + 60.0
                        return False  # Skip unverifiable anomalies instead of blindly approving

                    data = await response.json()
                    if not isinstance(data, list) or len(data) == 0:
                        return True

                    # Find entry matching our specific version
                    for entry in data:
                        if entry.get("version") == version:
                            target_entry = entry
                            break

                    if not target_entry:
                        target_entry = data[0]

                    # Save in cache
                    self.history_cache[cache_key] = (now, target_entry)

            if not target_entry:
                return True

            # Resolve recent median using shared utility
            recent_median = resolve_recent_median(target_entry)

            if recent_median is None:
                return True

            # Edge pre-filter: 5% discount threshold (backend applies stricter 15% with downtrend penalties)
            base_discount = 0.95
            applied_discount = base_discount

            threshold = recent_median * applied_discount

            if price_usd <= threshold:
                logger.info(
                    "Verified! Price $%.2f <= Snipe Threshold $%.2f (Recent Median: $%.2f)",
                    price_usd,
                    threshold,
                    recent_median,
                )
                return True
            else:
                logger.info(
                    "Filtered out! Price $%.2f > Snipe Threshold $%.2f (Recent Median: $%.2f)",
                    price_usd,
                    threshold,
                    recent_median,
                )
                return False
        except Exception as e:
            logger.error("Error verifying anomaly for '%s': %s", market_hash_name, e)
            return False

    async def listen_websocket_stream(self) -> AsyncGenerator[MarketTick, None]:
        """
        Subscribes to the local Redis Pub/Sub channel 'skinport:live_listings'
        to ingest real-time listings relayed by the Node.js WebSocket sidecar.
        """
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6380))

        cache = Redis(host=redis_host, port=redis_port, decode_responses=True)
        pubsub = cache.pubsub()
        await pubsub.subscribe("skinport:live_listings")
        logger.info("Subscribed to Redis channel 'skinport:live_listings'")

        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                try:
                    data = json.loads(message["data"])
                    sales = data.get("sales", [])
                    for sale in sales:
                        market_hash_name = sale.get("marketHashName")
                        sale_price = sale.get("salePrice")

                        if market_hash_name and sale_price is not None:
                            # salePrice is in USD cents when currency is USD
                            price_usd = float(sale_price) / 100.0
                            wear = sale.get("wear")
                            stickers = sale.get("stickers", [])

                            # Handle version suffix using shared utility
                            version = sale.get("version")
                            market_hash_name = build_versioned_name(market_hash_name, version)

                            yield MarketTick(
                                market_hash_name=market_hash_name, price_usd=price_usd, float_value=wear, stickers=stickers
                            )
                except Exception as parse_err:
                    logger.error("Error parsing sidecar listing message: %s", parse_err)
        finally:
            await pubsub.unsubscribe("skinport:live_listings")
            await cache.aclose()
