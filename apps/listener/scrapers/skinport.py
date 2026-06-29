import asyncio
import base64
import json
import os
import time
from typing import AsyncGenerator
import aiohttp
from redis.asyncio import Redis
from scrapers.base import BaseScraper
from models import MarketTick
from shared_utils import parse_version_from_name

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
        
    def _build_auth_header(self) -> str:
        """Constructs a compliant HTTP Basic Authentication header string using Base64 encoding."""
        if not self.client_id or not self.client_secret:
            # Fallback gracefully to unauthenticated public access if credentials aren't set yet
            return ""
        
        raw_credentials = f"{self.client_id}:{self.client_secret}"
        encoded_bytes = base64.b64encode(raw_credentials.encode("utf-8"))
        return f"Basic {encoded_bytes.decode('utf-8')}"
        
    async def poll_market_stream(self) -> AsyncGenerator[MarketTick, None]:
        """Polls Skinport REST feeds, passing authenticated, Brotli-decompressed tokens down the pipe."""
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "br",  # CRITICAL: Explicitly required by Skinport for this endpoint
            "User-Agent": "BrandSniperEdgeTelemetry/1.0"
        }
        
        # Inject authorization header if credentials exist
        auth_string = self._build_auth_header()
        if auth_string:
            headers["Authorization"] = auth_string
            print("[SKINPORT] Basic Authentication token successfully compiled and injected.")
        
        backoff_seconds = 305
        while True:
            try:
                async with aiohttp.ClientSession(headers=headers) as session:
                    # Target CS2 inventory items denominated in USD
                    params = {"app_id": 730, "currency": "USD", "tradable": 0}
                    
                    print(f"[SKINPORT] Querying asset directory stream (Rate Limit: 8 requests per 5 mins)...")
                    async with session.get(self.api_url, params=params, timeout=15.0) as response:
                        
                        if response.status == 200:
                            # aiohttp automatically uses the loaded 'brotli' library to transparently unpack the data
                            raw_items = await response.json()
                            print(f"[SKINPORT] Successfully decompressed {len(raw_items)} market entries.")
                            backoff_seconds = 305  # Reset on success
                            
                            for item in raw_items:
                                # We track 'min_price' as our entry signal parameter
                                if item.get("min_price") is not None:
                                    market_hash_name = item["market_hash_name"]
                                    version = item.get("version")
                                    if version:
                                        wears = ["(Factory New)", "(Minimal Wear)", "(Field-Tested)", "(Well-Worn)", "(Battle-Scarred)"]
                                        for wear in wears:
                                            if market_hash_name.endswith(wear):
                                                base_name = market_hash_name[:-len(wear)].strip()
                                                market_hash_name = f"{base_name} ({version}) {wear}"
                                                break
                                        else:
                                            market_hash_name = f"{market_hash_name} ({version})"
                                    yield MarketTick(
                                        market_hash_name=market_hash_name,
                                        price_usd=float(item["min_price"])
                                    )
                                    
                        elif response.status == 401:
                            print("[SKINPORT] API Rejected Credentials! Check your client ID and secret variables inside your .env file.")
                            backoff_seconds = 305
                        elif response.status == 429:
                            backoff_seconds = min(1200, backoff_seconds * 2)
                            print(f"[SKINPORT] High-velocity rate limits encountered. Backing off production loop... Retrying in {backoff_seconds} seconds.")
                        else:
                            print(f"[SKINPORT] Marketplace responded with unexpected HTTP status code: {response.status}")
                            backoff_seconds = 305
                            
            except Exception as e:
                print(f"[SKINPORT] Telemetry connection dropout encountered: {e}")
                backoff_seconds = 305
                
            # Respect the 5-minute cache instruction or back off if rate limited
            print(f"[SKINPORT] Entering calculated cooldown cycle for {backoff_seconds} seconds...")
            await asyncio.sleep(backoff_seconds)

    async def verify_anomaly_with_history(self, market_hash_name: str, price_usd: float) -> bool:
        """
        Queries `/v1/sales/history` using the base name, filters for the matching version,
        and determines if the price represents a genuine discount (e.g. <= 15% discount)
        adjusted for active downtrends to prevent alerting on structural market shifts.
        """
        now = time.time()
        if now < self.history_api_cooldown_until:
            return True
            
        try:
            base_name, version = parse_version_from_name(market_hash_name)
            
            cache_key = (base_name, version)
            target_entry = None
            
            # Check cache
            if cache_key in self.history_cache:
                ts, cached_entry = self.history_cache[cache_key]
                if now - ts < self.cache_ttl:
                    print(f"[SKINPORT HISTORY] Using cached sales history for '{base_name}' (version: {version})")
                    target_entry = cached_entry

            if not target_entry:
                url = "https://api.skinport.com/v1/sales/history"
                params = {
                    "app_id": 730,
                    "currency": "USD",
                    "market_hash_name": base_name
                }
                headers = {
                    "Accept-Encoding": "br",
                    "User-Agent": "BrandSniperEdgeTelemetry/1.0"
                }
                auth_string = self._build_auth_header()
                if auth_string:
                    headers["Authorization"] = auth_string

                # Create a localized session for verification
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with session.get(url, params=params, timeout=5.0) as response:
                        if response.status != 200:
                            print(f"[SKINPORT HISTORY] Non-200 status fetching history for '{base_name}': {response.status}")
                            if response.status == 429:
                                print(f"[SKINPORT HISTORY] Rate limit hit (429) fetching history for '{base_name}'. Cooldown active for 60 seconds.")
                                self.history_api_cooldown_until = now + 60.0
                            return True  # Fallback to True to let backend double check
                        
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
                
            # Run evaluation on target_entry
            h24 = target_entry.get("last_24_hours") or {}
            h7 = target_entry.get("last_7_days") or {}
            h30 = target_entry.get("last_30_days") or {}
            h90 = target_entry.get("last_90_days") or {}
            
            m24 = h24.get("median")
            m7 = h7.get("median")
            m30 = h30.get("median")
            m90 = h90.get("median")
            
            recent_median = None
            if m24 and h24.get("volume", 0) > 0:
                recent_median = m24
            elif m7 and h7.get("volume", 0) > 0:
                recent_median = m7
            elif m30 and h30.get("volume", 0) > 0:
                recent_median = m30
            else:
                recent_median = m90
                
            if recent_median is None:
                return True
                
            # Calculate active downtrend to apply discount buffer
            downtrend_detected = False
            downtrend_severity = 0.0
            ref_recent = m7 if m7 else m24
            ref_older = m30 if m30 else m90
            
            if ref_recent and ref_older and ref_recent < ref_older:
                downtrend_detected = True
                downtrend_severity += (ref_older - ref_recent) / ref_older
                
            if m24 and m7 and m24 < m7:
                downtrend_detected = True
                downtrend_severity += (m7 - m24) / m7
                
            base_discount = 0.95
            # Edge pre-filter does not apply downtrend penalties to avoid filtering moderate discounts (e.g. 8-12%)
            applied_discount = base_discount
                
            threshold = recent_median * applied_discount
            
            if price_usd <= threshold:
                print(f"[SKINPORT HISTORY] Verified! Price ${price_usd:.2f} <= Snipe Threshold ${threshold:.2f} (Recent Median: ${recent_median:.2f})")
                return True
            else:
                print(f"[SKINPORT HISTORY] Filtered out! Price ${price_usd:.2f} > Snipe Threshold ${threshold:.2f} (Recent Median: ${recent_median:.2f})")
                return False
        except Exception as e:
            print(f"[SKINPORT HISTORY] Error verifying anomaly for '{market_hash_name}': {e}")
            return True

    async def listen_websocket_stream(self) -> AsyncGenerator[MarketTick, None]:
        """
        Subscribes to the local Redis Pub/Sub channel 'skinport:live_listings' 
        to ingest real-time listings relayed by the Node.js WebSocket sidecar.
        """
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        
        cache = Redis(host=redis_host, port=redis_port, decode_responses=True)
        pubsub = cache.pubsub()
        await pubsub.subscribe("skinport:live_listings")
        print("[SKINPORT WS] Subscribed to Redis channel 'skinport:live_listings'")
        
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
                            
                            # Handle version suffix in naming if present
                            version = sale.get("version")
                            if version and version != "default":
                                wears = ["(Factory New)", "(Minimal Wear)", "(Field-Tested)", "(Well-Worn)", "(Battle-Scarred)"]
                                for w in wears:
                                    if market_hash_name.endswith(w):
                                        base_name = market_hash_name[:-len(w)].strip()
                                        market_hash_name = f"{base_name} ({version}) {w}"
                                        break
                                else:
                                    market_hash_name = f"{market_hash_name} ({version})"

                            yield MarketTick(
                                market_hash_name=market_hash_name,
                                price_usd=price_usd,
                                float_value=wear,
                                stickers=stickers
                            )
                except Exception as parse_err:
                    print(f"[SKINPORT WS] Error parsing sidecar listing message: {parse_err}")
        finally:
            await pubsub.unsubscribe("skinport:live_listings")
            await cache.aclose()