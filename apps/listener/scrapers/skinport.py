import asyncio
import base64
import os
from typing import AsyncGenerator
import aiohttp
from scrapers.base import BaseScraper
from models import MarketTick

class SkinportScraper(BaseScraper):
    """Production Ingestion Engine for Skinport utilizing Basic Auth and mandatory Brotli compression."""
    
    def __init__(self):
        super().__init__(platform_name="skinport")
        self.api_url = "https://api.skinport.com/v1/items"
        
        # Pull secure platform credentials out of environment variables
        self.client_id = os.getenv("SKINPORT_CLIENT_ID")
        self.client_secret = os.getenv("SKINPORT_CLIENT_SECRET")
        
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
                            
                            for item in raw_items:
                                # We track 'min_price' as our entry signal parameter
                                if item.get("min_price") is not None:
                                    yield MarketTick(
                                        market_hash_name=item["market_hash_name"],
                                        price_usd=float(item["min_price"])
                                    )
                                    
                        elif response.status == 401:
                            print("[SKINPORT] API Rejected Credentials! Check your client ID and secret variables inside your .env file.")
                        elif response.status == 429:
                            print("[SKINPORT] High-velocity rate limits encountered. Backing off production loop...")
                        else:
                            print(f"[SKINPORT] Marketplace responded with unexpected HTTP status code: {response.status}")
                            
            except Exception as e:
                print(f"[SKINPORT] Telemetry connection dropout encountered: {e}")
                
            # Respect the 5-minute cache instruction documented by Skinport
            # 305 seconds ensures we sit just outside the cache expiration boundary to maximize optimization
            print("[SKINPORT] Entering calculated cooldown cycle until next cache window refresh...")
            await asyncio.sleep(305)