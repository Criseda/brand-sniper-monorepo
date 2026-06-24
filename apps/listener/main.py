import asyncio
import math
from datetime import datetime, timezone
import aiohttp
from redis.asyncio import Redis

from scrapers.factory import ScraperFactory
from models import MarketTick

COMPUTE_NODE_URL = "http://localhost:8080/api/v1/alerts/anomaly"

async def dispatch_anomaly_to_core(tick: MarketTick, z_score: float):
    """Asynchronously pipes discovered flash-crashes to the compute machine core."""
    payload = {
        "market_hash_name": tick.market_hash_name,
        "price_usd": tick.price_usd,
        "price_cents": tick.price_cents,
        "z_score": round(z_score, 4),
        "triggered_at": tick.timestamp
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(COMPUTE_NODE_URL, json=payload, timeout=2.0) as resp:
                if resp.status == 202:
                    print(f"[LAN DISPATCH] Core Node accepted deal for {tick.market_hash_name}!")
        except Exception:
            print("[LAN DISPATCH] Core Node communication failure. Check backend server status.")

async def process_live_telemetry_stream(platform_target: str):
    print("======================================================================")
    print(f"Initializing Live Edge Stream Engine Target: {platform_target.upper()}")
    print("======================================================================")
    
    cache = Redis(host="localhost", port=6379, decode_responses=True)
    
    # Dynamically resolve the client engine via our Factory registry
    scraper = ScraperFactory.get_scraper(platform_target)
    
    # Ingest incoming ticks asynchronously as they fly off the provider network wire
    item_counter = 0
    async for tick in scraper.poll_market_stream():
        redis_key = f"market:ticks:{tick.market_hash_name}"
        current_time = int(datetime.now(timezone.utc).timestamp())
        
        # 1. Store the newest data vector in Redis RAM
        value_string = f"{tick.timestamp}:{tick.price_cents}"
        await cache.zadd(redis_key, {value_string: tick.timestamp})
        
        # 2. Enforce the sliding timeline window guardrail (Drop > 5 mins old)
        await cache.zremrangebyscore(redis_key, "-inf", current_time - 300)
        
        # 3. Pull active window history to compute metrics
        raw_elements = await cache.zrange(redis_key, 0, -1)
        prices = [int(element.split(":")[1]) for element in raw_elements]
        
        item_counter += 1
        if len(prices) < 3:
            # SILENCED: Prevents thousands of initialization lines on startup
            # print(f"[{tick.market_hash_name}] Seeding window data...")
            continue
            
        # 4. Process real-time anomaly isolation math
        current_tick_price = prices[-1]
        historical_prices = prices[:-1]
        n = len(historical_prices)
        
        mean_cents = sum(historical_prices) / n
        variance = sum((x - mean_cents) ** 2 for x in historical_prices) / n
        std_dev = math.sqrt(variance)
        
        z_score = (current_tick_price - mean_cents) / std_dev if std_dev > 0 else 0.0
        
        # 5. Evaluate actionable signal boundaries
        if z_score < -2.5:
            # Keep this active! This is what we care about.
            print(f"[ANOMALY] Outlier spotted on {tick.market_hash_name}! Price dropped to ${tick.price_usd:.2f} (Z={z_score:.2f})")
            await dispatch_anomaly_to_core(tick, z_score)
        # else:
            # SILENCED: Prevents steady-state market noise from flooding the screen
            # print(f"[NOMINAL] {tick.market_hash_name:<30} | Price: ${tick.price_usd:<7.2f}")

    await cache.aclose()

if __name__ == "__main__":
    # Launch tracking Skinport. Swapping to another marketplace in the future is now a 1-word adjustment.
    asyncio.run(process_live_telemetry_stream("skinport"))