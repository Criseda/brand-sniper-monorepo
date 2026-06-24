import asyncio
import math
from datetime import datetime, timezone
from typing import List
import aiohttp
from redis.asyncio import Redis

from scrapers.factory import ScraperFactory
from models import MarketTick

# Core Endpoints Mapping
COMPUTE_NODE_IP = "localhost"  # Update to your machine's local IP when separated
ANOMALY_URL = f"http://{COMPUTE_NODE_IP}:8080/api/v1/alerts/anomaly"
BULK_INGEST_URL = f"http://{COMPUTE_NODE_IP}:8080/api/v1/ingest/bulk"

async def dispatch_anomaly_to_core(tick: MarketTick, z_score: float):
    """Pipes an isolated flash-crash alert directly to the backend processing queue."""
    payload = {
        "market_hash_name": tick.market_hash_name,
        "price_usd": tick.price_usd,
        "price_cents": tick.price_cents,
        "z_score": round(z_score, 4),
        "triggered_at": tick.timestamp
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(ANOMALY_URL, json=payload, timeout=2.0) as resp:
                if resp.status == 202:
                    print(f"📡 [LAN] Single alert dispatched for {tick.market_hash_name}")
    except Exception:
        pass # Protect edge loops from network drops

async def flush_batch_chunk_to_postgres(source: str, chunk: List[dict]):
    """Fires a non-blocking network transmission containing structured bulk arrays."""
    payload = {"source": source, "ticks": chunk}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(BULK_INGEST_URL, json=payload, timeout=10.0) as resp:
                if resp.status == 201:
                    print(f"⚡ [BATCH FLUSH] Successfully committed {len(chunk)} items to Compute Node.")
                else:
                    print(f"⚠️  [BATCH FLUSH] Backend rejected batch with status: {resp.status}")
    except Exception as e:
        print(f"❌ [BATCH FLUSH] Failed to reach Compute Node database router: {e}")

async def process_live_telemetry_stream(platform_target: str):
    print("======================================================================")
    print(f"🚀 Initializing Extensible Stream Engine: {platform_target.upper()}")
    print("======================================================================")
    
    cache = Redis(host="localhost", port=6379, decode_responses=True)
    scraper = ScraperFactory.get_scraper(platform_target)
    
    batch_buffer = []
    CHUNK_LIMIT = 2500  # Optimal payload size balancing network MTU and memory
    
    async for tick in scraper.poll_market_stream():
        redis_key = f"market:ticks:{tick.market_hash_name}"
        current_time = int(datetime.now(timezone.utc).timestamp())
        
        # Accumulate records for long-term database tracking
        batch_buffer.append({
            "market_hash_name": tick.market_hash_name,
            "price_cents": tick.price_cents,
            "timestamp": tick.timestamp
        })
        
        # 1. Update Volatile Sliding Cache Layer
        value_string = f"{tick.timestamp}:{tick.price_cents}"
        await cache.zadd(redis_key, {value_string: tick.timestamp})
        await cache.zremrangebyscore(redis_key, "-inf", current_time - 300)
        
        # 2. Check Z-Score boundary constraints if enough local window history exists
        raw_elements = await cache.zrange(redis_key, 0, -1)
        prices = [int(element.split(":")[1]) for element in raw_elements]
        
        if len(prices) >= 3:
            current_tick_price = prices[-1]
            historical_prices = prices[:-1]
            n = len(historical_prices)
            
            mean_cents = sum(historical_prices) / n
            variance = sum((x - mean_cents) ** 2 for x in historical_prices) / n
            std_dev = math.sqrt(variance)
            
            z_score = (current_tick_price - mean_cents) / std_dev if std_dev > 0 else 0.0
            
            if z_score < -2.5:
                print(f"🚨 [ANOMALY] Outlier spotted! {tick.market_hash_name} dropped to ${tick.price_usd:.2f}")
                asyncio.create_task(dispatch_anomaly_to_core(tick, z_score))

        # 3. When buffer matches target density constraints, dispatch non-blocking task
        if len(batch_buffer) >= CHUNK_LIMIT:
            asyncio.create_task(flush_batch_chunk_to_postgres(platform_target, batch_buffer.copy()))
            batch_buffer.clear()

    await cache.aclose()

if __name__ == "__main__":
    asyncio.run(process_live_telemetry_stream("skinport"))