import asyncio
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List
import aiohttp
from dotenv import load_dotenv
from redis.asyncio import Redis

from scrapers.factory import ScraperFactory
from models import MarketTick

# DYNAMIC NETWORK INFRASTRUCTURE CONFIGURATION
# Load environment configuration from .env file
listener_env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=listener_env_path)

# Pulls target node location from RAM environment, falling back to local loopback
COMPUTE_NODE_IP = os.getenv("COMPUTE_NODE_IP", "localhost")
COMPUTE_PORT = os.getenv("COMPUTE_NODE_PORT", "8080")

ANOMALY_URL = f"http://{COMPUTE_NODE_IP}:{COMPUTE_PORT}/api/v1/alerts/anomaly"
BULK_INGEST_URL = f"http://{COMPUTE_NODE_IP}:{COMPUTE_PORT}/api/v1/ingest/bulk"

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
                    print(f"[LAN] Single alert dispatched for {tick.market_hash_name}")
                else:
                    print(f"[LAN] Warning: Backend rejected anomaly payload with status {resp.status}")
    except Exception as e:
        print(f"[LAN] Telemetry transmission warning (Compute node offline?): {e}")

async def flush_batch_chunk_to_postgres(source: str, chunk: List[dict]):
    """Fires a non-blocking network transmission containing structured bulk arrays."""
    payload = {"source": source, "ticks": chunk}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(BULK_INGEST_URL, json=payload, timeout=10.0) as resp:
                if resp.status == 201:
                    print(f"[BATCH FLUSH] Successfully committed {len(chunk)} items to Compute Node.")
                else:
                    print(f"[BATCH FLUSH] Backend rejected batch with status: {resp.status}")
    except Exception as e:
        print(f"[BATCH FLUSH] Failed to reach Compute Node database router: {e}")

async def process_live_telemetry_stream(platform_target: str):
    print("======================================================================")
    print(f"Initializing Extensible Stream Engine: {platform_target.upper()}")
    print(f"Target Routing Node Core             : {COMPUTE_NODE_IP}:{COMPUTE_PORT}")
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
        
        # Keep only the last 30 ticks to resolve the illiquidity Z-score trap
        card = await cache.zcard(redis_key)
        if card > 30:
            await cache.zremrangebyrank(redis_key, 0, card - 31)
        
        # 2. Check Z-Score boundary constraints if enough local window history exists
        raw_elements = await cache.zrange(redis_key, 0, -1)
        prices = [int(element.split(":")[1]) for element in raw_elements]
        
        if len(prices) >= 4:
            current_tick_price = prices[-1]
            historical_prices = prices[:-1]
            n = len(historical_prices)
            
            mean_cents = sum(historical_prices) / n
            variance = sum((x - mean_cents) ** 2 for x in historical_prices) / n
            std_dev = math.sqrt(variance)
            
            z_score = (current_tick_price - mean_cents) / std_dev if std_dev > 0 else 0.0
            
            if z_score < -2.5:
                # Perform historical verification on the edge node before dispatching
                print(f"[ANOMALY] Outlier potential detected: {tick.market_hash_name} at ${tick.price_usd:.2f} (Z={z_score:.2f}). Running edge history verification...")
                
                async def verify_and_dispatch():
                    try:
                        is_valid = await scraper.verify_anomaly_with_history(tick.market_hash_name, tick.price_usd)
                        if is_valid:
                            print(f"[ANOMALY] Confirmed true outlier! {tick.market_hash_name} dropped to ${tick.price_usd:.2f}. Dispatching to core compute...")
                            await dispatch_anomaly_to_core(tick, z_score)
                        else:
                            print(f"[ANOMALY] False outlier filtered: {tick.market_hash_name} at ${tick.price_usd:.2f} is within acceptable historical bounds.")
                    except Exception as ve:
                        print(f"[ANOMALY] Error during edge history verification: {ve}. Dispatching to core as fallback.")
                        await dispatch_anomaly_to_core(tick, z_score)
                
                asyncio.create_task(verify_and_dispatch())

        # 3. When buffer matches target density constraints, dispatch non-blocking task
        if len(batch_buffer) >= CHUNK_LIMIT:
            asyncio.create_task(flush_batch_chunk_to_postgres(platform_target, batch_buffer.copy()))
            batch_buffer.clear()

    await cache.aclose()

if __name__ == "__main__":
    asyncio.run(process_live_telemetry_stream("skinport"))