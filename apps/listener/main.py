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
        "triggered_at": tick.timestamp,
        "float_value": tick.float_value,
        "stickers": tick.stickers
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

async def rest_poll_producer(scraper, queue: asyncio.Queue):
    """Periodically polls REST stream and puts ticks into the queue."""
    while True:
        try:
            async for tick in scraper.poll_market_stream():
                await queue.put(tick)
        except Exception as e:
            print(f"[REST POLL] Producer error: {e}. Retrying REST stream in 10 seconds...")
            await asyncio.sleep(10)

async def websocket_subscriber_producer(scraper, queue: asyncio.Queue):
    """Listens to real-time events from the platform's WebSocket stream relay and puts them into the queue."""
    if not hasattr(scraper, "listen_websocket_stream"):
        return
        
    while True:
        try:
            async for tick in scraper.listen_websocket_stream():
                await queue.put(tick)
        except Exception as e:
            print(f"[{scraper.platform_name.upper()} WS] Ingestion watchdog caught subscriber crash: {e}. Reconnecting in 10 seconds...")
            await asyncio.sleep(10)

async def tick_consumer(queue: asyncio.Queue, platform_target: str):
    cache = Redis(host="localhost", port=6379, decode_responses=True)
    scraper = ScraperFactory.get_scraper(platform_target)
    
    batch_buffer = []
    CHUNK_LIMIT = 2500  # Optimal payload size balancing network MTU and memory
    
    # Deduplication map: market_hash_name -> (timestamp, price_cents)
    dedup_cache = {}
    
    print("[CONSUMER] Telemetry processing consumer loop is active.")
    
    try:
        while True:
            tick = await queue.get()
            try:
                redis_key = f"market:ticks:{tick.market_hash_name}"
                
                # 1. Deduplication Filter
                last_ts, last_price = dedup_cache.get(tick.market_hash_name, (0, 0))
                # If price is unchanged and less than 5 minutes have elapsed, skip
                if tick.price_cents == last_price and (tick.timestamp - last_ts) < 300:
                    queue.task_done()
                    continue
                
                # Update deduplication state
                dedup_cache[tick.market_hash_name] = (tick.timestamp, tick.price_cents)
                
                # Accumulate records for long-term database tracking
                batch_buffer.append({
                    "market_hash_name": tick.market_hash_name,
                    "price_cents": tick.price_cents,
                    "timestamp": tick.timestamp
                })
                
                # 2. Update Volatile Sliding Cache Layer
                value_string = f"{tick.timestamp}:{tick.price_cents}"
                await cache.zadd(redis_key, {value_string: tick.timestamp})
                
                # Keep only the last 30 ticks to resolve the illiquidity Z-score trap
                card = await cache.zcard(redis_key)
                if card > 30:
                    await cache.zremrangebyrank(redis_key, 0, card - 31)
                
                # 3. Check Z-Score boundary constraints if enough local window history exists
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
                    
                    # Relaxed trigger threshold if item has applied stickers
                    sticker_count = len(tick.stickers)
                    threshold_z = -1.0 if sticker_count > 0 else -2.5
                    
                    if z_score < threshold_z:
                        sticker_tag = f" ({sticker_count} stickers)" if sticker_count > 0 else ""
                        print(f"[ANOMALY] Outlier potential detected: {tick.market_hash_name}{sticker_tag} at ${tick.price_usd:.2f} (Z={z_score:.2f}). Running edge history verification...")
                        
                        async def verify_and_dispatch(t=tick, z=z_score):
                            try:
                                is_valid = await scraper.verify_anomaly_with_history(t.market_hash_name, t.price_usd)
                                if is_valid:
                                    print(f"[ANOMALY] Confirmed true outlier! {t.market_hash_name} dropped to ${t.price_usd:.2f}. Dispatching to core compute...")
                                    await dispatch_anomaly_to_core(t, z)
                                else:
                                    print(f"[ANOMALY] False outlier filtered: {t.market_hash_name} at ${t.price_usd:.2f} is within acceptable historical bounds.")
                            except Exception as ve:
                                print(f"[ANOMALY] Error during edge history verification: {ve}. Dispatching to core as fallback.")
                                await dispatch_anomaly_to_core(t, z)
                        
                        asyncio.create_task(verify_and_dispatch())
        
                # 4. When buffer matches target density constraints, dispatch non-blocking task
                if len(batch_buffer) >= CHUNK_LIMIT:
                    asyncio.create_task(flush_batch_chunk_to_postgres(platform_target, batch_buffer.copy()))
                    batch_buffer.clear()
            except Exception as item_err:
                print(f"[CONSUMER] Error processing tick for '{tick.market_hash_name}': {item_err}")
            finally:
                queue.task_done()
    finally:
        await cache.aclose()

async def process_live_telemetry_stream(platform_target: str):
    print("======================================================================")
    print(f"Initializing Extensible Stream Engine: {platform_target.upper()}")
    print(f"Target Routing Node Core             : {COMPUTE_NODE_IP}:{COMPUTE_PORT}")
    print("======================================================================")
    
    queue = asyncio.Queue()
    scraper = ScraperFactory.get_scraper(platform_target)
    
    tasks = [
        asyncio.create_task(tick_consumer(queue, platform_target)),
        asyncio.create_task(rest_poll_producer(scraper, queue)),
        asyncio.create_task(websocket_subscriber_producer(scraper, queue))
    ]
        
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(process_live_telemetry_stream("skinport"))