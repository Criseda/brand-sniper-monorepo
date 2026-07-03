import asyncio
import math
import os
import signal
import sys
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from redis.asyncio import Redis

from scrapers.factory import ScraperFactory
from models import MarketTick

# Force standard streams to use UTF-8 to support Unicode characters (like ★) on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# DYNAMIC NETWORK INFRASTRUCTURE CONFIGURATION
# Load environment configuration from .env file
listener_env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=listener_env_path)

# Pulls target node location from RAM environment, falling back to local loopback
COMPUTE_NODE_IP = os.getenv("COMPUTE_NODE_IP", "localhost")
COMPUTE_PORT = os.getenv("COMPUTE_NODE_PORT", "8080")

ANOMALY_URL = f"http://{COMPUTE_NODE_IP}:{COMPUTE_PORT}/api/v1/alerts/anomaly"
BULK_INGEST_URL = f"http://{COMPUTE_NODE_IP}:{COMPUTE_PORT}/api/v1/ingest/bulk"

# --- Tunable Detection Parameters (configurable via .env) ---
# Minimum absolute savings required for non-stickered anomaly validation
MIN_SAVINGS_USD = float(os.getenv("MIN_SAVINGS_USD", "0.75"))
MIN_SAVINGS_CENTS = round(MIN_SAVINGS_USD * 100)
# Z-score threshold for standard items
Z_SCORE_THRESHOLD = float(os.getenv("Z_SCORE_THRESHOLD", "-2.5"))
# Z-score threshold for stickered items (relaxed to catch sticker snipes)
Z_SCORE_STICKER_THRESHOLD = float(os.getenv("Z_SCORE_STICKER_THRESHOLD", "-1.0"))
# Minimum std dev regularization factor (prevents hyper-sensitivity on stable prices)
MIN_STD_DEV_FACTOR = float(os.getenv("MIN_STD_DEV_FACTOR", "0.04"))
# Sliding window size for Redis price history
SLIDING_WINDOW_SIZE = int(os.getenv("SLIDING_WINDOW_SIZE", "30"))
# Minimum data points required before Z-score analysis
MIN_HISTORY_POINTS = int(os.getenv("MIN_HISTORY_POINTS", "4"))
# Dedup cache max entries (LRU eviction above this cap)
DEDUP_CACHE_MAX_SIZE = int(os.getenv("DEDUP_CACHE_MAX_SIZE", "25000"))
# Batch buffer chunk limit for bulk ingest dispatches
CHUNK_LIMIT = int(os.getenv("CHUNK_LIMIT", "2500"))

# Shared aiohttp session (initialized at startup, closed at shutdown)
_http_session: aiohttp.ClientSession | None = None


async def get_http_session() -> aiohttp.ClientSession:
    """Returns the shared aiohttp session, creating it lazily if needed."""
    global _http_session
    if _http_session is None or _http_session.closed:
        timeout = aiohttp.ClientTimeout(total=10, connect=3)
        _http_session = aiohttp.ClientSession(timeout=timeout)
    return _http_session


async def close_http_session():
    """Closes the shared aiohttp session cleanly."""
    global _http_session
    if _http_session and not _http_session.closed:
        await _http_session.close()
        _http_session = None


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
        session = await get_http_session()
        async with session.post(ANOMALY_URL, json=payload, timeout=aiohttp.ClientTimeout(total=5, connect=2)) as resp:
            if resp.status == 202:
                print(f"[LAN] Single alert dispatched for {tick.market_hash_name}")
            else:
                print(f"[LAN] Warning: Backend rejected anomaly payload with status {resp.status}")
    except Exception as e:
        print(f"[LAN] Telemetry transmission warning (Compute node offline?): {e}")


async def flush_batch_chunk_to_postgres(source: str, chunk: list[dict]):
    """Fires a non-blocking network transmission containing structured bulk arrays."""
    payload = {"source": source, "ticks": chunk}
    try:
        session = await get_http_session()
        async with session.post(BULK_INGEST_URL, json=payload, timeout=aiohttp.ClientTimeout(total=15, connect=3)) as resp:
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


# --- Anomaly Detection Helpers ---

def is_duplicate(tick: MarketTick, dedup_cache: OrderedDict) -> bool:
    """Returns True if this tick is a duplicate (same price within the dedup window)."""
    last_ts, last_price = dedup_cache.get(tick.market_hash_name, (0, 0))
    return tick.price_cents == last_price and (tick.timestamp - last_ts) < 300


def update_dedup_cache(tick: MarketTick, dedup_cache: OrderedDict):
    """Updates the LRU dedup cache with the latest tick, evicting oldest if over capacity."""
    # Move to end if exists (LRU touch), or insert fresh
    if tick.market_hash_name in dedup_cache:
        dedup_cache.move_to_end(tick.market_hash_name)
    dedup_cache[tick.market_hash_name] = (tick.timestamp, tick.price_cents)
    # Evict oldest entries when cache exceeds capacity
    while len(dedup_cache) > DEDUP_CACHE_MAX_SIZE:
        dedup_cache.popitem(last=False)


def calculate_z_score(prices: list[int]) -> tuple[float, float] | None:
    """
    Calculates the Z-score of the most recent price against the historical window.
    Returns (z_score, mean_cents) or None if insufficient data.
    """
    if len(prices) < MIN_HISTORY_POINTS:
        return None

    current_tick_price = prices[-1]
    historical_prices = prices[:-1]
    n = len(historical_prices)

    mean_cents = sum(historical_prices) / n
    variance = sum((x - mean_cents) ** 2 for x in historical_prices) / n
    std_dev = math.sqrt(variance)

    # Regularize standard deviation to prevent hyper-sensitivity on low-variance histories
    min_std_dev = mean_cents * MIN_STD_DEV_FACTOR
    effective_std_dev = max(std_dev, min_std_dev)
    z_score = (current_tick_price - mean_cents) / effective_std_dev

    return z_score, mean_cents


def should_trigger_anomaly(z_score: float, mean_cents: float, tick: MarketTick) -> bool:
    """
    Determines if a Z-score outlier should proceed to history verification,
    applying sticker-aware thresholds and the absolute savings floor.
    """
    sticker_count = len(tick.stickers)
    threshold_z = Z_SCORE_STICKER_THRESHOLD if sticker_count > 0 else Z_SCORE_THRESHOLD

    if z_score >= threshold_z:
        return False

    # Enforce absolute savings floor on non-stickered items to filter micro-value spam
    if sticker_count == 0:
        savings_cents = mean_cents - tick.price_cents
        if savings_cents < MIN_SAVINGS_CENTS:
            return False

    return True


async def verify_and_dispatch(tick: MarketTick, z_score: float, scraper):
    """Verifies an anomaly candidate against historical data and dispatches if confirmed."""
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


async def tick_consumer(queue: asyncio.Queue, platform_target: str, scraper):
    """Processes ticks from the queue: deduplicates, caches, detects anomalies, and batches for ingest."""
    cache = Redis(host="localhost", port=6380, decode_responses=True)
    
    batch_buffer = []
    dedup_cache = OrderedDict()
    
    print("[CONSUMER] Telemetry processing consumer loop is active.")
    
    try:
        while True:
            tick = await queue.get()
            try:
                # 1. Deduplication Filter
                if is_duplicate(tick, dedup_cache):
                    continue
                update_dedup_cache(tick, dedup_cache)
                
                # Accumulate records for long-term database tracking
                batch_buffer.append({
                    "market_hash_name": tick.market_hash_name,
                    "price_cents": tick.price_cents,
                    "timestamp": tick.timestamp
                })
                
                # 2. Update Volatile Sliding Cache Layer
                redis_key = f"market:ticks:{tick.market_hash_name}"
                value_string = f"{tick.timestamp}:{tick.price_cents}"
                await cache.zadd(redis_key, {value_string: tick.timestamp})
                
                # Keep only the last N ticks to resolve the illiquidity Z-score trap
                card = await cache.zcard(redis_key)
                if card > SLIDING_WINDOW_SIZE:
                    await cache.zremrangebyrank(redis_key, 0, card - SLIDING_WINDOW_SIZE - 1)
                
                # 3. Z-Score anomaly detection
                raw_elements = await cache.zrange(redis_key, 0, -1)
                prices = [int(element.split(":")[1]) for element in raw_elements]
                
                result = calculate_z_score(prices)
                if result is not None:
                    z_score, mean_cents = result
                    
                    if should_trigger_anomaly(z_score, mean_cents, tick):
                        sticker_count = len(tick.stickers)
                        sticker_tag = f" ({sticker_count} stickers)" if sticker_count > 0 else ""
                        print(f"[ANOMALY] Outlier potential detected: {tick.market_hash_name}{sticker_tag} at ${tick.price_usd:.2f} (Z={z_score:.2f}). Running edge history verification...")
                        asyncio.create_task(verify_and_dispatch(tick, z_score, scraper))
    
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


async def start_sidecar_process(scraper):
    """Spawns the Node.js WebSocket scraper sidecar as an async subprocess and handles its lifetime."""
    sidecar_path = scraper.sidecar_script_path
    if not sidecar_path or not sidecar_path.exists():
        return

    print(f"[AGENT] Spawning Node.js sidecar: {sidecar_path}")
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            "node",
            str(sidecar_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        async def log_stream(stream, prefix):
            while True:
                line = await stream.readline()
                if not line:
                    break
                print(f"{prefix}{line.decode('utf-8').strip()}")
                
        asyncio.create_task(log_stream(proc.stdout, ""))
        asyncio.create_task(log_stream(proc.stderr, "[SKINPORT WS ERR] "))
        
        await proc.wait()
        print(f"[AGENT] Node.js sidecar process exited with code {proc.returncode}")
    except Exception as e:
        print(f"[AGENT] Error running Node.js sidecar: {e}")
    finally:
        if proc and proc.returncode is None:
            print("[AGENT] Terminating Node.js sidecar process...")
            try:
                proc.terminate()
                await proc.wait()
            except Exception:
                pass


async def process_live_telemetry_stream(platform_target: str):
    print("======================================================================")
    print(f"Initializing Extensible Stream Engine: {platform_target.upper()}")
    print(f"Target Routing Node Core             : {COMPUTE_NODE_IP}:{COMPUTE_PORT}")
    print("======================================================================")
    
    queue = asyncio.Queue()
    scraper = ScraperFactory.get_scraper(platform_target)
    
    tasks = [
        asyncio.create_task(tick_consumer(queue, platform_target, scraper)),
        asyncio.create_task(rest_poll_producer(scraper, queue)),
        asyncio.create_task(websocket_subscriber_producer(scraper, queue))
    ]
    
    if scraper.sidecar_script_path:
        tasks.append(asyncio.create_task(start_sidecar_process(scraper)))

    # Register graceful shutdown on SIGINT/SIGTERM
    shutdown_event = asyncio.Event()

    def _signal_handler():
        print("\n[SHUTDOWN] Signal received. Cleaning up...")
        shutdown_event.set()

    if os.name != "nt":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _signal_handler)

    try:
        # Wait until shutdown signal or task failure
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        done, pending = await asyncio.wait(
            tasks + [shutdown_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        # Cancel all remaining tasks
        for task in pending:
            task.cancel()
        # Allow cancellation to propagate
        await asyncio.gather(*pending, return_exceptions=True)
    finally:
        await close_http_session()
        print("[SHUTDOWN] Cleanup complete.")

if __name__ == "__main__":
    asyncio.run(process_live_telemetry_stream("skinport"))