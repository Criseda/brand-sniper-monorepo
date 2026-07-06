import asyncio
import json
import math
import os
import signal
import sys
from collections import OrderedDict
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from executor import PaperExecutor
from models import MarketTick
from redis.asyncio import Redis
from rules_engine import evaluate_opportunity
from scrapers.factory import ScraperFactory

# Force standard streams to use UTF-8 to support Unicode characters (like ★) on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from shared_utils import get_logger

logger = get_logger("listener.main")

# DYNAMIC NETWORK INFRASTRUCTURE CONFIGURATION
# Load root .env (shared) first, then listener-specific overrides
project_root = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=project_root / ".env")
listener_env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=listener_env_path, override=True)

# Pulls target node location from RAM environment, falling back to local loopback
COMPUTE_NODE_IP = os.getenv("COMPUTE_NODE_IP", "localhost")
COMPUTE_PORT = os.getenv("COMPUTE_NODE_PORT", "8080")

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


async def flush_batch_chunk_to_postgres(source: str, chunk: list[dict]):
    """Fires a non-blocking network transmission containing structured bulk arrays."""
    payload = {"source": source, "ticks": chunk}
    try:
        session = await get_http_session()
        async with session.post(BULK_INGEST_URL, json=payload, timeout=aiohttp.ClientTimeout(total=15, connect=3)) as resp:
            if resp.status == 201:
                logger.info("Successfully committed %d items to Compute Node.", len(chunk))
            else:
                logger.warning("Backend rejected batch with status: %s", resp.status)
    except Exception as e:
        logger.error("Failed to reach Compute Node database router: %s", e)


async def rest_poll_producer(scraper, queue: asyncio.Queue):
    """Periodically polls REST stream and puts ticks into the queue."""
    while True:
        try:
            async for tick in scraper.poll_market_stream():
                await queue.put(tick)
        except Exception as e:
            logger.warning("Producer error: %s. Retrying REST stream in 10 seconds...", e)
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
            logger.warning("Ingestion watchdog caught subscriber crash: %s. Reconnecting in 10 seconds...", e)
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
    variance = sum((x - mean_cents) ** 2 for x in historical_prices) / (n - 1) if n > 1 else 0.0
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


async def evaluate_and_execute(tick: MarketTick, z_score: float, mean_cents: float, cache: Redis):
    """Evaluates an anomaly locally on the edge and executes the trade if valid."""
    try:
        is_approved = await evaluate_opportunity(tick, cache)
        if is_approved:
            logger.info(
                "Confirmed true outlier by Edge DRE! %s dropped to $%.2f. Executing trade...",
                tick.market_hash_name,
                tick.price_usd,
            )

            baseline_raw = await cache.get(f"baseline:{tick.market_hash_name}")
            est_profit_cents = 0
            if baseline_raw:
                baseline = json.loads(baseline_raw)
                est_profit_cents = baseline.get("latest_price_cents", tick.price_cents) - tick.price_cents

            executor = PaperExecutor(f"http://{COMPUTE_NODE_IP}:{COMPUTE_PORT}")
            await executor.execute(
                market_hash_name=tick.market_hash_name,
                purchase_price_cents=tick.price_cents,
                estimated_profit_cents=est_profit_cents,
                z_score=z_score,
            )
        else:
            logger.info("False outlier filtered by Edge DRE: %s at $%.2f.", tick.market_hash_name, tick.price_usd)
    except Exception as e:
        logger.error("Edge DRE failure for %s: %s", tick.market_hash_name, e)


async def tick_consumer(queue: asyncio.Queue, platform_target: str, scraper):
    """Processes ticks from the queue: deduplicates, caches, detects anomalies, and batches for ingest."""
    cache = Redis(host="localhost", port=6380, decode_responses=True)

    batch_buffer = []
    dedup_cache: OrderedDict[str, float] = OrderedDict()

    logger.info("Telemetry processing consumer loop is active.")

    try:
        while True:
            tick = await queue.get()
            try:
                # 1. Deduplication Filter
                if is_duplicate(tick, dedup_cache):
                    continue
                update_dedup_cache(tick, dedup_cache)

                # Accumulate records for long-term database tracking
                batch_buffer.append(
                    {"market_hash_name": tick.market_hash_name, "price_cents": tick.price_cents, "timestamp": tick.timestamp}
                )

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
                raw_elements_list: list[str] = raw_elements  # type: ignore[assignment]
                prices = [int(element.split(":")[1]) for element in raw_elements_list]

                result = calculate_z_score(prices)
                if result is not None:
                    z_score, mean_cents = result

                    if should_trigger_anomaly(z_score, mean_cents, tick):
                        sticker_count = len(tick.stickers)
                        sticker_tag = f" ({sticker_count} stickers)" if sticker_count > 0 else ""
                        logger.info(
                            "Outlier potential detected: %s%s at $%.2f (Z=%.2f). Running Edge DRE...",
                            tick.market_hash_name,
                            sticker_tag,
                            tick.price_usd,
                            z_score,
                        )
                        asyncio.create_task(evaluate_and_execute(tick, z_score, mean_cents, cache))

                # 4. When buffer matches target density constraints, dispatch non-blocking task
                if len(batch_buffer) >= CHUNK_LIMIT:
                    asyncio.create_task(flush_batch_chunk_to_postgres(platform_target, batch_buffer.copy()))
                    batch_buffer.clear()
            except Exception as item_err:
                logger.error("Error processing tick for '%s': %s", tick.market_hash_name, item_err)
            finally:
                queue.task_done()
    finally:
        await cache.aclose()


async def start_sidecar_process(scraper):
    """Spawns the Node.js WebSocket scraper sidecar as an async subprocess and handles its lifetime."""
    sidecar_path = scraper.sidecar_script_path
    if not sidecar_path or not sidecar_path.exists():
        return

    logger.info("Spawning Node.js sidecar: %s", sidecar_path)
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            "node", str(sidecar_path), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        async def log_stream(stream, prefix):
            while True:
                line = await stream.readline()
                if not line:
                    break
                logger.info("%s%s", prefix, line.decode("utf-8").strip())

        asyncio.create_task(log_stream(proc.stdout, ""))
        asyncio.create_task(log_stream(proc.stderr, "[SKINPORT WS ERR] "))

        await proc.wait()
        logger.info("Node.js sidecar process exited with code %s", proc.returncode)
    except Exception as e:
        logger.error("Error running Node.js sidecar: %s", e)
    finally:
        if proc and proc.returncode is None:
            logger.info("Terminating Node.js sidecar process...")
            try:
                proc.terminate()
                await proc.wait()
            except Exception:
                pass


async def process_live_telemetry_stream(platform_target: str):
    logger.info("======================================================================")
    logger.info("Initializing Extensible Stream Engine: %s", platform_target.upper())
    logger.info("Target Routing Node Core             : %s:%s", COMPUTE_NODE_IP, COMPUTE_PORT)
    logger.info("======================================================================")

    queue: asyncio.Queue[MarketTick] = asyncio.Queue()
    scraper = ScraperFactory.get_scraper(platform_target)

    tasks = [
        asyncio.create_task(tick_consumer(queue, platform_target, scraper)),
        asyncio.create_task(rest_poll_producer(scraper, queue)),
        asyncio.create_task(websocket_subscriber_producer(scraper, queue)),
    ]

    if scraper.sidecar_script_path:
        tasks.append(asyncio.create_task(start_sidecar_process(scraper)))

    # Register graceful shutdown on SIGINT/SIGTERM
    shutdown_event = asyncio.Event()

    def _signal_handler():
        logger.info("Signal received. Cleaning up...")
        shutdown_event.set()

    if os.name != "nt":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _signal_handler)

    try:
        # Wait until shutdown signal or task failure
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        done, pending = await asyncio.wait(tasks + [shutdown_task], return_when=asyncio.FIRST_COMPLETED)
        # Cancel all remaining tasks
        for task in pending:
            task.cancel()
        # Allow cancellation to propagate
        await asyncio.gather(*pending, return_exceptions=True)
    finally:
        await close_http_session()
        logger.info("Cleanup complete.")


if __name__ == "__main__":
    platform_target = os.getenv("LISTENER_PLATFORM", "skinport")
    asyncio.run(process_live_telemetry_stream(platform_target))
