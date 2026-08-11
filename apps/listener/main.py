import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from collections import OrderedDict
from functools import partial
from pathlib import Path
from uuid import uuid4

import aiohttp
from aiohttp import web
from dotenv import load_dotenv

# Force standard streams to use UTF-8 to support Unicode characters (like ★) on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Load root .env (shared) first, then listener-specific overrides.
project_root = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=project_root / ".env")
listener_env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=listener_env_path, override=True)

from batch_delivery import RedisBatchStore, StoredBatch, deliver_stored_batch
from executor import ExecutionService, PaperExecutor
from executor import close_http_session as close_executor_http_session
from listener_telemetry import (
    anomalies_confirmed_total,
    anomalies_detected_total,
    anomalies_rejected_total,
    batch_buffer_size,
    dedup_cache_size,
    redis_operation_latency_seconds,
    rules_engine_latency_seconds,
    tick_queue_size,
    ticks_deduplicated_total,
    ticks_processed_total,
)
from models import MarketTick
from prometheus_client import start_http_server
from redis.asyncio import Redis
from rules_engine import evaluate_opportunity
from scrapers.factory import ScraperFactory
from shared_utils import backend_api_headers, get_backend_api_key, get_logger
from task_supervisor import BoundedTaskPool
from zscore import calculate_z_score, should_trigger_anomaly

logger = get_logger("listener.main")

# Pulls target node location from RAM environment, falling back to local loopback
COMPUTE_NODE_IP = os.getenv("COMPUTE_NODE_IP", "localhost")
COMPUTE_PORT = os.getenv("COMPUTE_NODE_PORT", "8080")

BULK_INGEST_URL = f"http://{COMPUTE_NODE_IP}:{COMPUTE_PORT}/api/v1/ingest/bulk"


def _positive_int_env(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return value


def _positive_float_env(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0")
    return value


# --- Tunable Detection Parameters (configurable via .env) ---
# Sliding window size for Redis price history
SLIDING_WINDOW_SIZE = int(os.getenv("SLIDING_WINDOW_SIZE", "20"))
# Dedup cache max entries (LRU eviction above this cap)
DEDUP_CACHE_MAX_SIZE = int(os.getenv("DEDUP_CACHE_MAX_SIZE", "25000"))
# Batch buffer chunk limit for bulk ingest dispatches
CHUNK_LIMIT = int(os.getenv("CHUNK_LIMIT", "2500"))
# End-to-end backpressure and bounded background-work settings.
TICK_QUEUE_SIZE = _positive_int_env("LISTENER_TICK_QUEUE_SIZE", 10000)
ANOMALY_WORKERS = _positive_int_env("LISTENER_ANOMALY_WORKERS", 4)
ANOMALY_QUEUE_SIZE = _positive_int_env("LISTENER_ANOMALY_QUEUE_SIZE", 128)
BATCH_WORKERS = _positive_int_env("LISTENER_BATCH_WORKERS", 2)
BATCH_QUEUE_SIZE = _positive_int_env("LISTENER_BATCH_QUEUE_SIZE", 8)
SHUTDOWN_GRACE_SECONDS = _positive_int_env("LISTENER_SHUTDOWN_GRACE_SECONDS", 30)
BATCH_MAX_ATTEMPTS = _positive_int_env("LISTENER_BATCH_MAX_ATTEMPTS", 5)
BATCH_RETRY_BASE_SECONDS = _positive_float_env("LISTENER_BATCH_RETRY_BASE_SECONDS", 0.5)
BATCH_RETRY_MAX_SECONDS = _positive_float_env("LISTENER_BATCH_RETRY_MAX_SECONDS", 8)
LISTENER_HEALTH_PORT = _positive_int_env("LISTENER_HEALTH_PORT", 9101)

# Shared aiohttp session (initialized at startup, closed at shutdown)
_http_session: aiohttp.ClientSession | None = None
DedupCache = OrderedDict[str, tuple[int, int]]


async def get_http_session() -> aiohttp.ClientSession:
    """Returns the shared aiohttp session, creating it lazily if needed."""
    global _http_session
    if _http_session is None or _http_session.closed:
        timeout = aiohttp.ClientTimeout(total=10, connect=3)
        _http_session = aiohttp.ClientSession(headers=backend_api_headers(), timeout=timeout)
    return _http_session


async def close_http_session() -> None:
    """Closes the shared aiohttp session cleanly."""
    global _http_session
    if _http_session and not _http_session.closed:
        await _http_session.close()
        _http_session = None


async def listener_health(_request: web.Request) -> web.Response:
    """Respond from the listener event loop so a successful probe proves loop liveness."""
    return web.json_response({"status": "healthy"})


def create_listener_health_app() -> web.Application:
    """Create the listener's container-internal health application."""
    app = web.Application()
    app.router.add_get("/health", listener_health)
    return app


async def start_listener_health_server(port: int) -> web.AppRunner:
    """Start the container-internal listener health endpoint."""
    runner = web.AppRunner(create_listener_health_app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    logger.info("[HEALTH] Listener health endpoint listening on 127.0.0.1:%d/health", port)
    return runner


def _decode_zset_element(element: str | bytes) -> str:
    """Decodes a Redis sorted-set member to str regardless of client decode mode."""
    return element.decode("utf-8") if isinstance(element, bytes) else element


async def schedule_stored_batch(
    batch: StoredBatch,
    store: RedisBatchStore,
    batch_pool: BoundedTaskPool,
) -> None:
    """Schedule delivery for a batch that is already durable in Redis."""
    await batch_pool.submit(
        partial(
            deliver_stored_batch,
            store,
            batch,
            url=BULK_INGEST_URL,
            session_factory=get_http_session,
            max_attempts=BATCH_MAX_ATTEMPTS,
            base_delay_seconds=BATCH_RETRY_BASE_SECONDS,
            max_delay_seconds=BATCH_RETRY_MAX_SECONDS,
        )
    )


async def flush_batch_buffer(
    source: str,
    buffer: list[dict],
    *,
    batch_id: str,
    store: RedisBatchStore,
    batch_pool: BoundedTaskPool,
) -> None:
    """Persist a stable batch, transfer buffer ownership, then schedule delivery."""
    snapshot = buffer.copy()
    batch = await store.add(source, snapshot, batch_id=batch_id)
    buffer.clear()
    batch_buffer_size.set(0)
    try:
        await schedule_stored_batch(batch, store, batch_pool)
    except asyncio.CancelledError:
        logger.warning("[BATCH FLUSH] Batch %s remains pending after scheduling was cancelled.", batch.batch_id)
        raise
    except Exception:
        logger.exception("[BATCH FLUSH] Batch %s remains pending after scheduling failed.", batch.batch_id)
        raise


async def recover_pending_batches(store: RedisBatchStore, batch_pool: BoundedTaskPool) -> int:
    """Reschedule batches left pending by an earlier listener process."""
    recovered = 0
    async for batch in store.iter_pending():
        await schedule_stored_batch(batch, store, batch_pool)
        recovered += 1
    if recovered:
        logger.warning("[BATCH FLUSH] Recovered %d pending batches from Redis.", recovered)
    return recovered


async def rest_poll_producer(scraper, queue: asyncio.Queue[MarketTick | None]) -> None:
    """Periodically polls REST stream and puts ticks into the queue."""
    while True:
        try:
            async for tick in scraper.poll_market_stream():
                await queue.put(tick)
                tick_queue_size.set(queue.qsize())
        # Broad on purpose: supervisor loop must survive any transient failure and retry.
        except Exception as e:
            logger.warning("Producer error: %s. Retrying REST stream in 10 seconds...", e)
            await asyncio.sleep(10)


async def websocket_subscriber_producer(scraper, queue: asyncio.Queue[MarketTick | None]) -> None:
    """Listens to real-time events from the platform's WebSocket stream relay and puts them into the queue."""
    if not hasattr(scraper, "listen_websocket_stream"):
        return

    while True:
        try:
            async for tick in scraper.listen_websocket_stream():
                await queue.put(tick)
                tick_queue_size.set(queue.qsize())
        # Broad on purpose: supervisor loop must survive any transient failure and retry.
        except Exception as e:
            logger.warning("Ingestion watchdog caught subscriber crash: %s. Reconnecting in 10 seconds...", e)
            await asyncio.sleep(10)


# --- Anomaly Detection Helpers ---


def is_duplicate(tick: MarketTick, dedup_cache: DedupCache) -> bool:
    """Returns True if this tick is a duplicate (same price within the dedup window)."""
    last_ts, last_price = dedup_cache.get(tick.market_hash_name, (0, 0))
    return tick.price_cents == last_price and (tick.timestamp - last_ts) < 300


def update_dedup_cache(tick: MarketTick, dedup_cache: DedupCache) -> None:
    """Updates the LRU dedup cache with the latest tick, evicting oldest if over capacity."""
    # Move to end if exists (LRU touch), or insert fresh
    if tick.market_hash_name in dedup_cache:
        dedup_cache.move_to_end(tick.market_hash_name)
    dedup_cache[tick.market_hash_name] = (tick.timestamp, tick.price_cents)
    # Evict oldest entries when cache exceeds capacity
    while len(dedup_cache) > DEDUP_CACHE_MAX_SIZE:
        dedup_cache.popitem(last=False)


async def evaluate_and_execute(
    tick: MarketTick,
    z_score: float,
    cache: Redis,
    executor: ExecutionService,
    baseline: dict | None = None,
    source: str = "local",
) -> None:
    """Evaluates an anomaly locally on the edge and executes the trade if valid."""
    _dre_t0 = time.monotonic()
    is_approved = await evaluate_opportunity(tick, cache, baseline)
    rules_engine_latency_seconds.observe(time.monotonic() - _dre_t0)
    if is_approved:
        anomalies_confirmed_total.inc()
        logger.info(
            "[ANOMALY] Confirmed true outlier by Edge DRE (%s)! %s dropped to $%.2f. Executing trade (Z=%.2f)...",
            source,
            tick.market_hash_name,
            tick.price_usd,
            z_score,
        )

        if baseline is None:
            baseline_raw = await cache.get(f"baseline:{tick.market_hash_name}")
            baseline = json.loads(baseline_raw) if baseline_raw else {}
        est_profit_cents = baseline.get("latest_price_cents", tick.price_cents) - tick.price_cents

        await executor.execute(
            market_hash_name=tick.market_hash_name,
            purchase_price_cents=tick.price_cents,
            estimated_profit_cents=est_profit_cents,
            z_score=z_score,
        )
    else:
        anomalies_rejected_total.inc()
        logger.info(
            "[ANOMALY] False outlier filtered by Edge DRE (%s): %s at $%.2f.", source, tick.market_hash_name, tick.price_usd
        )


async def tick_consumer(
    queue: asyncio.Queue[MarketTick | None],
    platform_target: str,
    anomaly_pool: BoundedTaskPool,
    batch_pool: BoundedTaskPool,
    batch_store: RedisBatchStore,
    executor: ExecutionService,
) -> None:
    """Processes ticks from the queue: deduplicates, caches, detects anomalies, and batches for ingest."""
    edge_redis_url = os.getenv("EDGE_REDIS_URL", "redis://localhost:6380")
    redis_password = os.getenv("REDIS_PASSWORD")
    cache = Redis.from_url(edge_redis_url, username="default", password=redis_password, decode_responses=True)

    batch_buffer: list[dict] = []
    batch_buffer_id: str | None = None
    dedup_cache: DedupCache = OrderedDict()

    logger.info("Telemetry processing consumer loop is active (Redis: %s).", edge_redis_url)

    try:
        while True:
            tick = await queue.get()
            tick_queue_size.set(queue.qsize())
            if tick is None:
                queue.task_done()
                break
            try:
                try:
                    # 1. Deduplication Filter
                    if is_duplicate(tick, dedup_cache):
                        ticks_deduplicated_total.inc()
                        continue
                    update_dedup_cache(tick, dedup_cache)
                    dedup_cache_size.set(len(dedup_cache))
                    ticks_processed_total.inc()

                    # Accumulate records for long-term database tracking
                    batch_buffer.append(
                        {
                            "market_hash_name": tick.market_hash_name,
                            "price_cents": tick.price_cents,
                            "timestamp": tick.timestamp,
                        }
                    )
                    batch_buffer_size.set(len(batch_buffer))

                    # 2. Update Volatile Sliding Cache Layer
                    redis_key = f"market:ticks:{tick.market_hash_name}"
                    value_string = f"{tick.timestamp}:{tick.price_cents}"
                    _t0 = time.monotonic()
                    await cache.zadd(redis_key, {value_string: tick.timestamp})
                    redis_operation_latency_seconds.observe(time.monotonic() - _t0)

                    # Keep only the last N ticks
                    _t1 = time.monotonic()
                    card = await cache.zcard(redis_key)
                    redis_operation_latency_seconds.observe(time.monotonic() - _t1)
                    if card > SLIDING_WINDOW_SIZE:
                        _t2 = time.monotonic()
                        await cache.zremrangebyrank(redis_key, 0, card - SLIDING_WINDOW_SIZE - 1)
                        redis_operation_latency_seconds.observe(time.monotonic() - _t2)

                    # 3. Z-Score anomaly detection with macro baseline fallback
                    _t3 = time.monotonic()
                    raw_elements = await cache.zrange(redis_key, 0, -1)
                    redis_operation_latency_seconds.observe(time.monotonic() - _t3)
                    prices = [
                        int(_decode_zset_element(element).split(":")[1])
                        for element in raw_elements
                        if isinstance(element, (str, bytes))
                    ]

                    # Fetch macro baseline for volatility-aware Z-score (Layers 1-2)
                    _t4 = time.monotonic()
                    baseline_raw = await cache.get(f"baseline:{tick.market_hash_name}")
                    redis_operation_latency_seconds.observe(time.monotonic() - _t4)
                    baseline_data: dict | None = json.loads(baseline_raw) if baseline_raw else None

                    macro_avg = baseline_data.get("rolling_30d_avg_cents") if baseline_data else None
                    macro_vol = baseline_data.get("volatility_cents") if baseline_data else None
                    macro_cv = baseline_data.get("coefficient_of_variation") if baseline_data else None

                    result = calculate_z_score(prices, macro_avg, macro_vol, macro_cv)
                    if result is not None:
                        z_score, mean_cents, source = result

                        if should_trigger_anomaly(z_score, mean_cents, tick, source):
                            sticker_count = len(tick.stickers)
                            sticker_tag = f" ({sticker_count} stickers)" if sticker_count > 0 else ""
                            logger.info(
                                "[ANOMALY] Outlier potential detected (%s): %s%s at $%.2f (Z=%.2f). Running Edge DRE...",
                                source,
                                tick.market_hash_name,
                                sticker_tag,
                                tick.price_usd,
                                z_score,
                            )
                            anomalies_detected_total.labels(source=source).inc()
                            await anomaly_pool.submit(
                                partial(
                                    evaluate_and_execute,
                                    tick,
                                    z_score,
                                    cache,
                                    executor,
                                    baseline_data,
                                    source,
                                )
                            )
                # Broad on purpose: one bad tick must not kill the consumer loop.
                except Exception as item_err:
                    logger.error("Error processing tick for '%s': %s", tick.market_hash_name, item_err)

                # 4. Persist full buffers outside the per-item exception boundary.
                if len(batch_buffer) >= CHUNK_LIMIT:
                    batch_buffer_id = batch_buffer_id or str(uuid4())
                    await flush_batch_buffer(
                        platform_target,
                        batch_buffer,
                        batch_id=batch_buffer_id,
                        store=batch_store,
                        batch_pool=batch_pool,
                    )
                    batch_buffer_id = None
            finally:
                queue.task_done()
    finally:
        try:
            if batch_buffer:
                batch_buffer_id = batch_buffer_id or str(uuid4())
                await flush_batch_buffer(
                    platform_target,
                    batch_buffer,
                    batch_id=batch_buffer_id,
                    store=batch_store,
                    batch_pool=batch_pool,
                )
        finally:
            await cache.aclose()


async def start_sidecar_process(scraper) -> None:
    """Spawns the Node.js WebSocket scraper sidecar as an async subprocess and handles its lifetime."""
    sidecar_path = scraper.sidecar_script_path
    if not sidecar_path:
        return
    if not sidecar_path.exists():
        raise FileNotFoundError(f"Node.js sidecar script does not exist: {sidecar_path}")

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

        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(log_stream(proc.stdout, ""))
            task_group.create_task(log_stream(proc.stderr, "[SKINPORT WS ERR] "))
            await proc.wait()
        logger.info("Node.js sidecar process exited with code %s", proc.returncode)
        raise RuntimeError(f"Node.js sidecar exited unexpectedly with code {proc.returncode}")
    except (OSError, subprocess.SubprocessError) as e:
        logger.error("Error running Node.js sidecar: %s", e)
        raise RuntimeError("Node.js sidecar failed") from e
    finally:
        if proc and proc.returncode is None:
            logger.info("Terminating Node.js sidecar process...")
            try:
                proc.terminate()
                await proc.wait()
            except OSError as e:
                logger.warning("Error terminating sidecar process: %s", e)


async def process_live_telemetry_stream(platform_target: str) -> None:
    get_backend_api_key()

    # Start Prometheus metrics HTTP server on a background thread
    _metrics_port = int(os.getenv("LISTENER_METRICS_PORT", "9100"))
    start_http_server(_metrics_port)
    logger.info("[METRICS] Prometheus metrics endpoint listening on :%d/metrics", _metrics_port)

    logger.info("======================================================================")
    logger.info("Initializing Extensible Stream Engine: %s", platform_target.upper())
    logger.info("Target Routing Node Core             : %s:%s", COMPUTE_NODE_IP, COMPUTE_PORT)
    logger.info("======================================================================")

    queue: asyncio.Queue[MarketTick | None] = asyncio.Queue(maxsize=TICK_QUEUE_SIZE)
    scraper = ScraperFactory.get_scraper(platform_target)
    executor = PaperExecutor(f"http://{COMPUTE_NODE_IP}:{COMPUTE_PORT}")
    edge_redis_url = os.getenv("EDGE_REDIS_URL", "redis://localhost:6380")
    redis_password = os.getenv("REDIS_PASSWORD")

    # Register graceful shutdown on SIGINT/SIGTERM
    shutdown_event = asyncio.Event()
    health_runner: web.AppRunner | None = None

    def _signal_handler():
        logger.info("Signal received. Cleaning up...")
        shutdown_event.set()

    if os.name != "nt":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _signal_handler)

    try:
        async with (
            RedisBatchStore.from_url(edge_redis_url, password=redis_password) as batch_store,
            BoundedTaskPool(
                "anomaly",
                workers=ANOMALY_WORKERS,
                queue_size=ANOMALY_QUEUE_SIZE,
                shutdown_timeout=SHUTDOWN_GRACE_SECONDS,
            ) as anomaly_pool,
            BoundedTaskPool(
                "batch_flush",
                workers=BATCH_WORKERS,
                queue_size=BATCH_QUEUE_SIZE,
                shutdown_timeout=SHUTDOWN_GRACE_SECONDS,
            ) as batch_pool,
        ):
            await recover_pending_batches(batch_store, batch_pool)
            async with asyncio.TaskGroup() as task_group:
                consumer_task = task_group.create_task(
                    tick_consumer(queue, platform_target, anomaly_pool, batch_pool, batch_store, executor),
                    name="tick-consumer",
                )
                producer_tasks = [
                    task_group.create_task(rest_poll_producer(scraper, queue), name="rest-poll-producer"),
                    task_group.create_task(websocket_subscriber_producer(scraper, queue), name="websocket-producer"),
                ]
                sidecar_task = None
                if scraper.sidecar_script_path:
                    sidecar_task = task_group.create_task(start_sidecar_process(scraper), name="websocket-sidecar")

                health_runner = await start_listener_health_server(LISTENER_HEALTH_PORT)
                await shutdown_event.wait()

                for task in producer_tasks:
                    task.cancel()
                await asyncio.gather(*producer_tasks, return_exceptions=True)

                try:
                    async with asyncio.timeout(SHUTDOWN_GRACE_SECONDS):
                        await queue.join()
                except TimeoutError:
                    logger.warning(
                        "[BACKGROUND] Timed out draining the tick queue after %d seconds",
                        SHUTDOWN_GRACE_SECONDS,
                    )
                    consumer_task.cancel()
                else:
                    await queue.put(None)
                    await consumer_task

                if sidecar_task is not None:
                    sidecar_task.cancel()
    finally:
        if health_runner is not None:
            await health_runner.cleanup()
        if os.name != "nt":
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)
        tick_queue_size.set(0)
        await close_http_session()
        await close_executor_http_session()
        await scraper.close()
        logger.info("Cleanup complete.")


if __name__ == "__main__":
    platform_target = os.getenv("LISTENER_PLATFORM", "skinport")
    asyncio.run(process_live_telemetry_stream(platform_target))
