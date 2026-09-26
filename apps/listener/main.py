import asyncio
import os
import signal
import subprocess
from collections import OrderedDict
from functools import partial
from uuid import uuid4

import aiohttp
from aiohttp import web
from shared_utils import setup_service_environment

# Load root .env (shared) first, then listener-specific overrides.
setup_service_environment(__file__)

from baseline_loader import BaselineState, baselines_url, keep_baselines_loaded
from batch_delivery import RedisBatchStore, StoredBatch, deliver_stored_batch
from detection import (
    DedupCache,
    is_duplicate,
    is_unchanged_snapshot,
    push_to_window,
    update_dedup_cache,
    update_window_and_detect,
)
from executor import ExecutionService, PaperExecutor
from executor import close_http_session as close_executor_http_session
from listener_telemetry import (
    batch_buffer_size,
    dedup_cache_size,
    feed_events_received_total,
    snapshots_unchanged_total,
    tick_queue_size,
    ticks_deduplicated_total,
    ticks_processed_total,
)
from models import FeedEvent, MarketTick
from prometheus_client import start_http_server
from redis.asyncio import Redis
from scrapers.factory import ScraperFactory
from shared_utils import backend_api_headers, fees_for, get_backend_api_key, get_logger, utc_now_naive
from task_supervisor import BoundedTaskPool

logger = get_logger("listener.main")

# Pulls target node location from RAM environment, falling back to local loopback
COMPUTE_NODE_IP = os.getenv("COMPUTE_NODE_IP", "localhost")
COMPUTE_PORT = os.getenv("COMPUTE_NODE_PORT", "8080")

BACKEND_BASE_URL = f"http://{COMPUTE_NODE_IP}:{COMPUTE_PORT}"
BULK_INGEST_URL = f"{BACKEND_BASE_URL}/api/v1/ingest/bulk"


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
# Everything a producer can hand to the consumer; None is the shutdown sentinel.
type StreamItem = MarketTick | FeedEvent


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


def create_listener_health_app(baseline_state: BaselineState | None = None) -> web.Application:
    """Create the listener's container-internal health application.

    The probe answers from the listener event loop, so any response proves loop liveness. It reports
    503 when the edge has no usable baselines: the listener still records everything, but its DRE
    cannot approve anything correctly.
    """

    async def listener_health(_request: web.Request) -> web.Response:
        problem = baseline_state.problem(utc_now_naive()) if baseline_state is not None else None
        if problem is not None:
            return web.json_response({"status": "degraded", "reason": problem}, status=503)
        return web.json_response({"status": "healthy"})

    app = web.Application()
    app.router.add_get("/health", listener_health)
    return app


async def start_listener_health_server(port: int, baseline_state: BaselineState | None = None) -> web.AppRunner:
    """Start the container-internal listener health endpoint."""
    runner = web.AppRunner(create_listener_health_app(baseline_state))
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    logger.info("[HEALTH] Listener health endpoint listening on 127.0.0.1:%d/health", port)
    return runner


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
    feed_event_buffer: list[dict] | None = None,
) -> None:
    """Persist a stable batch, transfer buffer ownership, then schedule delivery."""
    snapshot = buffer.copy()
    feed_event_snapshot = feed_event_buffer.copy() if feed_event_buffer else []
    batch = await store.add(source, snapshot, batch_id=batch_id, feed_events=feed_event_snapshot)
    buffer.clear()
    if feed_event_buffer is not None:
        feed_event_buffer.clear()
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


async def rest_poll_producer(scraper, queue: asyncio.Queue[StreamItem | None]) -> None:
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


async def websocket_subscriber_producer(scraper, queue: asyncio.Queue[StreamItem | None]) -> None:
    """Listens to real-time events from the platform's WebSocket stream relay and puts them into the queue."""
    if not hasattr(scraper, "listen_websocket_stream"):
        return

    while True:
        try:
            async for item in scraper.listen_websocket_stream():
                await queue.put(item)
                tick_queue_size.set(queue.qsize())
        # Broad on purpose: supervisor loop must survive any transient failure and retry.
        except Exception as e:
            logger.warning("Ingestion watchdog caught subscriber crash: %s. Reconnecting in 10 seconds...", e)
            await asyncio.sleep(10)


async def tick_consumer(
    queue: asyncio.Queue[StreamItem | None],
    platform_target: str,
    anomaly_pool: BoundedTaskPool,
    batch_pool: BoundedTaskPool,
    batch_store: RedisBatchStore,
    executor: ExecutionService,
) -> None:
    """Processes stream items: records every tick and raw feed event, and scores live offers for anomalies."""
    edge_redis_url = os.getenv("EDGE_REDIS_URL", "redis://localhost:6380")
    redis_password = os.getenv("REDIS_PASSWORD")
    cache = Redis.from_url(edge_redis_url, username="default", password=redis_password, decode_responses=True)

    batch_buffer: list[dict] = []
    feed_event_buffer: list[dict] = []
    batch_buffer_id: str | None = None
    dedup_cache: DedupCache = OrderedDict()

    logger.info("Telemetry processing consumer loop is active (Redis: %s).", edge_redis_url)

    try:
        while True:
            item = await queue.get()
            tick_queue_size.set(queue.qsize())
            if item is None:
                queue.task_done()
                break
            try:
                try:
                    if isinstance(item, FeedEvent):
                        # Raw payloads are recorded verbatim for replay and labeling.
                        feed_event_buffer.append(item.to_batch_record())
                        feed_events_received_total.labels(event_type=item.event_type).inc()
                    elif not item.feeds_price_window:
                        # Sold events are market outcomes: recorded, but they never touch the
                        # dedup cache, the price window, or the DRE.
                        batch_buffer.append(item.to_batch_record())
                    elif is_duplicate(item, dedup_cache):
                        ticks_deduplicated_total.inc()
                        if item.listing_id is not None or item.is_rest_snapshot:
                            # A distinct listing at a repeated price, and every REST snapshot, is still
                            # recorded (so replay sees every tick live saw); it just does not re-enter
                            # the price window, so decisions are unchanged.
                            batch_buffer.append(item.to_batch_record())
                    else:
                        unchanged_snapshot = is_unchanged_snapshot(item, dedup_cache)
                        update_dedup_cache(item, dedup_cache)
                        dedup_cache_size.set(len(dedup_cache))
                        ticks_processed_total.inc()

                        # Accumulate records for long-term database tracking
                        batch_buffer.append(item.to_batch_record())
                        if unchanged_snapshot:
                            # The lowest ask has not changed since the last poll: keep the window as it
                            # was, but do not score (or paper trade) the same reading again.
                            snapshots_unchanged_total.inc()
                            await push_to_window(item, cache)
                        else:
                            await update_window_and_detect(item, cache, anomaly_pool, executor)
                    batch_buffer_size.set(len(batch_buffer))
                # Broad on purpose: one bad tick must not kill the consumer loop.
                except Exception as item_err:
                    item_label = f"feed event {item.event_type}" if isinstance(item, FeedEvent) else item.market_hash_name
                    logger.error("Error processing tick for '%s': %s", item_label, item_err)

                # Persist full buffers outside the per-item exception boundary.
                if len(batch_buffer) >= CHUNK_LIMIT or len(feed_event_buffer) >= CHUNK_LIMIT:
                    batch_buffer_id = batch_buffer_id or str(uuid4())
                    await flush_batch_buffer(
                        platform_target,
                        batch_buffer,
                        batch_id=batch_buffer_id,
                        store=batch_store,
                        batch_pool=batch_pool,
                        feed_event_buffer=feed_event_buffer,
                    )
                    batch_buffer_id = None
            finally:
                queue.task_done()
    finally:
        try:
            if batch_buffer or feed_event_buffer:
                batch_buffer_id = batch_buffer_id or str(uuid4())
                await flush_batch_buffer(
                    platform_target,
                    batch_buffer,
                    batch_id=batch_buffer_id,
                    store=batch_store,
                    batch_pool=batch_pool,
                    feed_event_buffer=feed_event_buffer,
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

    queue: asyncio.Queue[StreamItem | None] = asyncio.Queue(maxsize=TICK_QUEUE_SIZE)
    scraper = ScraperFactory.get_scraper(platform_target)
    # Fail at startup, not on the first approved trade, when the venue has no fee schedule.
    fees_for(scraper.platform_name)
    executor = PaperExecutor(BACKEND_BASE_URL)
    edge_redis_url = os.getenv("EDGE_REDIS_URL", "redis://localhost:6380")
    redis_password = os.getenv("REDIS_PASSWORD")
    baseline_state = BaselineState(scraper.platform_name)
    baseline_cache = Redis.from_url(edge_redis_url, username="default", password=redis_password, decode_responses=True)

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
                baseline_task = task_group.create_task(
                    keep_baselines_loaded(
                        baseline_state,
                        get_http_session,
                        baselines_url(BACKEND_BASE_URL, baseline_state.venue),
                        baseline_cache,
                    ),
                    name="baseline-loader",
                )
                sidecar_task = None
                if scraper.sidecar_script_path:
                    sidecar_task = task_group.create_task(start_sidecar_process(scraper), name="websocket-sidecar")

                health_runner = await start_listener_health_server(LISTENER_HEALTH_PORT, baseline_state)
                await shutdown_event.wait()

                for task in [*producer_tasks, baseline_task]:
                    task.cancel()
                await asyncio.gather(*producer_tasks, baseline_task, return_exceptions=True)

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
        await baseline_cache.aclose()
        logger.info("Cleanup complete.")


if __name__ == "__main__":
    platform_target = os.getenv("LISTENER_PLATFORM", "skinport")
    asyncio.run(process_live_telemetry_stream(platform_target))
