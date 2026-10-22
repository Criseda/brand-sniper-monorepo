"""
One benchmark run: one scenario played once through the current ingress path, in fresh processes.

    feed server process --websocket--> Node.js sidecar --Redis Pub/Sub--> this process:
    SkinportScraper.listen_websocket_stream -> tick queue -> main.tick_consumer -> price window, Z score -> DRE

Everything after the feed server is the listener's own code (main.websocket_subscriber_producer,
main.tick_consumer and main.start_sidecar_process), with two stand-ins: batches are acknowledged instead of
being sent to the backend, and the executor notes the order instead of posting it.
"""

import asyncio
import cProfile
import gzip
import io
import json
import os
import platform
import pstats
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psutil
from backtest.sources import BaselineSnapshot
from backtest.strategy import ZScoreDreStrategy
from batch_delivery import RedisBatchStore
from latency_bench import probes
from latency_bench.fixture import LISTED, Fixture
from latency_bench.listener import listener_main
from latency_bench.resources import LoopLagMonitor, ResourceSampler, redis_cpu_seconds, redis_memory
from latency_bench.scenarios import Scenario, ScenarioBuilder, ScheduledEvent, listed_count
from latency_bench.stats import (
    SidecarTiming,
    clock_order_violations,
    distribution_ms,
    listing_stages,
    stage_breakdown,
)
from redis.asyncio import Redis
from scrapers.skinport import SALE_FEED_CHANNEL, VENUE, SkinportScraper
from shared_utils import edge_baselines_key, edge_sticker_prices_key, get_logger
from task_supervisor import BoundedTaskPool

logger = get_logger("listener.latency_bench")

RESULT_FORMAT_VERSION = 1
LISTENER_DIR = Path(__file__).resolve().parents[1]
CLOCK_PROBES = 50
# How long the client gets to connect and subscribe, and the drain allowed after the last scheduled event.
CONNECT_TIMEOUT_SECONDS = 60.0
DRAIN_TIMEOUT_SECONDS = 120.0
# How long the sidecar gets to write its timings; with --cpu-prof on a loaded machine it can take a while.
FLUSH_TIMEOUT_SECONDS = 120.0
# Quiet time between the warm-up's last decision and the first measured event.
WARMUP_SETTLE_SECONDS = 1.0
# Where the environment of a run is read from on Linux.
CPUINFO = Path("/proc/cpuinfo")
CGROUP_CPU_MAX = Path("/sys/fs/cgroup/cpu.max")


@dataclass(frozen=True, slots=True)
class RunConfig:
    scenario: Scenario
    fixture_dir: Path
    redis_url: str
    out_dir: Path
    seed: int
    run_index: int
    probe_level: str = "full"
    measured_listings: int | None = None
    profile: bool = False


class FeedServerProcess:
    """The feed server in its own Python process, driven over its stdin and stdout."""

    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self.process = process

    @classmethod
    async def start(cls, schedule_path: Path, emit_out: Path) -> "FeedServerProcess":
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "latency_bench",
            "serve",
            "--schedule",
            str(schedule_path),
            "--emit-out",
            str(emit_out),
            cwd=LISTENER_DIR,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        return cls(process)

    async def expect(self, prefix: str, timeout: float) -> str:
        assert self.process.stdout is not None
        while True:
            line = (await asyncio.wait_for(self.process.stdout.readline(), timeout)).decode("utf-8").strip()
            if not line:
                raise RuntimeError(f"the feed server exited while waiting for {prefix}")
            if line.startswith(prefix):
                return line

    async def send(self, command: str) -> None:
        assert self.process.stdin is not None
        self.process.stdin.write(f"{command}\n".encode())
        await self.process.stdin.drain()

    async def stop(self) -> None:
        if self.process.returncode is None:
            self.process.terminate()
            await self.process.wait()


def write_schedule(path: Path, events: list[ScheduledEvent]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as lines:
        for event in events:
            record = {"offset_ms": event.offset_ms, "measured": event.measured, "payload": event.payload}
            lines.write(json.dumps(record, separators=(",", ":")) + "\n")


async def prepare_redis(cache: Redis, baselines: BaselineSnapshot) -> None:
    """
    Refuses any Redis that holds keys: the benchmark publishes on the live sale feed channel and empties the
    database afterwards, so it must never run against the edge Redis.
    """
    keys = await cache.dbsize()
    if keys:
        names = [name async for name in cache.scan_iter(count=100)][:5]
        raise SystemExit(
            f"Refusing to run: the benchmark Redis holds {keys} keys ({', '.join(names)}). "
            "Use an empty Redis of its own, never the edge Redis."
        )
    await cache.hset(
        edge_baselines_key(VENUE),
        mapping={name: json.dumps(baseline, sort_keys=True) for name, baseline in baselines.baselines.items()},
    )
    if baselines.sticker_prices:
        await cache.hset(
            edge_sticker_prices_key(VENUE),
            mapping={name: str(price) for name, price in baselines.sticker_prices.items()},
        )


async def wait_for_decisions(recorder: probes.StageRecorder, count: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while recorder.decided_count < count:
        if time.monotonic() > deadline:
            raise TimeoutError(f"the listener decided on {recorder.decided_count} of {count} warm-up listings")
        await asyncio.sleep(0.05)


async def wait_for_subscriber(cache: Redis, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        subscribers = dict(await cache.pubsub_numsub(SALE_FEED_CHANNEL))
        if subscribers.get(SALE_FEED_CHANNEL, 0) >= 1:
            return
        await asyncio.sleep(0.05)
    raise TimeoutError("the listener never subscribed to the sale feed channel")


async def clock_offsets(server: FeedServerProcess) -> dict[str, Any]:
    """
    Reads the feed server's perf_counter between two of ours, CLOCK_PROBES times. On one monotonic clock its
    reading always falls inside our bracket; the narrowest bracket bounds how far apart two readings can be.
    """
    outside = 0
    narrowest_ns: int | None = None
    for _ in range(CLOCK_PROBES):
        before = time.perf_counter_ns()
        await server.send("CLOCK")
        line = await server.expect("CLOCK", 10.0)
        after = time.perf_counter_ns()
        reading = int(line.split()[1])
        if not before <= reading <= after:
            outside += 1
        narrowest_ns = after - before if narrowest_ns is None else min(narrowest_ns, after - before)
    return {"probes": CLOCK_PROBES, "outside_bracket": outside, "narrowest_bracket_ms": (narrowest_ns or 0) / 1e6}


def _sidecar_pid() -> int | None:
    """The Node.js sidecar among this process's children (Node names its process MainThread, so match the script)."""
    for child in psutil.Process().children(recursive=True):
        try:
            if any(argument.endswith("sidecar.js") for argument in child.cmdline()):
                return child.pid
        except psutil.Error:
            continue
    return None


def _sidecar_timings(path: Path) -> tuple[dict[str, SidecarTiming], dict[str, Any]]:
    if not path.exists():
        return {}, {}
    report = json.loads(path.read_text(encoding="utf-8"))
    timings = {
        key: SidecarTiming(callback=int(callback), publish=int(publish), published=int(published))
        for key, callback, publish, published in report.get("timings", [])
    }
    usage = {key: value for key, value in report.items() if key != "timings"}
    return timings, usage


def _cpu_model(cpuinfo: Path = CPUINFO) -> str:
    """The CPU model; platform.processor() is empty inside a Linux container."""
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    return platform.processor()


def _cpu_limit(cpu_max: Path = CGROUP_CPU_MAX) -> float | None:
    """The container's CPU limit from cgroup v2, None without one."""
    if not cpu_max.exists():
        return None
    quota, period = cpu_max.read_text(encoding="utf-8").split()
    return None if quota == "max" else int(quota) / int(period)


def _node_version() -> str:
    try:
        return subprocess.run(["node", "--version"], capture_output=True, text=True, check=False).stdout.strip()
    except OSError:
        return ""


def environment() -> dict[str, Any]:
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": _cpu_model(),
        "cpu_limit": _cpu_limit(),
        "cpu_count": os.cpu_count(),
        "memory_bytes": psutil.virtual_memory().total,
        "python": sys.version.split()[0],
        "node": _node_version(),
        "in_container": Path("/.dockerenv").exists(),
    }


async def run_once(config: RunConfig) -> dict[str, Any]:
    run_dir = config.out_dir / f"{config.scenario.name}-{config.probe_level}-run{config.run_index}"
    run_dir.mkdir(parents=True, exist_ok=True)
    fixture = Fixture.load(config.fixture_dir)
    events = ScenarioBuilder(fixture, config.seed).schedule(config.scenario, config.measured_listings)
    schedule_path = run_dir / "schedule.jsonl"
    write_schedule(schedule_path, events)
    emit_path = run_dir / "emitted.json"
    sidecar_path = run_dir / "sidecar_timings.json"

    main = listener_main()
    # main loaded the .env files on import, and apps/listener/.env overrides; the benchmark's settings win.
    os.environ.update(
        {
            "EDGE_REDIS_URL": config.redis_url,
            "REDIS_PASSWORD": "",
            "CLOUDFLARE_COOKIE": "",
            "DEBUG_TELEMETRY": "false",
            "SIDECAR_TIMING_FILE": str(sidecar_path),
        }
    )
    if config.profile:
        os.environ["NODE_OPTIONS"] = f"--cpu-prof --cpu-prof-dir={run_dir / 'node_profile'}"

    cache = Redis.from_url(config.redis_url, decode_responses=True)
    await prepare_redis(cache, fixture.baselines)

    recorder = probes.StageRecorder(level=config.probe_level, expected=listed_count(events))
    sampler = ResourceSampler()
    lag = LoopLagMonitor()
    server = await FeedServerProcess.start(schedule_path, emit_path)
    tasks: list[asyncio.Task[Any]] = []
    profiler = cProfile.Profile() if config.profile else None
    try:
        port = int((await server.expect("READY", CONNECT_TIMEOUT_SECONDS)).split()[1])
        os.environ["SKINPORT_WS_URL"] = f"http://127.0.0.1:{port}"
        sampler.watch("listener", os.getpid())
        sampler.watch("feed_server", server.process.pid)

        with probes.install(recorder):
            queue: asyncio.Queue[Any] = probes.InstrumentedQueue(recorder, maxsize=main.TICK_QUEUE_SIZE)
            scraper = SkinportScraper()
            executor = probes.BenchExecutor(recorder)
            async with (
                RedisBatchStore.for_venue(config.redis_url, password=None, venue=VENUE) as batch_store,
                BoundedTaskPool(
                    "anomaly",
                    workers=main.ANOMALY_WORKERS,
                    queue_size=main.ANOMALY_QUEUE_SIZE,
                    shutdown_timeout=main.SHUTDOWN_GRACE_SECONDS,
                ) as anomaly_pool,
                BoundedTaskPool(
                    "batch_flush",
                    workers=main.BATCH_WORKERS,
                    queue_size=main.BATCH_QUEUE_SIZE,
                    shutdown_timeout=main.SHUTDOWN_GRACE_SECONDS,
                ) as batch_pool,
            ):
                consumer = asyncio.create_task(
                    main.tick_consumer(queue, VENUE, anomaly_pool, batch_pool, batch_store, executor)
                )
                producer = asyncio.create_task(main.websocket_subscriber_producer(scraper, queue))
                sidecar = asyncio.create_task(main.start_sidecar_process(scraper))
                tasks = [producer, sidecar, asyncio.create_task(lag.run())]
                try:
                    await server.expect("JOINED", CONNECT_TIMEOUT_SECONDS)
                    await wait_for_subscriber(cache, CONNECT_TIMEOUT_SECONDS)
                    sidecar_pid = _sidecar_pid()
                    if sidecar_pid is not None:
                        sampler.watch("sidecar", sidecar_pid)
                    sampler.start()
                    clock = await clock_offsets(server)

                    # Warm-up: fill every item's price window, and wait until the listener has decided on all of it.
                    warmup_listings = listed_count(events, measured=False)
                    await server.send("WARMUP")
                    await server.expect("WARMED", DRAIN_TIMEOUT_SECONDS)
                    await wait_for_decisions(recorder, warmup_listings, DRAIN_TIMEOUT_SECONDS)
                    await asyncio.sleep(WARMUP_SETTLE_SECONDS)

                    # The measured phase: from here on lag, CPU and Redis count.
                    cpu_before = {name: usage.cpu_seconds for name, usage in sampler.usage.items()}
                    redis_cpu_before = await redis_cpu_seconds(cache)
                    lag.recording = True
                    if profiler is not None:
                        profiler.enable()
                    measured_started = time.perf_counter()
                    await server.send("GO")

                    timed_out = False
                    schedule_seconds = max(event.offset_ms for event in events if event.measured) / 1000
                    try:
                        await asyncio.wait_for(recorder.all_decided.wait(), timeout=schedule_seconds + DRAIN_TIMEOUT_SECONDS)
                    except TimeoutError:
                        timed_out = True
                    measured_ended = time.perf_counter()
                    measured_seconds = measured_ended - measured_started
                    if profiler is not None:
                        profiler.disable()
                    lag.recording = False
                    cpu_after = {name: usage.cpu_seconds for name, usage in sampler.usage.items()}
                    redis_cpu_after = await redis_cpu_seconds(cache)

                    await server.expect("PLAYED", DRAIN_TIMEOUT_SECONDS)
                    await server.send("FLUSH")
                    await server.expect("FLUSHED", FLUSH_TIMEOUT_SECONDS)
                    await server.send("STOP")
                    await server.expect("DONE", 30.0)

                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    await queue.join()
                    await queue.put(None)
                    await consumer
                finally:
                    # On a failure too: stop every task, the consumer included, while the pools and the batch store
                    # are still open, so nothing writes to Redis after it has been emptied.
                    for task in (*tasks, consumer):
                        task.cancel()
                    await asyncio.gather(*tasks, consumer, return_exceptions=True)
                resources = sampler.stop()
                await scraper.close()
        redis_memory_info = await redis_memory(cache)
    finally:
        for task in tasks:
            task.cancel()
        await server.stop()
        await cache.flushdb()
        await cache.aclose()

    emitted = {key: emitted_ns for key, emitted_ns in json.loads(emit_path.read_text(encoding="utf-8"))["emitted"]}
    sidecar_timings, sidecar_usage = _sidecar_timings(sidecar_path)
    summary = summarize(
        config=config,
        events=events,
        recorder=recorder,
        emitted=emitted,
        sidecar_timings=sidecar_timings,
        run_dir=run_dir,
    )
    summary.update(
        {
            "timed_out": timed_out,
            "measured_seconds": measured_seconds,
            "clock": clock,
            "loop_lag": distribution_ms(lag.lags_ns),
            "cpu_seconds_measured": {name: cpu_after.get(name, 0.0) - cpu_before.get(name, 0.0) for name in cpu_after},
            "redis_cpu_seconds_measured": redis_cpu_after - redis_cpu_before,
            "system_cpu_percent_measured": sampler.system_cpu(measured_started, measured_ended),
            "resources": resources,
            "sidecar_usage": sidecar_usage,
            "redis_memory": redis_memory_info,
            "orders_placed": executor.orders,
        }
    )
    if profiler is not None:
        summary["python_profile_top"] = profile_top(profiler, run_dir / "python_profile.txt")
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def profile_top(profiler: cProfile.Profile, path: Path, rows: int = 40) -> list[dict[str, Any]]:
    """The functions that held the CPU longest themselves during the measured phase."""
    text = io.StringIO()
    stats = pstats.Stats(profiler, stream=text).sort_stats("tottime")
    stats.print_stats(rows)
    path.write_text(text.getvalue(), encoding="utf-8")
    top = []
    entries: dict[Any, Any] = stats.stats  # type: ignore[attr-defined]
    for (filename, line, function), (_, calls, tottime, cumtime, _) in sorted(
        entries.items(), key=lambda entry: entry[1][2], reverse=True
    )[:rows]:
        top.append(
            {"function": f"{Path(filename).name}:{line}:{function}", "calls": calls, "tottime_s": tottime, "cumtime_s": cumtime}
        )
    return top


def decision_rate(decided: int, first_decided: int | None, last_decided: int | None) -> float | None:
    if decided < 2 or first_decided is None or last_decided is None or last_decided <= first_decided:
        return None
    return (decided - 1) / ((last_decided - first_decided) / 1e9)


def summarize(
    *,
    config: RunConfig,
    events: list[ScheduledEvent],
    recorder: probes.StageRecorder,
    emitted: dict[str, int],
    sidecar_timings: dict[str, SidecarTiming],
    run_dir: Path,
) -> dict[str, Any]:
    """Per listing stages (written to stages.jsonl.gz) and their distributions over the measured listings."""
    rows: list[dict[str, Any]] = []
    violations = 0
    missing = 0
    duplicates = 0
    approvals = 0
    first_emitted: int | None = None
    first_decided: int | None = None
    last_decided: int | None = None
    measured_events = [event for event in events if event.measured]
    for event in measured_events:
        if event.event_type != LISTED:
            continue
        emitted_ns = emitted.get(event.key)
        sidecar = sidecar_timings.get(event.key)
        for product_id in event.product_ids:
            times = recorder.listings.get(product_id)
            if times is None or times.decided is None:
                missing += 1
                continue
            duplicates += max(0, times.decisions - 1)
            approvals += int(times.executed is not None)
            violations += clock_order_violations(times, emitted_ns, sidecar)
            if emitted_ns is not None:
                first_emitted = emitted_ns if first_emitted is None else min(first_emitted, emitted_ns)
            first_decided = times.decided if first_decided is None else min(first_decided, times.decided)
            last_decided = times.decided if last_decided is None else max(last_decided, times.decided)
            stages = listing_stages(times, emitted_ns, sidecar)
            rows.append({"listing": product_id, "triggered": times.triggered, "dre_reason": times.dre_reason, **stages})

    with gzip.open(run_dir / "stages.jsonl.gz", "wt", encoding="utf-8") as raw:
        for row in rows:
            raw.write(json.dumps(row, separators=(",", ":")) + "\n")

    measured = listed_count(events, measured=True)
    span_s = (last_decided - first_emitted) / 1e9 if first_emitted is not None and last_decided is not None else None
    scheduled_span_s = (measured_events[-1].offset_ms - measured_events[0].offset_ms) / 1000 if measured_events else 0.0
    emitted_measured = [emitted[event.key] for event in measured_events if event.key in emitted]
    emitted_span_s = (max(emitted_measured) - min(emitted_measured)) / 1e9 if emitted_measured else None
    stage_rows = [
        {key: value for key, value in row.items() if key not in ("listing", "triggered", "dre_reason")} for row in rows
    ]
    return {
        "format": RESULT_FORMAT_VERSION,
        "pipeline": "current",
        "scenario": config.scenario.name,
        "scenario_description": config.scenario.description,
        "probe_level": config.probe_level,
        "run_index": config.run_index,
        "seed": config.seed,
        "measured_listings": measured,
        "measured_events": len(measured_events),
        "decided": len(rows),
        "dropped": missing,
        "duplicate_decisions": duplicates,
        "triggered": sum(1 for row in rows if row["triggered"]),
        "approved": approvals,
        "clock_order_violations": violations,
        "throughput_listings_per_second": measured / span_s if span_s else None,
        # How fast the listener decided while it was deciding; unlike throughput, not lowered by dropped listings.
        "decision_rate_listings_per_second": decision_rate(len(rows), first_decided, last_decided),
        "scheduled_span_seconds": scheduled_span_s,
        "emitted_span_seconds": emitted_span_s,
        "stages_ms": stage_breakdown(stage_rows),
        "decision_config": ZScoreDreStrategy().config(),
        "environment": environment(),
    }
