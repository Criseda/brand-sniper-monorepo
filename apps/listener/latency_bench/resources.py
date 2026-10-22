"""
What a run costs besides latency: CPU and memory of every process on the path (sampled from a thread, so the
sampling never runs on the event loop under test), the event loop's lag, and the Redis server's CPU time.
"""

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import psutil
from redis.asyncio import Redis

SAMPLE_INTERVAL_SECONDS = 0.1
LOOP_LAG_INTERVAL_SECONDS = 0.01


@dataclass(slots=True)
class ProcessUsage:
    cpu_seconds: float = 0.0
    peak_rss_bytes: int = 0
    samples: int = 0


class ResourceSampler:
    """Samples the CPU time and resident memory of named processes on a background thread."""

    def __init__(self) -> None:
        self._processes: dict[str, psutil.Process] = {}
        self._start_cpu: dict[str, float] = {}
        self.usage: dict[str, ProcessUsage] = {}
        # (perf_counter seconds, CPU use of the whole machine in percent) per sample. Inside a container this is
        # the Docker VM, so it shows the other containers' load too; it is how a noisy run shows up as noisy.
        self.system_samples: list[tuple[float, float]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="resource-sampler", daemon=True)
        self._lock = threading.Lock()

    def watch(self, name: str, pid: int) -> None:
        process = psutil.Process(pid)
        cpu = process.cpu_times()
        with self._lock:
            self._processes[name] = process
            self._start_cpu[name] = cpu.user + cpu.system
            self.usage[name] = ProcessUsage()

    def start(self) -> None:
        psutil.cpu_percent(interval=None)  # The first call only sets the starting point.
        self._thread.start()

    def system_cpu(self, start: float, end: float) -> dict[str, float | int]:
        """Mean and highest machine wide CPU use between two perf_counter readings."""
        values = [percent for at, percent in self.system_samples if start <= at <= end]
        if not values:
            return {"samples": 0}
        return {"samples": len(values), "mean_percent": sum(values) / len(values), "max_percent": max(values)}

    def _sample(self) -> None:
        self.system_samples.append((time.perf_counter(), psutil.cpu_percent(interval=None)))
        with self._lock:
            processes = list(self._processes.items())
        for name, process in processes:
            try:
                cpu = process.cpu_times()
                rss = process.memory_info().rss
            except psutil.Error:
                continue
            usage = self.usage[name]
            usage.cpu_seconds = cpu.user + cpu.system - self._start_cpu[name]
            usage.peak_rss_bytes = max(usage.peak_rss_bytes, rss)
            usage.samples += 1

    def _run(self) -> None:
        while not self._stop.wait(SAMPLE_INTERVAL_SECONDS):
            self._sample()

    def stop(self) -> dict[str, dict[str, Any]]:
        self._stop.set()
        self._thread.join()
        self._sample()
        return {
            name: {"cpu_seconds": usage.cpu_seconds, "peak_rss_bytes": usage.peak_rss_bytes, "samples": usage.samples}
            for name, usage in self.usage.items()
        }


@dataclass
class LoopLagMonitor:
    """How late the event loop wakes a task that sleeps LOOP_LAG_INTERVAL_SECONDS: time it spent busy elsewhere."""

    lags_ns: list[int] = field(default_factory=list)
    recording: bool = False

    async def run(self) -> None:
        interval_ns = round(LOOP_LAG_INTERVAL_SECONDS * 1e9)
        while True:
            started = time.perf_counter_ns()
            await asyncio.sleep(LOOP_LAG_INTERVAL_SECONDS)
            if self.recording:
                self.lags_ns.append(max(0, time.perf_counter_ns() - started - interval_ns))


async def redis_cpu_seconds(cache: Redis) -> float:
    info = await cache.info("cpu")
    return float(info["used_cpu_user"]) + float(info["used_cpu_sys"])


async def redis_memory(cache: Redis) -> dict[str, int]:
    info = await cache.info("memory")
    return {"used_memory_peak_bytes": int(info["used_memory_peak"])}
