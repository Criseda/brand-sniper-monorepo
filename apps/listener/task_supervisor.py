import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from listener_telemetry import background_jobs_active, background_jobs_queued, background_jobs_total
from shared_utils import get_logger

logger = get_logger("listener.task_supervisor")

JobFactory = Callable[[], Awaitable[None]]


class PoolState(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    DRAINING = "draining"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class WorkItem:
    factory: JobFactory


class BoundedTaskPool:
    """Owns a bounded queue and a fixed set of structured-concurrency workers."""

    def __init__(self, name: str, *, workers: int, queue_size: int, shutdown_timeout: float):
        if workers < 1:
            raise ValueError("workers must be at least 1")
        if queue_size < 1:
            raise ValueError("queue_size must be at least 1")
        if shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be greater than 0")

        self.name = name
        self.worker_count = workers
        self.shutdown_timeout = shutdown_timeout
        self._queue: asyncio.Queue[WorkItem | None] = asyncio.Queue(maxsize=queue_size)
        self._task_group: asyncio.TaskGroup | None = None
        self._workers: list[asyncio.Task[None]] = []
        self._state = PoolState.CREATED

    @property
    def state(self) -> PoolState:
        return self._state

    @property
    def pending(self) -> int:
        return self._queue.qsize()

    async def __aenter__(self) -> "BoundedTaskPool":
        if self._state is not PoolState.CREATED:
            raise RuntimeError(f"Task pool '{self.name}' cannot be started from state {self._state}")

        self._task_group = asyncio.TaskGroup()
        await self._task_group.__aenter__()
        self._state = PoolState.RUNNING
        self._workers = [
            self._task_group.create_task(self._worker(), name=f"{self.name}-worker-{index}")
            for index in range(self.worker_count)
        ]
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.close()

    async def submit(self, factory: JobFactory) -> None:
        """Queues work, applying asynchronous backpressure when the queue is full."""
        if self._state is not PoolState.RUNNING:
            background_jobs_total.labels(job_type=self.name, outcome="rejected").inc()
            raise RuntimeError(f"Task pool '{self.name}' is not accepting work")

        await self._queue.put(WorkItem(factory=factory))
        background_jobs_queued.labels(job_type=self.name).set(self._queue.qsize())
        background_jobs_total.labels(job_type=self.name, outcome="submitted").inc()

    async def close(self) -> None:
        """Stops submissions, drains queued work, then shuts down all workers."""
        if self._state is PoolState.CLOSED:
            return
        if self._state is PoolState.CREATED:
            self._state = PoolState.CLOSED
            return

        self._state = PoolState.DRAINING
        timed_out = False
        try:
            async with asyncio.timeout(self.shutdown_timeout):
                await self._queue.join()
        except TimeoutError:
            timed_out = True
            logger.warning(
                "[BACKGROUND] Timed out draining '%s' after %.1f seconds; cancelling remaining work",
                self.name,
                self.shutdown_timeout,
            )

        if timed_out:
            self._cancel_queued_work()
            for worker in self._workers:
                worker.cancel()
        else:
            for _ in self._workers:
                self._queue.put_nowait(None)

        task_group = cast(asyncio.TaskGroup, self._task_group)
        await task_group.__aexit__(None, None, None)

        background_jobs_queued.labels(job_type=self.name).set(0)
        background_jobs_active.labels(job_type=self.name).set(0)
        self._state = PoolState.CLOSED

    def _cancel_queued_work(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            background_jobs_total.labels(job_type=self.name, outcome="cancelled").inc()
            self._queue.task_done()

    async def _worker(self) -> None:
        while True:
            item = await self._queue.get()
            background_jobs_queued.labels(job_type=self.name).set(self._queue.qsize())
            if item is None:
                self._queue.task_done()
                return

            background_jobs_active.labels(job_type=self.name).inc()
            try:
                await item.factory()
            except asyncio.CancelledError:
                background_jobs_total.labels(job_type=self.name, outcome="cancelled").inc()
                raise
            except Exception as exc:
                background_jobs_total.labels(job_type=self.name, outcome="failed").inc()
                logger.error(
                    "[BACKGROUND] Job failed in '%s': %s",
                    self.name,
                    exc,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
            else:
                background_jobs_total.labels(job_type=self.name, outcome="succeeded").inc()
            finally:
                background_jobs_active.labels(job_type=self.name).dec()
                self._queue.task_done()
