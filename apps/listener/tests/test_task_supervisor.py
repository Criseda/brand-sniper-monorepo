import asyncio

import pytest
from listener_telemetry import background_jobs_total
from task_supervisor import BoundedTaskPool, PoolState


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"workers": 0}, "workers must be at least 1"),
        ({"queue_size": 0}, "queue_size must be at least 1"),
        ({"shutdown_timeout": 0}, "shutdown_timeout must be greater than 0"),
    ],
)
def test_pool_rejects_invalid_configuration(overrides, message):
    options = {"workers": 1, "queue_size": 1, "shutdown_timeout": 1}
    options.update(overrides)

    with pytest.raises(ValueError, match=message):
        BoundedTaskPool("test_invalid", **options)


@pytest.mark.asyncio
async def test_pool_caps_concurrent_jobs():
    active = 0
    maximum_active = 0
    both_workers_started = asyncio.Event()
    release = asyncio.Event()

    async def job() -> None:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        if active == 2:
            both_workers_started.set()
        await release.wait()
        active -= 1

    async with BoundedTaskPool("test_concurrency", workers=2, queue_size=4, shutdown_timeout=1) as pool:
        for _ in range(4):
            await pool.submit(job)
        await asyncio.wait_for(both_workers_started.wait(), timeout=1)
        assert maximum_active == 2
        release.set()


@pytest.mark.asyncio
async def test_pool_applies_backpressure_when_queue_is_full():
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocking_job() -> None:
        started.set()
        await release.wait()

    async def noop_job() -> None:
        return None

    async with BoundedTaskPool("test_backpressure", workers=1, queue_size=1, shutdown_timeout=1) as pool:
        await pool.submit(blocking_job)
        await asyncio.wait_for(started.wait(), timeout=1)
        await pool.submit(noop_job)
        blocked_submission = asyncio.create_task(pool.submit(noop_job))
        await asyncio.sleep(0)
        assert not blocked_submission.done()

        release.set()
        await asyncio.wait_for(blocked_submission, timeout=1)


@pytest.mark.asyncio
async def test_failed_job_is_counted_and_does_not_stop_worker():
    completed = asyncio.Event()
    failed_counter = background_jobs_total.labels(job_type="test_failures", outcome="failed")
    before = failed_counter._value.get()

    async def failing_job() -> None:
        raise RuntimeError("boom")

    async def successful_job() -> None:
        completed.set()

    async with BoundedTaskPool("test_failures", workers=1, queue_size=2, shutdown_timeout=1) as pool:
        await pool.submit(failing_job)
        await pool.submit(successful_job)
        await asyncio.wait_for(completed.wait(), timeout=1)

    assert failed_counter._value.get() == before + 1


@pytest.mark.asyncio
async def test_pool_drains_on_close_and_rejects_late_submissions():
    completed = asyncio.Event()

    async def job() -> None:
        completed.set()

    pool = BoundedTaskPool("test_shutdown", workers=1, queue_size=1, shutdown_timeout=1)
    async with pool:
        await pool.submit(job)

    assert completed.is_set()
    assert pool.state is PoolState.CLOSED
    with pytest.raises(RuntimeError, match="not accepting work"):
        await pool.submit(job)

    await pool.close()


@pytest.mark.asyncio
async def test_pool_reports_pending_work_and_rejects_restart():
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocking_job() -> None:
        started.set()
        await release.wait()

    async def noop_job() -> None:
        return None

    pool = BoundedTaskPool("test_state", workers=1, queue_size=1, shutdown_timeout=1)
    async with pool:
        await pool.submit(blocking_job)
        await asyncio.wait_for(started.wait(), timeout=1)
        await pool.submit(noop_job)

        assert pool.pending == 1
        with pytest.raises(RuntimeError, match="cannot be started"):
            await pool.__aenter__()

        release.set()


@pytest.mark.asyncio
async def test_pool_can_close_before_it_is_started():
    pool = BoundedTaskPool("test_never_started", workers=1, queue_size=1, shutdown_timeout=1)

    await pool.close()

    assert pool.state is PoolState.CLOSED


@pytest.mark.asyncio
async def test_pool_cancels_running_and_queued_work_after_shutdown_timeout():
    started = asyncio.Event()
    never_release = asyncio.Event()
    cancelled_counter = background_jobs_total.labels(job_type="test_timeout", outcome="cancelled")
    before = cancelled_counter._value.get()

    async def blocking_job() -> None:
        started.set()
        await never_release.wait()

    pool = BoundedTaskPool("test_timeout", workers=1, queue_size=1, shutdown_timeout=0.01)
    await pool.__aenter__()
    await pool.submit(blocking_job)
    await asyncio.wait_for(started.wait(), timeout=1)
    await pool.submit(blocking_job)

    await pool.close()

    assert pool.state is PoolState.CLOSED
    assert cancelled_counter._value.get() == before + 2
