import asyncio
import importlib.util
import json
from pathlib import Path
from unittest.mock import AsyncMock

import aiohttp
import batch_delivery
import pytest
from batch_delivery import (
    BatchDeliveryError,
    RedisBatchStore,
    StoredBatch,
    deliver_stored_batch,
    send_batch_with_retry,
)


class FakeRedis:
    def __init__(self):
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.next_id = 1
        self.pinged = False
        self.closed = False
        self.xlen_error: Exception | None = None

    async def ping(self):
        self.pinged = True

    async def aclose(self):
        self.closed = True

    async def xadd(self, key, fields):
        record_id = f"{self.next_id}-0"
        self.next_id += 1
        self.streams.setdefault(key, []).append((record_id, fields))
        return record_id

    async def xrange(self, key, *, min, max, count):
        entries = self.streams.get(key, [])
        if min.startswith("("):
            previous_id = min[1:]
            entries = [entry for entry in entries if entry[0] != previous_id and entry[0] > previous_id]
        return entries[:count]

    async def xdel(self, key, record_id):
        entries = self.streams.get(key, [])
        self.streams[key] = [entry for entry in entries if entry[0] != record_id]
        return len(entries) - len(self.streams[key])

    async def xlen(self, key):
        if self.xlen_error is not None:
            raise self.xlen_error
        return len(self.streams.get(key, []))

    def pipeline(self, *, transaction):
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.operations = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    def xadd(self, key, fields):
        self.operations.append(("xadd", key, fields))
        return self

    def xdel(self, key, record_id):
        self.operations.append(("xdel", key, record_id))
        return self

    async def execute(self):
        for operation, key, value in self.operations:
            if operation == "xadd":
                await self.redis.xadd(key, value)
            else:
                await self.redis.xdel(key, value)


class FakeResponse:
    def __init__(self, status: int, detail: str = ""):
        self.status = status
        self.detail = detail

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def text(self) -> str:
        return self.detail


class FakeSession:
    def __init__(self, responses: list[FakeResponse]):
        self.responses = responses
        self.payloads: list[dict] = []

    def post(self, _url, *, json, timeout):
        self.payloads.append(json)
        return self.responses.pop(0)


@pytest.fixture
def stored_batch() -> StoredBatch:
    return StoredBatch(
        record_id="1-0",
        batch_id="a3634aa6-364e-4090-958b-1b94932429d5",
        source="skinport",
        ticks=[{"market_hash_name": "Test Item", "price_cents": 1000, "timestamp": 1700000000}],
    )


def load_listener_main():
    module_path = Path(__file__).resolve().parents[1] / "main.py"
    spec = importlib.util.spec_from_file_location("listener_main_batch_test", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load listener main module for batching test")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param("[]", id="not_an_object"),
        pytest.param('{"batch_id":"","source":"skinport","ticks":[]}', id="empty_batch_id"),
        pytest.param('{"batch_id":"not-a-uuid","source":"skinport","ticks":[]}', id="invalid_batch_id"),
        pytest.param(
            '{"batch_id":"a3634aa6-364e-4090-958b-1b94932429d5","source":" ","ticks":[]}',
            id="blank_source",
        ),
        pytest.param(
            '{"batch_id":"a3634aa6-364e-4090-958b-1b94932429d5","source":"skinport","ticks":["bad"]}',
            id="invalid_ticks",
        ),
    ],
)
def test_stored_batch_rejects_invalid_payload_shapes(payload):
    with pytest.raises(ValueError):
        StoredBatch.deserialize("1-0", payload)


def test_stored_batch_rejects_missing_required_fields():
    payload = json.dumps({"batch_id": "a3634aa6-364e-4090-958b-1b94932429d5", "source": "skinport"})

    with pytest.raises(KeyError):
        StoredBatch.deserialize("1-0", payload)


@pytest.mark.asyncio
async def test_transient_failure_retries_with_same_batch_id(stored_batch, monkeypatch):
    session = FakeSession([FakeResponse(503, "busy"), FakeResponse(201)])
    sleep = AsyncMock()
    monkeypatch.setattr("batch_delivery.asyncio.sleep", sleep)

    async def get_session():
        return session

    await send_batch_with_retry(
        stored_batch,
        url="http://backend/api/v1/ingest/bulk",
        session_factory=get_session,
        max_attempts=3,
        base_delay_seconds=0.01,
        max_delay_seconds=0.1,
    )

    assert len(session.payloads) == 2
    assert {payload["batch_id"] for payload in session.payloads} == {stored_batch.batch_id}
    sleep.assert_awaited_once()


@pytest.mark.asyncio
async def test_permanent_failure_does_not_retry(stored_batch):
    session = FakeSession([FakeResponse(400, "invalid")])

    async def get_session():
        return session

    with pytest.raises(BatchDeliveryError, match="HTTP 400"):
        await send_batch_with_retry(
            stored_batch,
            url="http://backend/api/v1/ingest/bulk",
            session_factory=get_session,
            max_attempts=3,
            base_delay_seconds=0.01,
            max_delay_seconds=0.1,
        )

    assert len(session.payloads) == 1


@pytest.mark.asyncio
async def test_connection_failure_is_reported_as_retryable(stored_batch):
    async def fail_to_connect():
        raise aiohttp.ClientConnectionError("unreachable")

    with pytest.raises(BatchDeliveryError, match="Backend connection failed") as error:
        await send_batch_with_retry(
            stored_batch,
            url="http://backend/api/v1/ingest/bulk",
            session_factory=fail_to_connect,
            max_attempts=1,
            base_delay_seconds=0.01,
            max_delay_seconds=0.1,
        )

    assert error.value.retryable is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_attempts", "base_delay_seconds", "max_delay_seconds", "message"),
    [
        (0, 0.01, 0.1, "max_attempts must be at least 1"),
        (1, 0, 0.1, "retry delays must be greater than 0"),
        (1, 0.01, 0, "retry delays must be greater than 0"),
    ],
)
async def test_retry_configuration_is_validated(
    stored_batch,
    max_attempts,
    base_delay_seconds,
    max_delay_seconds,
    message,
):
    with pytest.raises(ValueError, match=message):
        await send_batch_with_retry(
            stored_batch,
            url="http://backend/api/v1/ingest/bulk",
            session_factory=AsyncMock(),
            max_attempts=max_attempts,
            base_delay_seconds=base_delay_seconds,
            max_delay_seconds=max_delay_seconds,
        )


@pytest.mark.asyncio
async def test_defensive_error_when_attempt_loop_does_not_run(stored_batch, monkeypatch):
    monkeypatch.setattr(batch_delivery, "range", lambda *_args: (), raising=False)

    with pytest.raises(RuntimeError, match="without an error"):
        await send_batch_with_retry(
            stored_batch,
            url="http://backend/api/v1/ingest/bulk",
            session_factory=AsyncMock(),
            max_attempts=1,
            base_delay_seconds=0.01,
            max_delay_seconds=0.1,
        )


@pytest.mark.asyncio
async def test_acknowledges_only_after_success(stored_batch):
    session = FakeSession([FakeResponse(201)])
    store = AsyncMock()

    async def get_session():
        return session

    await deliver_stored_batch(
        store,
        stored_batch,
        url="http://backend/api/v1/ingest/bulk",
        session_factory=get_session,
        max_attempts=1,
        base_delay_seconds=0.01,
        max_delay_seconds=0.1,
    )

    store.acknowledge.assert_awaited_once_with(stored_batch.record_id)
    store.dead_letter.assert_not_awaited()


@pytest.mark.asyncio
async def test_exhausted_batch_moves_to_dead_letter(stored_batch):
    session = FakeSession([FakeResponse(503, "busy")])
    store = AsyncMock()

    async def get_session():
        return session

    with pytest.raises(BatchDeliveryError, match="HTTP 503"):
        await deliver_stored_batch(
            store,
            stored_batch,
            url="http://backend/api/v1/ingest/bulk",
            session_factory=get_session,
            max_attempts=1,
            base_delay_seconds=0.01,
            max_delay_seconds=0.1,
        )

    store.dead_letter.assert_awaited_once()
    store.acknowledge.assert_not_awaited()


@pytest.mark.asyncio
async def test_redis_store_persists_and_dead_letters_batches():
    redis = FakeRedis()
    store = RedisBatchStore(redis, pending_key="pending", dead_letter_key="dead-letter")

    stored = await store.add(
        "skinport",
        [{"market_hash_name": "Test Item", "price_cents": 1000, "timestamp": 1700000000}],
    )
    recovered = [batch async for batch in store.iter_pending()]

    assert recovered == [stored]
    await store.dead_letter(stored, RuntimeError("permanent failure"))
    assert [batch async for batch in store.iter_pending()] == []
    dead_letters = [batch async for batch in store.iter_dead_letters()]
    assert len(dead_letters) == 1
    assert dead_letters[0].batch_id == stored.batch_id
    assert dead_letters[0].ticks == stored.ticks


@pytest.mark.asyncio
async def test_redis_store_lifecycle_and_acknowledgements(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr("batch_delivery.Redis.from_url", lambda *_args, **_kwargs: redis)
    store = RedisBatchStore.from_url("redis://edge:6380", password="secret")

    async with store as entered:
        assert entered is store
        stored = await store.add("skinport", [])
        await store.acknowledge(stored.record_id)
        redis.streams["listener:ingest:dead-letter"] = [("2-0", {"payload": stored.serialize()})]
        await store.acknowledge_dead_letter("2-0")

    assert redis.pinged is True
    assert redis.closed is True
    assert redis.streams["listener:ingest:pending"] == []
    assert redis.streams["listener:ingest:dead-letter"] == []


@pytest.mark.asyncio
async def test_successful_xadd_is_not_invalidated_by_metric_failure():
    redis = FakeRedis()
    redis.xlen_error = RuntimeError("metrics unavailable")
    store = RedisBatchStore(redis, pending_key="pending", dead_letter_key="dead-letter")
    stable_batch_id = "a3634aa6-364e-4090-958b-1b94932429d5"

    stored = await store.add("skinport", [{"item": 1}], batch_id=stable_batch_id)

    assert stored.batch_id == stable_batch_id
    assert len(redis.streams["pending"]) == 1


@pytest.mark.asyncio
async def test_buffer_ownership_transfers_before_scheduling_can_be_cancelled(monkeypatch, stored_batch):
    listener_main = load_listener_main()
    store = AsyncMock()
    store.add.return_value = stored_batch
    scheduling_started = asyncio.Event()

    async def block_scheduling(*_args):
        scheduling_started.set()
        await asyncio.Future()

    monkeypatch.setattr(listener_main, "schedule_stored_batch", block_scheduling)
    buffer = stored_batch.ticks.copy()

    flush_task = asyncio.create_task(
        listener_main.flush_batch_buffer(
            stored_batch.source,
            buffer,
            batch_id=stored_batch.batch_id,
            store=store,
            batch_pool=AsyncMock(),
        )
    )
    await scheduling_started.wait()

    assert buffer == []
    store.add.assert_awaited_once_with(stored_batch.source, stored_batch.ticks, batch_id=stored_batch.batch_id)

    flush_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await flush_task


@pytest.mark.asyncio
async def test_stream_iteration_skips_malformed_entries_and_paginates(stored_batch):
    redis = AsyncMock()
    redis.xrange.side_effect = [
        [
            (None, None),
            (b"2-0", {b"payload": stored_batch.serialize().encode()}),
        ],
        [],
    ]
    store = RedisBatchStore(redis, pending_key="pending", dead_letter_key="dead-letter")

    recovered = [batch async for batch in store.iter_pending(page_size=2)]

    assert recovered == [
        StoredBatch(
            record_id="2-0",
            batch_id=stored_batch.batch_id,
            source=stored_batch.source,
            ticks=stored_batch.ticks,
        )
    ]
    assert redis.xrange.await_args_list[1].kwargs["min"] == "(2-0"


@pytest.mark.asyncio
async def test_stream_iteration_paginates_after_string_id():
    redis = FakeRedis()
    store = RedisBatchStore(redis, pending_key="pending", dead_letter_key="dead-letter")
    for item in range(3):
        await store.add("skinport", [{"item": item}])

    recovered = [batch async for batch in store.iter_pending(page_size=2)]

    assert len(recovered) == 3


@pytest.mark.asyncio
async def test_stream_iteration_quarantines_poison_record_and_continues(stored_batch):
    redis = FakeRedis()
    redis.streams["pending"] = [
        ("1-0", {"payload": "not-json"}),
        ("2-0", {}),
        ("3-0", {"payload": stored_batch.serialize()}),
    ]
    store = RedisBatchStore(redis, pending_key="pending", dead_letter_key="dead-letter", malformed_key="malformed")

    recovered = [batch async for batch in store.iter_pending()]

    assert recovered == [
        StoredBatch(
            record_id="3-0",
            batch_id=stored_batch.batch_id,
            source=stored_batch.source,
            ticks=stored_batch.ticks,
        )
    ]
    assert redis.streams["pending"] == [("3-0", {"payload": stored_batch.serialize()})]
    assert len(redis.streams["malformed"]) == 2
    assert redis.streams["malformed"][0][1]["source_record_id"] == "1-0"
    assert redis.streams["malformed"][1][1]["source_record_id"] == "2-0"
