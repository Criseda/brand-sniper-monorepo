import asyncio
import json
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import aiohttp
from listener_telemetry import (
    batch_delivery_dead_letters_total,
    batch_delivery_pending,
    batch_delivery_retries_total,
    batch_flush_total,
)
from redis.asyncio import Redis
from shared_utils import get_logger

logger = get_logger("listener.batch_delivery")

SessionFactory = Callable[[], Awaitable[aiohttp.ClientSession]]


@dataclass(frozen=True, slots=True)
class StoredBatch:
    record_id: str
    batch_id: str
    source: str
    ticks: list[dict[str, Any]]

    @property
    def payload(self) -> dict[str, Any]:
        return {"batch_id": self.batch_id, "source": self.source, "ticks": self.ticks}

    def serialize(self) -> str:
        return json.dumps(self.payload, separators=(",", ":"))

    @classmethod
    def deserialize(cls, record_id: str | bytes, payload: str | bytes) -> "StoredBatch":
        decoded_id = record_id.decode() if isinstance(record_id, bytes) else record_id
        decoded_payload = payload.decode() if isinstance(payload, bytes) else payload
        data = json.loads(decoded_payload)
        return cls(
            record_id=decoded_id,
            batch_id=data["batch_id"],
            source=data["source"],
            ticks=data["ticks"],
        )


class BatchDeliveryError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool):
        super().__init__(message)
        self.retryable = retryable


class RedisBatchStore:
    """Redis Stream-backed pending queue and dead-letter queue."""

    def __init__(self, redis: Redis, *, pending_key: str, dead_letter_key: str):
        self.redis = redis
        self.pending_key = pending_key
        self.dead_letter_key = dead_letter_key

    @classmethod
    def from_url(
        cls,
        redis_url: str,
        *,
        password: str | None,
        pending_key: str = "listener:ingest:pending",
        dead_letter_key: str = "listener:ingest:dead-letter",
    ) -> "RedisBatchStore":
        redis = Redis.from_url(redis_url, username="default", password=password, decode_responses=True)
        return cls(redis, pending_key=pending_key, dead_letter_key=dead_letter_key)

    async def __aenter__(self) -> "RedisBatchStore":
        await self.redis.ping()
        await self._update_pending_metric()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.redis.aclose()

    async def add(self, source: str, ticks: list[dict[str, Any]]) -> StoredBatch:
        batch = StoredBatch(record_id="", batch_id=str(uuid4()), source=source, ticks=ticks)
        record_id = await self.redis.xadd(self.pending_key, {"payload": batch.serialize()})
        await self._update_pending_metric()
        return StoredBatch(record_id=str(record_id), batch_id=batch.batch_id, source=source, ticks=ticks)

    async def iter_pending(self, *, page_size: int = 100) -> AsyncIterator[StoredBatch]:
        async for batch in self._iter_stream(self.pending_key, page_size=page_size):
            yield batch

    async def iter_dead_letters(self, *, page_size: int = 100) -> AsyncIterator[StoredBatch]:
        async for batch in self._iter_stream(self.dead_letter_key, page_size=page_size):
            yield batch

    async def acknowledge(self, record_id: str) -> None:
        await self.redis.xdel(self.pending_key, record_id)
        await self._update_pending_metric()

    async def acknowledge_dead_letter(self, record_id: str) -> None:
        await self.redis.xdel(self.dead_letter_key, record_id)

    async def dead_letter(self, batch: StoredBatch, error: Exception) -> None:
        async with self.redis.pipeline(transaction=True) as pipeline:
            pipeline.xadd(
                self.dead_letter_key,
                {
                    "payload": batch.serialize(),
                    "last_error": str(error)[:500],
                },
            )
            pipeline.xdel(self.pending_key, batch.record_id)
            await pipeline.execute()
        batch_delivery_dead_letters_total.inc()
        await self._update_pending_metric()

    async def _iter_stream(self, key: str, *, page_size: int) -> AsyncIterator[StoredBatch]:
        start = "-"
        while True:
            entries = await self.redis.xrange(key, min=start, max="+", count=page_size)
            if not entries:
                return
            for record_id, fields in entries:
                if record_id is None or fields is None:
                    logger.error("[BATCH FLUSH] Ignoring malformed Redis stream entry.")
                    continue
                payload = fields.get("payload") or fields.get(b"payload")
                if payload is None:
                    logger.error("[BATCH FLUSH] Ignoring malformed Redis stream entry '%s'.", record_id)
                    continue
                yield StoredBatch.deserialize(record_id, payload)
            if len(entries) < page_size:
                return
            last_id = entries[-1][0]
            if isinstance(last_id, bytes):
                last_id = last_id.decode()
            start = f"({last_id}"

    async def _update_pending_metric(self) -> None:
        batch_delivery_pending.set(await self.redis.xlen(self.pending_key))


async def send_batch_with_retry(
    batch: StoredBatch,
    *,
    url: str,
    session_factory: SessionFactory,
    max_attempts: int,
    base_delay_seconds: float,
    max_delay_seconds: float,
) -> None:
    """Send a batch with bounded retries for transient transport and HTTP errors."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if base_delay_seconds <= 0 or max_delay_seconds <= 0:
        raise ValueError("retry delays must be greater than 0")

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            session = await session_factory()
            async with session.post(
                url,
                json=batch.payload,
                timeout=aiohttp.ClientTimeout(total=15, connect=3),
            ) as response:
                if 200 <= response.status < 300:
                    logger.info(
                        "[BATCH FLUSH] Committed batch %s with %d items to Compute Node.",
                        batch.batch_id,
                        len(batch.ticks),
                    )
                    batch_flush_total.labels(status="success").inc()
                    return

                response_detail = (await response.text())[:500]
                retryable = response.status in {408, 425, 429} or response.status >= 500
                raise BatchDeliveryError(
                    f"Backend returned HTTP {response.status}: {response_detail}",
                    retryable=retryable,
                )
        except (TimeoutError, aiohttp.ClientError) as exc:
            last_error = BatchDeliveryError(f"Backend connection failed: {exc}", retryable=True)
        except BatchDeliveryError as exc:
            last_error = exc

        if not isinstance(last_error, BatchDeliveryError) or not last_error.retryable or attempt == max_attempts:
            break

        batch_delivery_retries_total.inc()
        batch_flush_total.labels(status="retry").inc()
        delay = min(max_delay_seconds, base_delay_seconds * (2 ** (attempt - 1)))
        delay += random.uniform(0, delay * 0.25)
        logger.warning(
            "[BATCH FLUSH] Retrying batch %s after attempt %d/%d: %s",
            batch.batch_id,
            attempt,
            max_attempts,
            last_error,
        )
        await asyncio.sleep(delay)

    batch_flush_total.labels(status="dead_letter").inc()
    raise last_error or RuntimeError("Batch delivery failed without an error")


async def deliver_stored_batch(
    store: RedisBatchStore,
    batch: StoredBatch,
    *,
    url: str,
    session_factory: SessionFactory,
    max_attempts: int,
    base_delay_seconds: float,
    max_delay_seconds: float,
) -> None:
    try:
        await send_batch_with_retry(
            batch,
            url=url,
            session_factory=session_factory,
            max_attempts=max_attempts,
            base_delay_seconds=base_delay_seconds,
            max_delay_seconds=max_delay_seconds,
        )
    except Exception as exc:
        await store.dead_letter(batch, exc)
        raise
    await store.acknowledge(batch.record_id)
