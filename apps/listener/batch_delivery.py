import asyncio
import json
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import aiohttp
from listener_telemetry import (
    batch_delivery_dead_letters_total,
    batch_delivery_malformed_total,
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
        if not isinstance(data, dict):
            raise ValueError("Stored batch payload must be a JSON object")

        batch_id = data["batch_id"]
        source = data["source"]
        ticks = data["ticks"]
        if not isinstance(batch_id, str) or not batch_id:
            raise ValueError("Stored batch_id must be a non-empty string")
        UUID(batch_id)
        if not isinstance(source, str) or not source.strip():
            raise ValueError("Stored batch source must be a non-empty string")
        if not isinstance(ticks, list) or not all(isinstance(tick, dict) for tick in ticks):
            raise ValueError("Stored batch ticks must be a list of objects")

        return cls(
            record_id=decoded_id,
            batch_id=batch_id,
            source=source,
            ticks=ticks,
        )


class BatchDeliveryError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool):
        super().__init__(message)
        self.retryable = retryable


class RedisBatchStore:
    """Redis Stream-backed pending queue and dead-letter queue."""

    def __init__(
        self,
        redis: Redis,
        *,
        pending_key: str,
        dead_letter_key: str,
        malformed_key: str = "listener:ingest:malformed",
    ):
        self.redis = redis
        self.pending_key = pending_key
        self.dead_letter_key = dead_letter_key
        self.malformed_key = malformed_key

    @classmethod
    def from_url(
        cls,
        redis_url: str,
        *,
        password: str | None,
        pending_key: str = "listener:ingest:pending",
        dead_letter_key: str = "listener:ingest:dead-letter",
        malformed_key: str = "listener:ingest:malformed",
    ) -> "RedisBatchStore":
        redis = Redis.from_url(redis_url, username="default", password=password, decode_responses=True)
        return cls(
            redis,
            pending_key=pending_key,
            dead_letter_key=dead_letter_key,
            malformed_key=malformed_key,
        )

    async def __aenter__(self) -> "RedisBatchStore":
        await self.redis.ping()
        await self._refresh_pending_metric()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.redis.aclose()

    async def add(
        self,
        source: str,
        ticks: list[dict[str, Any]],
        *,
        batch_id: str | None = None,
    ) -> StoredBatch:
        batch = StoredBatch(record_id="", batch_id=batch_id or str(uuid4()), source=source, ticks=ticks)
        record_id = await self.redis.xadd(self.pending_key, {"payload": batch.serialize()})
        await self._refresh_pending_metric()
        decoded_record_id = record_id.decode() if isinstance(record_id, bytes) else str(record_id)
        return StoredBatch(record_id=decoded_record_id, batch_id=batch.batch_id, source=source, ticks=ticks)

    async def iter_pending(self, *, page_size: int = 100) -> AsyncIterator[StoredBatch]:
        async for batch in self._iter_stream(self.pending_key, page_size=page_size):
            yield batch

    async def iter_dead_letters(self, *, page_size: int = 100) -> AsyncIterator[StoredBatch]:
        async for batch in self._iter_stream(self.dead_letter_key, page_size=page_size):
            yield batch

    async def acknowledge(self, record_id: str) -> None:
        await self.redis.xdel(self.pending_key, record_id)
        await self._refresh_pending_metric()

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
        await self._refresh_pending_metric()

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
                    await self._quarantine_malformed(
                        key,
                        record_id,
                        "",
                        ValueError("Redis stream entry has no payload field"),
                    )
                    continue
                try:
                    batch = StoredBatch.deserialize(record_id, payload)
                except (json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError, ValueError) as exc:
                    await self._quarantine_malformed(key, record_id, payload, exc)
                    continue
                yield batch
            if len(entries) < page_size:
                return
            last_id = entries[-1][0]
            if isinstance(last_id, bytes):
                last_id = last_id.decode()
            start = f"({last_id}"

    async def _quarantine_malformed(
        self,
        source_key: str,
        record_id: str | bytes,
        payload: str | bytes,
        error: Exception,
    ) -> None:
        decoded_record_id = record_id.decode() if isinstance(record_id, bytes) else record_id
        decoded_payload = payload.decode(errors="replace") if isinstance(payload, bytes) else payload
        async with self.redis.pipeline(transaction=True) as pipeline:
            pipeline.xadd(
                self.malformed_key,
                {
                    "source_stream": source_key,
                    "source_record_id": decoded_record_id,
                    "payload": decoded_payload,
                    "last_error": str(error)[:500],
                },
            )
            pipeline.xdel(source_key, decoded_record_id)
            await pipeline.execute()
        logger.error(
            "[BATCH FLUSH] Quarantined malformed Redis stream entry '%s': %s",
            decoded_record_id,
            error,
        )
        batch_delivery_malformed_total.inc()
        await self._refresh_pending_metric()

    async def _refresh_pending_metric(self) -> None:
        """Refresh observability without invalidating a completed queue operation."""
        try:
            pending_count = await self.redis.xlen(self.pending_key)
            batch_delivery_pending.set(pending_count)
        except Exception as exc:
            logger.warning("[BATCH FLUSH] Failed to refresh pending-batch metric: %s", exc)


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
