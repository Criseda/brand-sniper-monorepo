from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import replay_batches
from batch_delivery import StoredBatch


class FakeBatchStore:
    def __init__(self, batches: list[StoredBatch]):
        self.batches = batches
        self.acknowledged: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def iter_dead_letters(self):
        for batch in self.batches:
            yield batch

    async def acknowledge_dead_letter(self, record_id: str):
        self.acknowledged.append(record_id)


class FakeClientSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None


def make_batch(record_id: str) -> StoredBatch:
    return StoredBatch(
        record_id=record_id,
        batch_id=f"00000000-0000-0000-0000-{int(record_id):012d}",
        source="skinport",
        ticks=[{"market_hash_name": "Test Item", "price_cents": 1000, "timestamp": 1700000000}],
    )


@pytest.mark.asyncio
async def test_replay_acknowledges_success_and_honors_limit(monkeypatch):
    store = FakeBatchStore([make_batch("1"), make_batch("2")])
    send = AsyncMock()

    async def send_success(*_args, session_factory, **_kwargs):
        assert await session_factory() is not None

    send.side_effect = send_success
    monkeypatch.setattr(replay_batches.RedisBatchStore, "from_url", lambda *_args, **_kwargs: store)
    monkeypatch.setattr(replay_batches.aiohttp, "ClientSession", FakeClientSession)
    monkeypatch.setattr(replay_batches, "send_batch_with_retry", send)

    replayed = await replay_batches.replay(limit=1)

    assert replayed == 1
    assert store.acknowledged == ["1"]
    send.assert_awaited_once()


@pytest.mark.asyncio
async def test_replay_retains_failure_and_continues(monkeypatch):
    store = FakeBatchStore([make_batch("1"), make_batch("2")])
    attempts = 0

    async def send_with_first_failure(*_args, session_factory, **_kwargs):
        nonlocal attempts
        attempts += 1
        assert await session_factory() is not None
        if attempts == 1:
            raise RuntimeError("still unavailable")

    send = AsyncMock(side_effect=send_with_first_failure)
    monkeypatch.setattr(replay_batches.RedisBatchStore, "from_url", lambda *_args, **_kwargs: store)
    monkeypatch.setattr(replay_batches.aiohttp, "ClientSession", FakeClientSession)
    monkeypatch.setattr(replay_batches, "send_batch_with_retry", send)

    replayed = await replay_batches.replay(limit=2)

    assert replayed == 1
    assert store.acknowledged == ["2"]
    assert send.await_count == 2


@pytest.mark.asyncio
async def test_replay_rejects_non_positive_limit():
    with pytest.raises(ValueError, match="at least 1"):
        await replay_batches.replay(limit=0)


def test_parse_args_reads_limit(monkeypatch):
    monkeypatch.setattr("sys.argv", ["replay_batches.py", "--limit", "7"])

    assert replay_batches.parse_args().limit == 7


def test_main_runs_replay_with_parsed_limit(monkeypatch):
    captured = []

    def run(coroutine):
        captured.append(coroutine)
        coroutine.close()

    monkeypatch.setattr(replay_batches, "parse_args", lambda: SimpleNamespace(limit=3))
    monkeypatch.setattr(replay_batches.asyncio, "run", run)

    replay_batches.main()

    assert len(captured) == 1
