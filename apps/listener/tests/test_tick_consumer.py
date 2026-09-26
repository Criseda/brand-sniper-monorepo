import asyncio
import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from models import FeedEvent, MarketTick


def load_listener_main():
    module_path = Path(__file__).resolve().parents[1] / "main.py"
    spec = importlib.util.spec_from_file_location("listener_main_consumer_test", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load listener main module for consumer tests")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


listener_main = load_listener_main()


def _tick(price_usd: float, *, event_type: str | None = None, listing_id: str | None = None) -> MarketTick:
    return MarketTick(
        venue="skinport",
        market_hash_name="AK-47 | Slate (Field-Tested)",
        price_usd=price_usd,
        timestamp=1_790_000_000,
        event_type=event_type,
        listing_id=listing_id,
    )


async def _run_consumer(monkeypatch, items: list) -> tuple[AsyncMock, list[dict]]:
    """Drive tick_consumer over `items` and return the detection mock plus every flushed batch."""
    cache = MagicMock()
    cache.aclose = AsyncMock()
    monkeypatch.setattr(listener_main.Redis, "from_url", lambda *_args, **_kwargs: cache)

    detect = AsyncMock()
    monkeypatch.setattr(listener_main, "update_window_and_detect", detect)

    flushed: list[dict] = []

    async def record_flush(source, buffer, *, batch_id, store, batch_pool, feed_event_buffer=None):
        flushed.append({"ticks": buffer.copy(), "feed_events": list(feed_event_buffer or [])})
        buffer.clear()
        if feed_event_buffer is not None:
            feed_event_buffer.clear()

    monkeypatch.setattr(listener_main, "flush_batch_buffer", record_flush)

    queue: asyncio.Queue = asyncio.Queue()
    for item in [*items, None]:
        queue.put_nowait(item)
    await listener_main.tick_consumer(queue, "skinport", AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock())
    return detect, flushed


@pytest.mark.asyncio
async def test_sold_ticks_and_feed_events_are_recorded_but_never_scored(monkeypatch):
    feed_event = FeedEvent(event_type="sold", received_at_ms=1_790_000_000_123, payload={"eventType": "sold", "sales": []})
    sold = _tick(3.97, event_type="sold", listing_id="58903454")

    detect, flushed = await _run_consumer(monkeypatch, [feed_event, sold])

    detect.assert_not_awaited()
    assert flushed == [{"ticks": [sold.to_batch_record()], "feed_events": [feed_event.to_batch_record()]}]


@pytest.mark.asyncio
async def test_listed_and_snapshot_ticks_still_drive_detection(monkeypatch):
    listed = _tick(3.97, event_type="listed", listing_id="1")
    snapshot = _tick(4.10)

    detect, flushed = await _run_consumer(monkeypatch, [listed, snapshot])

    assert [call.args[0] for call in detect.await_args_list] == [listed, snapshot]
    assert flushed[0]["ticks"] == [listed.to_batch_record(), snapshot.to_batch_record()]


@pytest.mark.asyncio
async def test_repeat_price_listing_is_recorded_without_reentering_the_window(monkeypatch):
    first = _tick(3.97, event_type="listed", listing_id="1")
    second_listing = _tick(3.97, event_type="listed", listing_id="2")
    repeated_snapshot = _tick(3.97)

    detect, flushed = await _run_consumer(monkeypatch, [first, second_listing, repeated_snapshot])

    assert [call.args[0] for call in detect.await_args_list] == [first]
    assert flushed[0]["ticks"] == [first.to_batch_record(), second_listing.to_batch_record()]


@pytest.mark.asyncio
async def test_feed_event_buffer_flushes_at_chunk_limit(monkeypatch):
    monkeypatch.setattr(listener_main, "CHUNK_LIMIT", 2)
    events = [
        FeedEvent(event_type="sold", received_at_ms=1_790_000_000_000 + offset, payload={"n": offset}) for offset in range(3)
    ]

    _detect, flushed = await _run_consumer(monkeypatch, events)

    assert [len(batch["feed_events"]) for batch in flushed] == [2, 1]
    assert all(batch["ticks"] == [] for batch in flushed)
