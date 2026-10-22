import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp.test_utils import TestClient, TestServer


def load_listener_main():
    module_path = Path(__file__).resolve().parents[1] / "main.py"
    spec = importlib.util.spec_from_file_location("listener_main_health_test", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load listener main module for health tests")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


listener_main = load_listener_main()
create_listener_health_app = listener_main.create_listener_health_app


@pytest.mark.asyncio
async def test_listener_bulk_client_uses_api_key():
    session = await listener_main.get_http_session()
    try:
        assert session.headers["X-API-Key"] == "listener-test-key-that-is-at-least-32-characters"
    finally:
        await listener_main.close_http_session()


@pytest.mark.asyncio
async def test_listener_health_runs_on_asyncio_http_server():
    async with TestClient(TestServer(create_listener_health_app())) as client:
        response = await client.get("/health")
        assert response.status == 200
        assert await response.json() == {"status": "healthy"}


@pytest.mark.asyncio
async def test_listener_health_reports_missing_or_stale_baselines():
    from datetime import timedelta

    from baseline_loader import BaselineState, LoadedBuild
    from shared_utils import utc_now_naive

    state = BaselineState("skinport", max_age=timedelta(hours=48))
    async with TestClient(TestServer(create_listener_health_app(state))) as client:
        missing = await client.get("/health")
        assert missing.status == 503
        assert await missing.json() == {
            "status": "degraded",
            "reason": "no skinport baselines loaded, so the DRE rejects every anomaly",
        }

        state.loaded = LoadedBuild(build_id=7, built_at=utc_now_naive() - timedelta(hours=1), item_count=8000)
        current = await client.get("/health")
        assert current.status == 200
        assert await current.json() == {"status": "healthy"}

        state.loaded = LoadedBuild(build_id=7, built_at=utc_now_naive() - timedelta(days=3), item_count=8000)
        assert (await client.get("/health")).status == 503


class _Feed:
    """A live feed whose connections go as scripted: each entry is what one connection yields, then how it ends."""

    def __init__(self, connections):
        self._connections = iter(connections)

    async def listen_websocket_stream(self):
        items, error = next(self._connections)
        for item in items:
            yield item
        if error is not None:
            raise error


@pytest.mark.asyncio
async def test_feed_reconnects_quickly_and_backs_off_while_connections_keep_failing(monkeypatch):
    delays = []

    async def sleep(seconds):
        delays.append(seconds)
        if len(delays) == 6:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", sleep)
    monkeypatch.setattr(listener_main, "FEED_RECONNECT_MAX_DELAY_SECONDS", 3.0)
    feed = _Feed(
        [
            (["a"], ConnectionError("dropped")),
            ([], ConnectionError("refused")),
            ([], ConnectionError("refused")),
            ([], ConnectionError()),
            (["b"], None),  # The feed ended without an error
            ([], ConnectionError("refused")),
        ]
    )
    queue = asyncio.Queue()

    with pytest.raises(asyncio.CancelledError):
        await listener_main.websocket_subscriber_producer(feed, queue)

    # Quick after a connection that delivered, doubling while they fail, capped, and reset by the next delivery.
    assert delays == [1.0, 2.0, 3.0, 3.0, 1.0, 2.0]
    assert [queue.get_nowait(), queue.get_nowait()] == ["a", "b"]


@pytest.mark.asyncio
async def test_a_venue_without_a_live_feed_starts_no_feed_producer():
    await listener_main.websocket_subscriber_producer(SimpleNamespace(), asyncio.Queue())
