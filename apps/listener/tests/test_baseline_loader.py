import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, cast

import aiohttp
import baseline_loader
import pytest
import pytest_asyncio
from aiohttp import web
from baseline_loader import (
    BASELINE_REFRESH_SECONDS,
    BASELINE_RETRY_SECONDS,
    BaselineState,
    LoadedBuild,
    baselines_url,
    fetch_latest_build,
    keep_baselines_loaded,
    read_loaded_build,
    refresh_baselines,
    store_build,
)
from listener_telemetry import baseline_build_age_seconds, baselines_loaded
from redis.asyncio import Redis

VENUE = "skinport"
ITEM = "AK-47 | Redline (Field-Tested)"
BUILT_AT = datetime(2026, 9, 26, 12, 0)


class FakeHashRedis:
    """The hash commands the loader uses, with Redis semantics (decode_responses=True)."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}

    async def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.hashes.get(name, {}))

    async def hget(self, name: str, key: str) -> str | None:
        return self.hashes.get(name, {}).get(key)

    async def hset(self, name: str, mapping: dict[str, str]) -> int:
        self.hashes.setdefault(name, {}).update(mapping)
        return len(mapping)

    async def delete(self, name: str) -> int:
        return 1 if self.hashes.pop(name, None) is not None else 0

    async def rename(self, source: str, destination: str) -> bool:
        self.hashes[destination] = self.hashes.pop(source)
        return True


def build(build_id: int = 7, built_at: datetime = BUILT_AT, items: dict | None = None, stickers: dict | None = None) -> dict:
    return {
        "build_id": build_id,
        "venue": VENUE,
        "method": "sales-history-v1",
        "built_at": built_at.isoformat(),
        "item_count": 1,
        "baselines": items if items is not None else {ITEM: {"latest_price_cents": 2739, "support_floor_cents": 2637}},
        "sticker_prices": stickers if stickers is not None else {"Crown (Foil)": 90000},
    }


def as_redis(fake: FakeHashRedis) -> Redis:
    return cast(Redis, fake)


@pytest.mark.asyncio
async def test_store_build_replaces_the_venue_hashes_and_records_the_build():
    cache = FakeHashRedis()
    await store_build(as_redis(cache), VENUE, build(items={"Old Item": {"latest_price_cents": 1}}))

    loaded = await store_build(as_redis(cache), VENUE, build(build_id=8))

    assert loaded == LoadedBuild(build_id=8, built_at=BUILT_AT, item_count=1)
    assert json.loads(cache.hashes["baselines:skinport"][ITEM]) == {"latest_price_cents": 2739, "support_floor_cents": 2637}
    assert "Old Item" not in cache.hashes["baselines:skinport"]
    assert cache.hashes["sticker_prices:skinport"] == {"Crown (Foil)": "90000"}
    assert not any(key.endswith(":staging") for key in cache.hashes)
    assert await read_loaded_build(as_redis(cache), VENUE) == loaded


@pytest.mark.asyncio
async def test_a_build_without_sticker_prices_clears_the_old_ones():
    cache = FakeHashRedis()
    await store_build(as_redis(cache), VENUE, build())
    await store_build(as_redis(cache), VENUE, build(build_id=8, stickers={}))

    assert "sticker_prices:skinport" not in cache.hashes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "meta",
    [{}, {"build_id": "x", "built_at": "2026-09-26T12:00:00", "item_count": "1"}, {"build_id": "1", "item_count": "1"}],
)
async def test_missing_or_garbled_meta_means_nothing_is_loaded(meta):
    cache = FakeHashRedis()
    cache.hashes["baseline_meta:skinport"] = meta
    assert await read_loaded_build(as_redis(cache), VENUE) is None


def test_baseline_state_reports_missing_empty_and_stale_builds():
    state = BaselineState(VENUE, max_age=timedelta(hours=48))
    assert state.problem(BUILT_AT) == "no skinport baselines loaded, so the DRE rejects every anomaly"

    state.loaded = LoadedBuild(build_id=7, built_at=BUILT_AT, item_count=0)
    assert state.problem(BUILT_AT) is not None

    state.loaded = LoadedBuild(build_id=7, built_at=BUILT_AT, item_count=10)
    assert state.problem(BUILT_AT + timedelta(hours=48)) is None
    assert state.problem(BUILT_AT + timedelta(hours=50)) == "skinport baseline build 7 is 50 hours old"


class FakeBackend:
    """Serves builds like the backend: 204 when the caller already has the newest one."""

    def __init__(self) -> None:
        self.documents: list[dict[str, Any]] = []
        self.status: int | None = None
        self.requests: list[dict[str, str]] = []

    async def handle(self, request: web.Request) -> web.Response:
        self.requests.append(dict(request.query))
        if self.status is not None:
            return web.json_response({"detail": "error"}, status=self.status)
        newest = self.documents[-1]
        after = request.query.get("after_build_id")
        if after is not None and newest.get("build_id", 0) <= int(after):
            return web.Response(status=204)
        return web.json_response(newest)


@pytest_asyncio.fixture
async def backend():
    fake = FakeBackend()
    app = web.Application()
    app.router.add_get("/api/v1/baselines/{venue}/latest", fake.handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    async with aiohttp.ClientSession() as session:
        yield fake, session, baselines_url(f"http://127.0.0.1:{port}", VENUE)
    await runner.cleanup()


@pytest.mark.asyncio
async def test_fetch_returns_none_when_the_caller_is_current(backend):
    fake, session, url = backend
    fake.documents.append(build())

    assert (await fetch_latest_build(session, url, None))["build_id"] == 7
    assert await fetch_latest_build(session, url, 7) is None
    assert fake.requests == [{}, {"after_build_id": "7"}]


@pytest.mark.asyncio
async def test_fetch_raises_on_errors_and_malformed_builds(backend):
    fake, session, url = backend
    fake.status = 404
    with pytest.raises(aiohttp.ClientResponseError):
        await fetch_latest_build(session, url, None)

    fake.status = None
    for malformed in (
        {"build_id": 1},
        {**build(), "baselines": {}},
        {**build(), "sticker_prices": None},
        {**build(), "built_at": "x"},
    ):
        fake.documents.append(malformed)
        with pytest.raises(ValueError):
            await fetch_latest_build(session, url, None)


@pytest.mark.asyncio
async def test_refresh_loads_once_and_reloads_after_a_redis_restart(backend):
    fake, session, url = backend
    fake.documents.append(build())
    cache = FakeHashRedis()

    first = await refresh_baselines(session, url, as_redis(cache), VENUE)
    unchanged = await refresh_baselines(session, url, as_redis(cache), VENUE)
    cache.hashes.clear()  # the edge Redis restarted: RAM only
    reloaded = await refresh_baselines(session, url, as_redis(cache), VENUE)

    assert first == unchanged == reloaded
    assert fake.requests == [{}, {"after_build_id": "7"}, {}]
    assert ITEM in cache.hashes["baselines:skinport"]


class StopLoop(Exception):
    pass


def _sleeps(limit: int) -> tuple[list[float], Any]:
    delays: list[float] = []

    async def sleep(seconds: float) -> None:
        delays.append(seconds)
        if len(delays) >= limit:
            raise StopLoop

    return delays, sleep


@pytest.mark.asyncio
async def test_loop_retries_quickly_while_baselines_are_missing(backend, caplog):
    fake, session, url = backend
    fake.status = 404
    state = BaselineState(VENUE)
    delays, sleep = _sleeps(2)

    async def session_factory() -> aiohttp.ClientSession:
        return session

    with pytest.raises(StopLoop):
        await keep_baselines_loaded(state, session_factory, url, as_redis(FakeHashRedis()), clock=lambda: BUILT_AT, sleep=sleep)

    assert delays == [BASELINE_RETRY_SECONDS, BASELINE_RETRY_SECONDS]
    assert state.loaded is None
    assert baselines_loaded.labels(venue=VENUE)._value.get() == 0
    assert "the DRE rejects every anomaly" in caplog.text


@pytest.mark.asyncio
async def test_loop_settles_to_the_refresh_interval_once_loaded(backend):
    fake, session, url = backend
    fake.documents.append(build())
    state = BaselineState(VENUE)
    delays, sleep = _sleeps(2)

    async def session_factory() -> aiohttp.ClientSession:
        return session

    now = BUILT_AT + timedelta(hours=1)
    with pytest.raises(StopLoop):
        await keep_baselines_loaded(state, session_factory, url, as_redis(FakeHashRedis()), clock=lambda: now, sleep=sleep)

    assert delays == [BASELINE_REFRESH_SECONDS, BASELINE_REFRESH_SECONDS]
    assert state.loaded == LoadedBuild(build_id=7, built_at=BUILT_AT, item_count=1)
    assert baselines_loaded.labels(venue=VENUE)._value.get() == 1
    assert baseline_build_age_seconds.labels(venue=VENUE)._value.get() == 3600


@pytest.mark.asyncio
async def test_loop_keeps_checking_quickly_while_the_build_is_stale(backend):
    fake, session, url = backend
    fake.documents.append(build())
    state = BaselineState(VENUE, max_age=timedelta(hours=48))
    delays, sleep = _sleeps(1)

    async def session_factory() -> aiohttp.ClientSession:
        return session

    with pytest.raises(StopLoop):
        await keep_baselines_loaded(
            state, session_factory, url, as_redis(FakeHashRedis()), clock=lambda: BUILT_AT + timedelta(days=3), sleep=sleep
        )

    assert delays == [BASELINE_RETRY_SECONDS]


@pytest.mark.asyncio
async def test_loop_survives_unexpected_errors():
    state = BaselineState(VENUE)
    delays, sleep = _sleeps(1)

    async def broken_session() -> aiohttp.ClientSession:
        raise RuntimeError("boom")

    with pytest.raises(StopLoop):
        await keep_baselines_loaded(state, broken_session, "http://unused", as_redis(FakeHashRedis()), sleep=sleep)

    assert delays == [BASELINE_RETRY_SECONDS]


def test_cancelling_the_loop_is_not_swallowed():
    async def run() -> None:
        state = BaselineState(VENUE)

        async def session_factory() -> aiohttp.ClientSession:
            raise asyncio.CancelledError

        await keep_baselines_loaded(state, session_factory, "http://unused", as_redis(FakeHashRedis()))

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(run())


def test_module_defaults_allow_a_day_of_missed_builds():
    assert baseline_loader.BASELINE_MAX_AGE_HOURS >= 24
