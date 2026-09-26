import json
from datetime import date, datetime, timedelta
from pathlib import Path

import build_baselines
import pytest
import pytest_asyncio
from aiohttp import web
from build_baselines import (
    VENUE,
    build_venue_baselines,
    latest_build_time,
    load_daily_medians,
    parse_sales_history,
    save_build,
)
from build_baselines import (
    build_baselines as build_all,
)
from shared_utils import BASELINE_METHOD
from shared_utils.baselines import MIN_DAILY_POINTS, VOLATILITY_FROM_DAILY_MEDIANS, VOLATILITY_FROM_SALES_SPREAD
from shared_utils.models import BaselineBuild, VenueBaseline
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import StaticPool

# Seven real entries from Skinport's sales history on 2026-09-26 (see the fixture for the raw numbers).
FIXTURE = Path(__file__).parent / "fixtures" / "skinport_sales_history.json"
NOW = datetime(2026, 9, 26, 12, 0)

REDLINE = "AK-47 | Redline (Field-Tested)"
DOPPLER_PHASE_4 = "★ Karambit | Doppler (Phase 4) (Factory New)"
CASE = "Dreams & Nightmares Case"
STICKER = "Sticker | 9INE (Glitter) | Paris 2023"


def fixture_entries() -> list[dict]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


@pytest_asyncio.fixture()
async def engine(monkeypatch):
    # One shared connection, so every "connect" sees the same in-memory database.
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: BaselineBuild.metadata.create_all(
                sync_conn, tables=[BaselineBuild.__table__, VenueBaseline.__table__]
            )
        )
    monkeypatch.setattr(build_baselines, "async_engine", engine)
    monkeypatch.setattr(build_baselines, "utc_now_naive", lambda: NOW)
    yield engine
    await engine.dispose()


def test_only_items_with_enough_sales_get_a_baseline():
    histories = parse_sales_history(fixture_entries())

    baselines = build_all(histories, {}, NOW.date())

    assert len(histories) == 7
    # The sapphire Doppler, the sticker slab, and a sticker with 4 sales in 30 days are left out.
    assert [baseline.market_hash_name for baseline in baselines] == sorted([REDLINE, DOPPLER_PHASE_4, CASE, STICKER])


def test_known_items_get_the_documented_numbers():
    by_name = {
        baseline.market_hash_name: baseline for baseline in build_all(parse_sales_history(fixture_entries()), {}, NOW.date())
    }

    redline = by_name[REDLINE]
    assert (redline.latest_price_cents, redline.rolling_30d_avg_cents, redline.volatility_cents) == (2739, 2929, 228)

    doppler = by_name[DOPPLER_PHASE_4]
    # Two sales in the last day is too thin, so the 7 day median is the latest price.
    assert doppler.latest_price_cents == 125735
    assert doppler.rolling_30d_avg_cents == 128895

    sticker = by_name[STICKER]
    assert (sticker.latest_price_cents, sticker.volatility_cents, sticker.support_floor_cents) == (18, 4, 13)


def test_parse_skips_entries_without_a_name_or_in_another_currency():
    entries = fixture_entries()
    entries.append({**entries[0], "currency": "EUR"})
    entries.append({"version": None})
    assert len(parse_sales_history(entries)) == 7


def test_todays_median_joins_the_earlier_daily_medians():
    histories = [history for history in parse_sales_history(fixture_entries()) if history.market_hash_name == CASE]
    earlier = {CASE: {date(2026, 9, 1) + timedelta(days=offset): 120 for offset in range(MIN_DAILY_POINTS - 1)}}

    (with_history,) = build_all(histories, earlier, NOW.date())
    (without_history,) = build_all(histories, {}, NOW.date())

    # 13 earlier days plus today reach MIN_DAILY_POINTS.
    assert with_history.volatility_method == VOLATILITY_FROM_DAILY_MEDIANS
    assert without_history.volatility_method == VOLATILITY_FROM_SALES_SPREAD


@pytest.mark.asyncio
async def test_save_build_round_trips_and_feeds_daily_medians(engine):
    baselines = build_all(parse_sales_history(fixture_entries()), {}, NOW.date())
    earlier = NOW - timedelta(days=1)

    first_id = await save_build(VENUE, earlier, baselines)
    second_id = await save_build(VENUE, NOW, baselines)

    assert second_id == first_id + 1
    assert await latest_build_time(VENUE) == NOW
    assert await latest_build_time("csfloat") is None

    async with engine.connect() as conn:
        build = (await conn.execute(select(BaselineBuild).where(BaselineBuild.id == second_id))).one()
        rows = (await conn.execute(select(VenueBaseline).where(VenueBaseline.build_id == second_id))).all()
    assert (build.venue, build.method, build.item_count) == (VENUE, BASELINE_METHOD, 4)
    assert len(rows) == 4

    medians = await load_daily_medians(VENUE, NOW - timedelta(days=30))
    # Items with a 24 hour median get one point per day; the sticker sold nothing today.
    assert medians[REDLINE] == {earlier.date(): 2739, NOW.date(): 2739}
    assert STICKER not in medians
    assert await load_daily_medians(VENUE, NOW + timedelta(seconds=1)) == {}


@pytest.mark.asyncio
async def test_flow_builds_when_stale_and_skips_when_fresh(engine, monkeypatch):
    calls = []

    async def fake_fetch():
        calls.append(1)
        return fixture_entries()

    monkeypatch.setattr(build_baselines, "fetch_sales_history", fake_fetch)

    first = await build_venue_baselines()
    skipped = await build_venue_baselines()
    forced = await build_venue_baselines(force=True)

    assert first is not None
    assert skipped is None
    assert forced == first + 1
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_flow_rebuilds_once_the_newest_build_is_old_enough(engine, monkeypatch):
    baselines = build_all(parse_sales_history(fixture_entries()), {}, NOW.date())
    await save_build(VENUE, NOW - timedelta(hours=21), baselines)

    async def fake_fetch():
        return fixture_entries()

    monkeypatch.setattr(build_baselines, "fetch_sales_history", fake_fetch)

    assert await build_venue_baselines(max_age_hours=20) is not None


@pytest.mark.asyncio
async def test_flow_refuses_to_store_an_empty_build(engine, monkeypatch):
    async def fake_fetch():
        return [entry for entry in fixture_entries() if entry["last_30_days"]["volume"] < 5]

    monkeypatch.setattr(build_baselines, "fetch_sales_history", fake_fetch)

    with pytest.raises(RuntimeError):
        await build_venue_baselines(force=True)
    assert await latest_build_time(VENUE) is None


async def _serve(handler) -> tuple[web.AppRunner, str]:
    app = web.Application()
    app.router.add_get("/v1/sales/history", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    return runner, f"http://127.0.0.1:{port}/v1/sales/history"


@pytest.mark.asyncio
async def test_fetch_asks_for_usd_with_brotli_and_keeps_only_objects(monkeypatch):
    seen = {}

    async def handler(request: web.Request) -> web.Response:
        seen["query"] = dict(request.query)
        seen["encoding"] = request.headers.get("Accept-Encoding")
        return web.json_response([{"market_hash_name": REDLINE}, "junk"])

    runner, url = await _serve(handler)
    monkeypatch.setattr(build_baselines, "SALES_HISTORY_URL", url)
    try:
        entries = await build_baselines.fetch_sales_history()
    finally:
        await runner.cleanup()

    assert entries == [{"market_hash_name": REDLINE}]
    assert seen == {"query": {"app_id": "730", "currency": "USD"}, "encoding": "br"}


@pytest.mark.asyncio
async def test_fetch_rejects_errors_and_unexpected_shapes(monkeypatch):
    async def not_a_list(_request: web.Request) -> web.Response:
        return web.json_response({"errors": []})

    async def rate_limited(_request: web.Request) -> web.Response:
        return web.json_response([], status=429)

    for handler, error in ((not_a_list, ValueError), (rate_limited, Exception)):
        runner, url = await _serve(handler)
        monkeypatch.setattr(build_baselines, "SALES_HISTORY_URL", url)
        try:
            with pytest.raises(error):
                await build_baselines.fetch_sales_history()
        finally:
            await runner.cleanup()


def test_heartbeat_is_fresh_only_after_a_recent_check(tmp_path):
    heartbeat = tmp_path / "heartbeat"
    assert build_baselines.heartbeat_is_fresh(heartbeat) is False

    build_baselines.record_heartbeat(heartbeat)

    assert build_baselines.heartbeat_is_fresh(heartbeat) is True
    assert build_baselines.heartbeat_is_fresh(heartbeat, max_age_seconds=0) is False
