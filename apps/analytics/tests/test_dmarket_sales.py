import functools
from datetime import UTC, datetime, timedelta
from typing import Any

import build_baselines
import dmarket_sales
import pytest
import pytest_asyncio
from aiohttp import web
from build_baselines import build_dmarket_baselines, latest_build_time, save_build, with_dmarket_fees
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from dmarket_sales import base_titles, fetch_title_sales, offer_sale_histories, skinport_titles, sync_sales
from shared_utils.baselines import VENUE_SALES_METHOD, ItemBaseline
from shared_utils.dmarket import DMarketClient, DMarketKeys, FeeSchedule, ReducedFee
from shared_utils.models import BaselineBuild, VenueBaseline, VenueSale
from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import StaticPool
from sqlmodel import col

NOW = datetime(2026, 10, 25, 12, 0)

REDLINE = "AK-47 | Redline (Field-Tested)"
DOPPLER = "★ Karambit | Doppler (Factory New)"
CASE = "Revolution Case"
SKINPORT_NAMES = [REDLINE, "★ Karambit | Doppler (Phase 2) (Factory New)", "★ Karambit | Doppler (Phase 4) (Factory New)", CASE]


def unix(moment: datetime) -> int:
    return int(moment.replace(tzinfo=UTC).timestamp())


def sale(moment: datetime, price: str, tx: str = "Offer", **attributes) -> dict:
    return {"price": price, "date": str(unix(moment)), "txOperationType": tx, "offerAttributes": attributes}


def sales_by_title() -> dict[str, list[dict]]:
    """Newest first, as DMarket sends them."""
    redline = []
    for day in range(45):
        moment = NOW - timedelta(days=day, hours=1)
        redline.append(sale(moment, "25.00", "Offer", floatValue=0.25, paintSeed=day))
        redline.append(sale(moment - timedelta(minutes=5), "23.00", "Target", floatValue=0.3, paintSeed=day))
    doppler = [
        sale(NOW - timedelta(days=day, hours=2), "1500", "Offer", floatValue=0.01, paintSeed=day, phaseTitle=phase)
        for day in range(12)
        for phase in ("phase-2", "phase-4")
    ]
    # Several identical case sales in one second, as DMarket reports a buyer taking several at once.
    case_second = NOW - timedelta(hours=3)
    case = [sale(case_second, "0.18") for _ in range(6)] + [sale(NOW - timedelta(days=200), "0.20")]
    return {REDLINE: redline, DOPPLER: doppler, CASE: case}


class FakeDMarket:
    """A local stand-in for the two DMarket endpoints the builder reads."""

    def __init__(self) -> None:
        self.sales = sales_by_title()
        self.failing_titles: set[str] = set()
        self.requests: list[tuple[str, int]] = []
        self.base_url = ""

    async def last_sales(self, request: web.Request) -> web.Response:
        assert request.headers["X-Request-Sign"].startswith("dmar ed25519 ")
        title = request.query["title"]
        offset = int(request.query.get("offset", 0))
        limit = int(request.query["limit"])
        self.requests.append((title, offset))
        if title in self.failing_titles:
            return web.json_response({"message": "boom"}, status=500)
        return web.json_response({"sales": self.sales.get(title, [])[offset : offset + limit]})

    async def customized_fees(self, request: web.Request) -> web.Response:
        offset = int(request.query.get("offset", 0))
        reduced = [{"title": REDLINE, "fraction": "0.02", "minPrice": 0, "maxPrice": 247500, "expiresAt": 1793131200}]
        return web.json_response(
            {"defaultFee": {"minAmount": 1, "fraction": "0.1"}, "reducedFees": reduced if offset == 0 else []}
        )


@pytest_asyncio.fixture()
async def engine(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    tables = [BaselineBuild.__table__, VenueBaseline.__table__, VenueSale.__table__]
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync_conn: BaselineBuild.metadata.create_all(sync_conn, tables=tables))
        result = await conn.execute(
            insert(BaselineBuild).values(venue="skinport", method="sales-history-v1", built_at=NOW, item_count=4)
        )
        build_id = result.inserted_primary_key[0]
        for name in SKINPORT_NAMES:
            await conn.execute(insert(VenueBaseline).values(**skinport_row(build_id, name)))
    monkeypatch.setattr(build_baselines, "async_engine", engine)
    monkeypatch.setattr(build_baselines, "utc_now_naive", lambda: NOW)
    yield engine
    await engine.dispose()


def skinport_row(build_id: int, name: str) -> dict:
    return {
        "build_id": build_id,
        "market_hash_name": name,
        "latest_price_cents": 100,
        "rolling_30d_avg_cents": 100,
        "rolling_90d_avg_cents": 100,
        "volatility_cents": 1,
        "support_floor_cents": 90,
        "avg_volume_30d": 1.0,
        "drift_percent": 0.0,
        "volatility_method": "sales_spread",
        "volume_24h": 0,
        "volume_7d": 0,
        "min_30d_cents": 90,
        "volume_30d": 30,
        "volume_90d": 90,
    }


@pytest_asyncio.fixture()
async def dmarket(monkeypatch):
    fake = FakeDMarket()
    app = web.Application()
    app.router.add_get("/trade-aggregator/v1/last-sales", fake.last_sales)
    app.router.add_get("/exchange/v1/customized-fees", fake.customized_fees)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    base_url = f"http://127.0.0.1:{port}"

    seed = bytes(range(32))
    public = Ed25519PrivateKey.from_private_bytes(seed).public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    monkeypatch.setenv("DMARKET_PUBLIC_KEY", public.hex())
    monkeypatch.setenv("DMARKET_SECRET_KEY", (seed + public).hex())
    monkeypatch.setattr(build_baselines, "DMarketClient", functools.partial(DMarketClient, base_url=base_url))
    fake.base_url = base_url
    yield fake
    await runner.cleanup()


async def stored_sales(engine) -> list[Any]:
    async with engine.connect() as conn:
        return list((await conn.execute(select(VenueSale).order_by(col(VenueSale.sold_at)))).all())


def test_base_titles_drop_the_phase_and_duplicates():
    assert base_titles(SKINPORT_NAMES) == sorted([REDLINE, DOPPLER, CASE])


@pytest.mark.asyncio
async def test_titles_come_from_the_newest_skinport_build(engine):
    assert await skinport_titles(engine) == sorted([REDLINE, DOPPLER, CASE])


@pytest.mark.asyncio
async def test_fetch_reads_past_the_boundary_second(dmarket):
    import aiohttp

    async with aiohttp.ClientSession() as session:
        client = DMarketClient(DMarketKeys.from_env(), session, 1000, base_url=dmarket.base_url)
        sales, requests = await fetch_title_sales(client, REDLINE, NOW - timedelta(days=10, hours=1))

    # Days 0 to 10 at one Offer and one Target each, but the Target of day 10 is older than the cut.
    assert len(sales) == 21
    assert min(sale.sold_at for sale in sales) == NOW - timedelta(days=10, hours=1)
    assert requests == 2  # The second page holds the first sale older than the cut.


@pytest.mark.asyncio
async def test_sync_stores_each_sale_once_and_resumes_after_a_failed_title(engine, dmarket):
    import aiohttp

    dmarket.failing_titles = {DOPPLER}
    async with aiohttp.ClientSession() as session:
        client = DMarketClient(DMarketKeys.from_env(), session, 1000, base_url=dmarket.base_url)
        first = await sync_sales(client, engine, [CASE, DOPPLER, REDLINE], NOW)
        assert first.failed_titles == [DOPPLER]
        after_first = await stored_sales(engine)

        dmarket.failing_titles = set()
        dmarket.requests.clear()
        second = await sync_sales(client, engine, [CASE, DOPPLER, REDLINE], NOW)
        after_second = await stored_sales(engine)
        second_run_requests = list(dmarket.requests)
        third = await sync_sales(client, engine, [CASE, DOPPLER, REDLINE], NOW)

    # 90 days of Redline (45 days of sales, both kinds), and the six identical case sales of one second.
    # The case sale from 200 days ago is older than the history the builder keeps.
    assert sum(1 for row in after_first if row.title == REDLINE) == 90
    assert sum(1 for row in after_first if row.title == CASE) == 6
    assert not any(row.title == DOPPLER for row in after_first)
    assert second.failed_titles == []
    assert sum(1 for row in after_second if row.title == DOPPLER) == 24
    # The second run read Redline only back to its newest stored sale: one page.
    assert [offset for title, offset in second_run_requests if title == REDLINE] == [0]
    assert len(await stored_sales(engine)) == len(after_second) == 90 + 6 + 24
    assert third.failed_titles == []

    doppler_names = {row.market_hash_name for row in after_second if row.title == DOPPLER}
    assert doppler_names == {"★ Karambit | Doppler (Phase 2) (Factory New)", "★ Karambit | Doppler (Phase 4) (Factory New)"}


@pytest.mark.asyncio
async def test_histories_use_offer_sales_only(engine, dmarket):
    import aiohttp

    async with aiohttp.ClientSession() as session:
        client = DMarketClient(DMarketKeys.from_env(), session, 1000, base_url=dmarket.base_url)
        await sync_sales(client, engine, [REDLINE, CASE], NOW)

    histories = {history.market_hash_name: history async for history in offer_sale_histories(engine, NOW)}
    redline = histories[REDLINE]
    # One Offer sale a day at $25.00; the $23.00 buy order fills are left out.
    assert (redline.last_30_days.volume, redline.last_30_days.median_cents, redline.last_30_days.min_cents) == (30, 2500, 2500)
    assert redline.last_90_days.volume == 45
    assert redline.last_24_hours.volume == 1
    assert histories[CASE].last_24_hours.volume == 6


def _baseline(name: str, latest_price_cents: int) -> ItemBaseline:
    return ItemBaseline(
        market_hash_name=name,
        latest_price_cents=latest_price_cents,
        rolling_30d_avg_cents=latest_price_cents,
        rolling_90d_avg_cents=latest_price_cents,
        volatility_cents=1,
        support_floor_cents=latest_price_cents,
        avg_volume_30d=1.0,
        drift_percent=0.0,
        volatility_method="sales_spread",
        median_24h_cents=None,
        volume_24h=0,
        median_7d_cents=None,
        volume_7d=0,
        min_30d_cents=latest_price_cents,
        volume_30d=30,
        volume_90d=90,
    )


def test_fees_are_looked_up_by_title_without_the_phase():
    fees = FeeSchedule(default_fee_bps=1000, reduced={DOPPLER: ReducedFee(200, 200_000), REDLINE: ReducedFee(200, 1000)})
    priced = with_dmarket_fees(
        [
            _baseline("★ Karambit | Doppler (Phase 2) (Factory New)", 150_000),
            _baseline(REDLINE, 2_500),  # above the reduced fee's cap
            _baseline(CASE, 18),
        ],
        fees,
    )
    assert [baseline.seller_fee_bps for baseline in priced] == [200, 1000, 1000]


@pytest.mark.asyncio
async def test_flow_syncs_builds_with_fees_and_skips_when_fresh(engine, dmarket, tmp_path):
    heartbeat = tmp_path / "heartbeat"
    build_id = await build_dmarket_baselines(requests_per_second=1000, heartbeat_path=heartbeat)
    skipped = await build_dmarket_baselines(requests_per_second=1000)

    assert build_id is not None
    assert skipped is None
    assert await latest_build_time("dmarket") == NOW
    async with engine.connect() as conn:
        build = (await conn.execute(select(BaselineBuild).where(BaselineBuild.id == build_id))).one()
        rows = {
            row.market_hash_name: row
            for row in (await conn.execute(select(VenueBaseline).where(VenueBaseline.build_id == build_id)))
        }
    assert (build.venue, build.method) == ("dmarket", VENUE_SALES_METHOD)
    # The case sold 6 times in 30 days, each Doppler phase 12 times, the Redline 30 times.
    assert set(rows) == {
        REDLINE,
        CASE,
        "★ Karambit | Doppler (Phase 2) (Factory New)",
        "★ Karambit | Doppler (Phase 4) (Factory New)",
    }
    assert rows[REDLINE].latest_price_cents == 2500
    assert rows[REDLINE].seller_fee_bps == 200
    assert rows[CASE].seller_fee_bps == 1000
    assert rows["★ Karambit | Doppler (Phase 2) (Factory New)"].seller_fee_bps == 1000


@pytest.mark.asyncio
async def test_flow_refuses_to_build_when_most_titles_fail(engine, dmarket):
    dmarket.failing_titles = {REDLINE, DOPPLER}
    with pytest.raises(RuntimeError, match="failed to sync"):
        await build_dmarket_baselines(requests_per_second=1000)
    assert await latest_build_time("dmarket") is None


@pytest.mark.asyncio
async def test_flow_needs_a_skinport_build_for_its_titles(engine, dmarket):
    async with engine.begin() as conn:
        await conn.execute(VenueBaseline.__table__.delete())
    with pytest.raises(RuntimeError, match="Skinport"):
        await build_dmarket_baselines(requests_per_second=1000)


@pytest.mark.asyncio
async def test_flow_refuses_an_empty_build(engine, dmarket):
    dmarket.sales = {}
    with pytest.raises(RuntimeError, match="enough sales"):
        await build_dmarket_baselines(requests_per_second=1000)


@pytest.mark.asyncio
async def test_saved_rows_keep_the_seller_fee(engine):
    await save_build("dmarket", NOW, [_baseline(REDLINE, 2500).with_seller_fee(200)], VENUE_SALES_METHOD)
    async with engine.connect() as conn:
        fee = (await conn.execute(select(func.max(VenueBaseline.seller_fee_bps)))).scalar()
    assert fee == 200


def test_heartbeat_path_per_venue():
    assert build_baselines.heartbeat_path_for("dmarket") == build_baselines.DMARKET_HEARTBEAT_PATH
    assert build_baselines.heartbeat_path_for("skinport") == build_baselines.HEARTBEAT_PATH


def test_module_constants_fit_the_rate_limit():
    # The listener takes up to 10 of the account's 30 requests a second (docs/dmarket_api.md).
    assert build_baselines.DMARKET_REQUESTS_PER_SECOND <= 10
    assert dmarket_sales.HISTORY_DAYS == 90
