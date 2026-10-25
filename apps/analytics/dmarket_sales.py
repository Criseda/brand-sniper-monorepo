"""
DMarket sales sync: copies each title's sales from DMarket's `last-sales` into `venue_sales` (#339).

DMarket baselines and labels are computed from the stored sales (docs/dmarket_api.md). A title is read
newest first, back to the newest sale already stored for it (or `HISTORY_DAYS` on the first run), and its
sales are stored as soon as the title is done, with ON CONFLICT DO NOTHING. So a sync that stops halfway
carries on where it stopped, and running it twice stores nothing twice.

The titles are the ones in the newest Skinport build, without the Doppler phase: DMarket's `last-sales`
takes the Steam market hash name and gives the phase on each sale.
"""

import asyncio
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import aiohttp
from shared_utils import SalesHistory, get_logger
from shared_utils.dmarket import LAST_SALES_PAGE_SIZE, SALE_TYPE_OFFER, DMarketAPIError, DMarketClient, DMarketSale, sale_keys
from shared_utils.item_classifier import parse_version_from_name
from shared_utils.models import BaselineBuild, VenueBaseline, VenueSale
from sqlalchemy import func, select
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlmodel import col

logger = get_logger("analytics.dmarket_sales")

VENUE = "dmarket"
# The first sync reads this far back: the 90 day window is the longest a baseline uses.
HISTORY_DAYS = 90
# Titles read at the same time. The client's pacing keeps the total under its share of the rate limit.
CONCURRENT_TITLES = 4
# A guard against a title that never ends; 90 days of the busiest titles is a few hundred pages.
MAX_PAGES_PER_TITLE = 1000
_INSERT_CHUNK_SIZE = 1000


def base_titles(names: Iterable[str]) -> list[str]:
    """DMarket titles for versioned item names: the phase removed, duplicates dropped, sorted."""
    return sorted({parse_version_from_name(name)[0] for name in names})


async def skinport_titles(engine: AsyncEngine) -> list[str]:
    """The titles of every item in the newest Skinport build."""
    newest_build = (
        select(col(BaselineBuild.id))
        .where(col(BaselineBuild.venue) == "skinport")
        .order_by(col(BaselineBuild.id).desc())
        .limit(1)
        .scalar_subquery()
    )
    stmt = select(col(VenueBaseline.market_hash_name)).where(col(VenueBaseline.build_id) == newest_build)
    async with engine.connect() as conn:
        result = await conn.execute(stmt)
        return base_titles(name for (name,) in result)


async def newest_sale_times(engine: AsyncEngine) -> dict[str, datetime]:
    """The newest stored DMarket sale of each title."""
    stmt = (
        select(col(VenueSale.title), func.max(col(VenueSale.sold_at)))
        .where(col(VenueSale.venue) == VENUE)
        .group_by(col(VenueSale.title))
    )
    async with engine.connect() as conn:
        result = await conn.execute(stmt)
        return {title: newest for title, newest in result}


async def fetch_title_sales(client: DMarketClient, title: str, read_back_to: datetime) -> tuple[list[DMarketSale], int]:
    """
    A title's sales from `read_back_to` (inclusive) to now, and the number of requests it took.

    Paging goes on until a page holds a sale older than `read_back_to`, so every sale of the second at
    `read_back_to` is read together and `sale_keys` numbers identical sales the same way as last time.
    """
    sales: list[DMarketSale] = []
    offset = 0
    requests = 0
    while requests < MAX_PAGES_PER_TITLE:
        page, entries = await client.last_sales(title, offset)
        requests += 1
        sales.extend(sale for sale in page if sale.sold_at >= read_back_to)
        if entries < LAST_SALES_PAGE_SIZE or any(sale.sold_at < read_back_to for sale in page):
            break
        offset += LAST_SALES_PAGE_SIZE
    else:
        logger.warning("[DMARKET] %s: stopped after %d pages without reaching %s.", title, requests, read_back_to)
    return sales, requests


def sale_rows(sales: Sequence[DMarketSale], fetched_at: datetime) -> list[dict]:
    rows = []
    for sale, key in zip(sales, sale_keys(sales), strict=True):
        rows.append(
            {
                "venue": VENUE,
                "title": sale.title,
                "market_hash_name": sale.market_hash_name,
                "sold_at": sale.sold_at,
                "price_cents": sale.price_cents,
                "sale_type": sale.sale_type,
                "float_value": sale.float_value,
                "paint_seed": sale.paint_seed,
                "sale_key": key,
                "fetched_at": fetched_at,
            }
        )
    return rows


async def store_sales(engine: AsyncEngine, sales: Sequence[DMarketSale], fetched_at: datetime) -> None:
    """Insert the sales, skipping the ones already stored."""
    rows = sale_rows(sales, fetched_at)
    if not rows:
        return
    async with engine.begin() as conn:
        dialect = postgresql if conn.dialect.name == "postgresql" else sqlite
        for start in range(0, len(rows), _INSERT_CHUNK_SIZE):
            stmt = dialect.insert(VenueSale).values(rows[start : start + _INSERT_CHUNK_SIZE])
            await conn.execute(stmt.on_conflict_do_nothing(index_elements=["venue", "title", "sale_key"]))


@dataclass
class SyncSummary:
    titles: int = 0
    requests: int = 0
    sales_read: int = 0
    failed_titles: list[str] = field(default_factory=list)


async def sync_sales(
    client: DMarketClient,
    engine: AsyncEngine,
    titles: Sequence[str],
    now: datetime,
    on_progress: Callable[[int], None] | None = None,
) -> SyncSummary:
    """
    Read every title's new sales into `venue_sales`. A title that fails is logged and left for the next
    run; the others carry on. `on_progress` gets the number of titles done after each title.
    """
    newest = await newest_sale_times(engine)
    oldest_wanted = now - timedelta(days=HISTORY_DAYS)
    summary = SyncSummary(titles=len(titles))
    semaphore = asyncio.Semaphore(CONCURRENT_TITLES)
    # Titles are read concurrently but stored one at a time: each write is small, and it keeps the
    # transactions of different titles apart on a single connection.
    write_lock = asyncio.Lock()
    done = 0

    async def sync_one(title: str) -> None:
        nonlocal done
        async with semaphore:
            read_back_to = max(newest.get(title, oldest_wanted), oldest_wanted)
            try:
                sales, requests = await fetch_title_sales(client, title, read_back_to)
                async with write_lock:
                    await store_sales(engine, sales, now)
            except (DMarketAPIError, aiohttp.ClientError, TimeoutError) as error:
                logger.warning("[DMARKET] %s: sales not synced (%s); retrying next run.", title, error)
                summary.failed_titles.append(title)
                return
            summary.requests += requests
            summary.sales_read += len(sales)
            done += 1
            if on_progress is not None:
                on_progress(done)

    await asyncio.gather(*(sync_one(title) for title in titles))
    return summary


async def offer_sale_histories(engine: AsyncEngine, now: datetime) -> AsyncIterator[SalesHistory]:
    """
    Each item's sales history from its stored `offer` sales of the last `HISTORY_DAYS` days.

    Buy order fills are left out: they sell about 7% below listings (docs/dmarket_api.md), and the DRE
    compares a listing with what listings sell for. Rows are streamed one item at a time.
    """
    stmt = (
        select(col(VenueSale.market_hash_name), col(VenueSale.sold_at), col(VenueSale.price_cents))
        .where(col(VenueSale.venue) == VENUE)
        .where(col(VenueSale.sale_type) == SALE_TYPE_OFFER)
        .where(col(VenueSale.sold_at) > now - timedelta(days=HISTORY_DAYS))
        .order_by(col(VenueSale.market_hash_name))
    )
    current_name: str | None = None
    current_sales: list[tuple[datetime, int]] = []
    async with engine.connect() as conn:
        result = await conn.stream(stmt)
        async for name, sold_at, price_cents in result:
            if name != current_name:
                if current_name is not None:
                    yield SalesHistory.from_sales(current_name, current_sales, now)
                current_name, current_sales = name, []
            current_sales.append((sold_at, price_cents))
    if current_name is not None:
        yield SalesHistory.from_sales(current_name, current_sales, now)
