"""
Market-outcome labeler (triple-barrier style) for recorded Skinport listings.

For every `listed` sale recorded in `feed_events`, asks what the market actually did afterwards:
- What did comparable items (same versioned name) sell for once the trade hold was over, and would
  reselling at that price have cleared the seller fee and the minimum margin?
- Was this exact listing (same productId) seen to sell, and how fast?

Labels are written to `listing_outcomes` under a label version. A listing is only labeled once its
horizon has passed, so a label never contains information from after `label_available_at`.
The flow is idempotent: re-running it for any date range rewrites the same rows.

Label definition: docs/roadmap_proven_edge.md, section 4.3.

Usage (from apps/analytics):
    uv run python label_outcomes.py --start 2026-09-24 --end 2026-09-26
"""

import asyncio
import bisect
import statistics
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from shared_utils import setup_script_environment
from sqlalchemy import text

setup_script_environment(__file__)

from prefect import flow, get_run_logger, task
from shared_utils import (
    SKINPORT_FEES,
    VenueFees,
    build_versioned_name,
    is_profitable_margin,
    net_resale_margin_cents,
    utc_now_naive,
    validate_required_env,
)
from shared_utils.db_connection import async_engine
from shared_utils.models import ListingOutcome
from sqlalchemy.dialects.postgresql import insert

SOURCE = "skinport"
LABEL_VERSION = "v1"
_UPSERT_CHUNK_SIZE = 1000


@dataclass(frozen=True)
class LabelConfig:
    """Parameters of one label version. Changing any of them requires a new `version`."""

    version: str = LABEL_VERSION
    fees: VenueFees = SKINPORT_FEES
    # Resale window is [listed_at + fees.hold_seconds, listed_at + horizon_seconds].
    horizon_seconds: int = 14 * 86_400
    # Fewer comparable sales than this in the resale window: neutral (insufficient evidence).
    min_comparable_sales: int = 3
    # Extra wait after the horizon before labeling, so batches still in flight from the edge
    # (listener buffer, Redis stream retries) have landed in feed_events.
    settle_seconds: int = 3600

    def __post_init__(self) -> None:
        if self.horizon_seconds <= self.fees.hold_seconds:
            raise ValueError("horizon_seconds must be longer than the trade hold")
        if self.min_comparable_sales < 1:
            raise ValueError("min_comparable_sales must be at least 1")
        if self.settle_seconds < 0:
            raise ValueError("settle_seconds must not be negative")


@dataclass(frozen=True)
class FeedSale:
    """One sale entry from a recorded saleFeed event, reduced to what labeling needs."""

    listing_id: str
    market_hash_name: str  # Versioned name (phase included)
    feed_name: str  # marketHashName exactly as the venue sent it (no phase), used to filter queries
    price_cents: int
    seen_at: datetime  # Edge receive time of the event that carried it (naive UTC)


@dataclass
class SoldIndex:
    """Sold sales, looked up by item name (sorted by time) and by listing ID (first sighting)."""

    times_by_name: dict[str, list[datetime]] = field(default_factory=dict)
    sales_by_name: dict[str, list[FeedSale]] = field(default_factory=dict)
    first_sold_by_listing: dict[str, FeedSale] = field(default_factory=dict)

    @classmethod
    def build(cls, sold_sales: Iterable[FeedSale]) -> "SoldIndex":
        # One listing can only be sold once, so a repeated productId is the same sale seen twice.
        unique_sales = first_sighting_per_listing(sold_sales)
        index = cls(first_sold_by_listing=unique_sales)
        for sale in sorted(unique_sales.values(), key=lambda s: (s.seen_at, s.listing_id)):
            index.times_by_name.setdefault(sale.market_hash_name, []).append(sale.seen_at)
            index.sales_by_name.setdefault(sale.market_hash_name, []).append(sale)
        return index

    def comparable_prices(self, name: str, window_start: datetime, window_end: datetime, exclude_listing: str) -> list[int]:
        """Prices of sales of `name` inside [window_start, window_end], excluding one listing."""
        times = self.times_by_name.get(name, [])
        sales = self.sales_by_name.get(name, [])
        first = bisect.bisect_left(times, window_start)
        last = bisect.bisect_right(times, window_end)
        return [sale.price_cents for sale in sales[first:last] if sale.listing_id != exclude_listing]


def first_sighting_per_listing(sales: Iterable[FeedSale]) -> dict[str, FeedSale]:
    """Keeps the earliest sighting of each listing ID (ties broken by the lower price)."""
    first: dict[str, FeedSale] = {}
    for sale in sales:
        current = first.get(sale.listing_id)
        if current is None or (sale.seen_at, sale.price_cents) < (current.seen_at, current.price_cents):
            first[sale.listing_id] = sale
    return first


def label_listing(listing: FeedSale, sold: SoldIndex, config: LabelConfig) -> dict[str, Any]:
    """Builds the `listing_outcomes` row for one listing. Uses no sale after listed_at + horizon."""
    listed_at = listing.seen_at
    label_available_at = listed_at + timedelta(seconds=config.horizon_seconds)
    resale_window_start = listed_at + timedelta(seconds=config.fees.hold_seconds)

    comparable = sold.comparable_prices(listing.market_hash_name, resale_window_start, label_available_at, listing.listing_id)
    resale_price_cents: int | None = None
    net_margin_cents: int | None = None
    is_profitable: bool | None = None
    if len(comparable) >= config.min_comparable_sales:
        # Lower median: the price a resale at the typical market level would have fetched. The
        # single best sale in the window would reward outliers that a seller could not count on.
        resale_price_cents = statistics.median_low(comparable)
        net_margin_cents = net_resale_margin_cents(listing.price_cents, resale_price_cents, config.fees)
        is_profitable = is_profitable_margin(net_margin_cents, config.fees)

    # A sale of this listing counts only inside [listed_at, label_available_at]. No sold event means
    # "not seen to sell" (the feed never reports cancellations or price changes): censored.
    listing_sold_within_s: int | None = None
    own_sale = sold.first_sold_by_listing.get(listing.listing_id)
    if own_sale is not None and listed_at <= own_sale.seen_at <= label_available_at:
        listing_sold_within_s = int((own_sale.seen_at - listed_at).total_seconds())

    return {
        "source": SOURCE,
        "listing_id": listing.listing_id,
        "label_version": config.version,
        "market_hash_name": listing.market_hash_name,
        "listed_at": listed_at,
        "listed_price_cents": listing.price_cents,
        "comparable_sales": len(comparable),
        "resale_price_cents": resale_price_cents,
        "resale_net_margin_cents": net_margin_cents,
        "is_profitable": is_profitable,
        "listing_sold_within_s": listing_sold_within_s,
        "sale_censored": listing_sold_within_s is None,
        "label_available_at": label_available_at,
    }


def label_listings(
    listed_sales: Iterable[FeedSale], sold_sales: Iterable[FeedSale], config: LabelConfig, as_of: datetime
) -> list[dict[str, Any]]:
    """Labels every listing whose horizon (plus settle time) has passed by `as_of`."""
    latest_labelable = as_of - timedelta(seconds=config.horizon_seconds + config.settle_seconds)
    sold = SoldIndex.build(sold_sales)
    listings = first_sighting_per_listing(listed_sales)
    rows = [label_listing(listing, sold, config) for listing in listings.values() if listing.seen_at <= latest_labelable]
    return sorted(rows, key=lambda row: (row["listed_at"], row["listing_id"]))


def parse_sale_row(row: Any) -> FeedSale | None:
    """Turns one (received_at, productId, marketHashName, version, salePrice) row into a FeedSale."""
    received_at, product_id, market_hash_name, version, sale_price = row
    if not product_id or not market_hash_name:
        return None
    try:
        price_cents = int(sale_price)
    except (TypeError, ValueError):
        return None
    if price_cents <= 0:
        return None
    return FeedSale(
        listing_id=str(product_id),
        market_hash_name=build_versioned_name(market_hash_name, version),
        feed_name=market_hash_name,
        price_cents=price_cents,
        seen_at=received_at,
    )


# One row per sale in the saleFeed events of one type and time range. Non-array `sales` are treated as
# empty so one malformed payload cannot fail the query. Only USD prices are comparable (the listener
# subscribes in USD).
_SALES_QUERY = """
SELECT fe.received_at,
       sale->>'productId',
       sale->>'marketHashName',
       sale->>'version',
       sale->>'salePrice'
FROM feed_events AS fe
CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(fe.payload->'sales') = 'array' THEN fe.payload->'sales' ELSE '[]'::jsonb END
) AS sale
WHERE fe.source = :source
  AND fe.event_type = :event_type
  AND fe.received_at >= :start
  AND fe.received_at < :end
  AND COALESCE(sale->>'currency', 'USD') = 'USD'
"""
_NAME_FILTER = "  AND sale->>'marketHashName' = ANY(:names)\n"


async def fetch_feed_sales(event_type: str, start: datetime, end: datetime, names: list[str] | None = None) -> list[FeedSale]:
    """Sales of one feed event type received in [start, end), optionally limited to raw item names."""
    query = _SALES_QUERY + (_NAME_FILTER if names is not None else "") + "ORDER BY fe.received_at, fe.id"
    params: dict[str, Any] = {"source": SOURCE, "event_type": event_type, "start": start, "end": end}
    if names is not None:
        params["names"] = names
    async with async_engine.connect() as conn:
        result = await conn.execute(text(query), params)
        rows = result.fetchall()
    sales = [parse_sale_row(row) for row in rows]
    return [sale for sale in sales if sale is not None]


async def save_outcomes(rows: list[dict[str, Any]]) -> None:
    """Upserts labels. On conflict the earliest sighting of a listing wins, so runs over overlapping
    or out-of-order date ranges converge on the same row."""
    table = ListingOutcome.__table__  # type: ignore[attr-defined]
    updatable = [name for name in rows[0] if name not in ("source", "listing_id", "label_version")] if rows else []
    async with async_engine.begin() as conn:
        for chunk_start in range(0, len(rows), _UPSERT_CHUNK_SIZE):
            chunk = rows[chunk_start : chunk_start + _UPSERT_CHUNK_SIZE]
            stmt = insert(table).values(chunk)
            set_: dict[str, Any] = {name: stmt.excluded[name] for name in updatable}
            set_["labeled_at"] = utc_now_naive()
            stmt = stmt.on_conflict_do_update(
                index_elements=["source", "listing_id", "label_version"],
                set_=set_,
                where=table.c.listed_at >= stmt.excluded.listed_at,
            )
            await conn.execute(stmt)


@task(retries=2, retry_delay_seconds=30)
async def label_window(start: datetime, end: datetime, as_of: datetime, config: LabelConfig) -> int:
    """Labels listings first seen in [start, end). Returns the number of labels written."""
    logger = get_run_logger()
    listed = await fetch_feed_sales("listed", start, end)
    if not listed:
        logger.info("[LABELER] No listings in %s .. %s.", start, end)
        return 0

    # Sales that can affect these labels: the listing's own sale from listed_at, comparable sales
    # up to the horizon. Nothing later is fetched, which is the look-ahead guard at the query level.
    feed_names = sorted({sale.feed_name for sale in listed})
    sold = await fetch_feed_sales("sold", start, end + timedelta(seconds=config.horizon_seconds), names=feed_names)

    rows = label_listings(listed, sold, config, as_of)
    if rows:
        await save_outcomes(rows)
    profitable = sum(1 for row in rows if row["is_profitable"] is True)
    neutral = sum(1 for row in rows if row["is_profitable"] is None)
    censored = sum(1 for row in rows if row["sale_censored"])
    logger.info(
        "[LABELER] %s .. %s: %d labels (%d profitable, %d neutral, %d censored) from %d listing sightings, %d sold sales.",
        start,
        end,
        len(rows),
        profitable,
        neutral,
        censored,
        len(listed),
        len(sold),
    )
    return len(rows)


def iter_windows(start: datetime, end: datetime, window: timedelta) -> list[tuple[datetime, datetime]]:
    """Splits [start, end) into consecutive windows of at most `window`."""
    windows = []
    cursor = start
    while cursor < end:
        window_end = min(cursor + window, end)
        windows.append((cursor, window_end))
        cursor = window_end
    return windows


@flow(name="listing-outcome-labeler")
async def label_listing_outcomes(
    start: datetime,
    end: datetime,
    as_of: datetime | None = None,
    window_hours: int = 24,
    config: LabelConfig | None = None,
) -> int:
    """Labels every listing first seen in [start, end) whose horizon has passed. Safe to re-run."""
    logger = get_run_logger()
    config = config or LabelConfig()
    as_of = as_of or utc_now_naive()
    if window_hours < 1:
        raise ValueError("window_hours must be at least 1")
    # Nothing listed after this point can be labeled yet; do not scan it.
    end = min(end, as_of - timedelta(seconds=config.horizon_seconds + config.settle_seconds))
    if end <= start:
        logger.info("[LABELER] Nothing to label yet: horizon has not passed for any listing after %s.", start)
        return 0

    total = 0
    for window_start, window_end in iter_windows(start, end, timedelta(hours=window_hours)):
        total += await label_window(window_start, window_end, as_of, config)
    logger.info("[LABELER] Wrote %d labels (version %s) for %s .. %s.", total, config.version, start, end)
    return total


def default_start(as_of: datetime, days_back: int, config: LabelConfig) -> datetime:
    """Start of the most recent `days_back` days of listings whose horizon has passed by `as_of`.

    Re-labeling a few matured days on each daily run picks up batches that reached the database late."""
    return as_of - timedelta(seconds=config.horizon_seconds + config.settle_seconds) - timedelta(days=days_back)


if __name__ == "__main__":  # pragma: no cover - entrypoint glue, covered via unit tests
    import argparse

    parser = argparse.ArgumentParser(description="Label recorded listings with their market outcome.")
    parser.add_argument("--start", type=datetime.fromisoformat, help="First listing time (UTC), e.g. 2026-09-24")
    parser.add_argument("--end", type=datetime.fromisoformat, help="End of the listing range, exclusive (default: now)")
    parser.add_argument(
        "--days-back",
        type=int,
        default=3,
        help="Without --start: label the last N days of listings whose horizon has passed (default: 3)",
    )
    parser.add_argument("--window-hours", type=int, default=24, help="Listing hours per labeling task (default: 24)")
    args = parser.parse_args()

    validate_required_env(["DATABASE_URL"])
    now = utc_now_naive()
    start = args.start or default_start(now, args.days_back, LabelConfig())
    asyncio.run(label_listing_outcomes(start=start, end=args.end or now, as_of=now, window_hours=args.window_hours))
