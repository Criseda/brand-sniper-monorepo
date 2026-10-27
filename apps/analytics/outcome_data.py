"""
Database access for outcome labels: listing events in, labels and label versions out.

Listing events come from the normalized listing-level rows of `live_market_ticks` (#256), keyed by venue,
so labeling another venue needs no parser here. `feed_events` stays the verbatim archive the listener's
parsers can be rerun on; nothing here reads its venue specific JSON.

The labeling rule itself is in outcome_labels.py.
"""

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from outcome_labels import (
    FeedSale,
    FinalLabels,
    ItemFees,
    LabelConfig,
    LabelVersionMismatchError,
    contract_differences,
    final_before,
)
from shared_utils import utc_now_naive
from shared_utils.db_connection import async_engine
from shared_utils.models import BaselineBuild, LabelVersion, ListingOutcome
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import col

_UPSERT_CHUNK_SIZE = 1000

# One row per listing-level tick of one venue and event type, by edge receive time. Rows written before
# observed_at existed carry NULL there and are not read (backfill_observed_at.py fills Skinport's). Served by
# ix_live_market_ticks_listing_observed, whose predicate the two IS NOT NULL conditions repeat.
_LISTING_EVENTS_QUERY = """
SELECT tick.observed_at, tick.listing_id, item.market_hash_name, tick.price_cents, tick.float_value, tick.pattern
FROM live_market_ticks AS tick
JOIN market_items AS item ON item.id = tick.item_id
WHERE tick.venue = :venue
  AND tick.event_type = :event_type
  AND tick.listing_id IS NOT NULL
  AND tick.observed_at IS NOT NULL
  AND tick.observed_at >= :start
  AND tick.observed_at < :end
"""
_NAME_FILTER = "  AND item.market_hash_name = ANY(:names)\n"


# When the same item (same name, float and paint seed) was next recorded as another listing, for a venue whose
# sales name no listing (`own_sale_by_item`). Served by the index on float_value, which is nearly unique.
_NEXT_LISTING_COLUMN = """,
       (SELECT MIN(later.observed_at)
        FROM live_market_ticks AS later
        WHERE later.float_value = tick.float_value
          AND later.item_id = tick.item_id
          AND later.pattern IS NOT DISTINCT FROM tick.pattern
          AND later.venue = tick.venue
          AND later.event_type = tick.event_type
          AND later.listing_id <> tick.listing_id
          AND later.observed_at > tick.observed_at) AS next_listed_at"""
_LISTING_COLUMNS_END = "tick.float_value, tick.pattern"


async def fetch_listing_events(
    venue: str,
    event_type: str,
    start: datetime,
    end: datetime,
    names: Sequence[str] | None = None,
    with_next_listing: bool = False,
) -> list[FeedSale]:
    """Listing events of one type seen on `venue` in [start, end), optionally limited to versioned item names.

    `with_next_listing` also reads when each listing's item was next recorded as another listing."""
    query = _LISTING_EVENTS_QUERY
    if with_next_listing:
        query = query.replace(_LISTING_COLUMNS_END, _LISTING_COLUMNS_END + _NEXT_LISTING_COLUMN, 1)
    query += (_NAME_FILTER if names is not None else "") + "ORDER BY tick.observed_at, tick.id"
    params: dict[str, Any] = {"venue": venue, "event_type": event_type, "start": start, "end": end}
    if names is not None:
        params["names"] = list(names)
    async with async_engine.connect() as conn:
        result = await conn.execute(text(query), params)
        rows = result.fetchall()
    return [
        FeedSale(
            listing_id=row[1],
            market_hash_name=row[2],
            price_cents=row[3],
            seen_at=row[0],
            float_value=row[4],
            pattern=row[5],
            next_listed_at=row[6] if with_next_listing else None,
        )
        for row in rows
    ]


# A venue's own sales of listings (#339): buy order fills (`target`) sell lower and are not what a listing
# resells for. A sale names no listing, so each gets an ID of its own that never matches a listing's; its float and
# paint seed tell which item sold (`own_sale_by_item`).
# Served by ix_venue_sales_venue_name_sold_at.
_VENUE_SALES_QUERY = """
SELECT sold_at, id, market_hash_name, price_cents, float_value, paint_seed
FROM venue_sales
WHERE venue = :venue AND sale_type = 'offer' AND sold_at >= :start AND sold_at < :end
ORDER BY sold_at, id
"""


async def fetch_venue_sales(venue: str, start: datetime, end: datetime) -> list[FeedSale]:
    """The listing purchases in `venue`'s own sales history in [start, end), as sold events."""
    async with async_engine.connect() as conn:
        result = await conn.execute(text(_VENUE_SALES_QUERY), {"venue": venue, "start": start, "end": end})
        rows = result.fetchall()
    return [
        FeedSale(
            listing_id=f"sale-{sale_id}",
            market_hash_name=market_hash_name,
            price_cents=price_cents,
            seen_at=sold_at,
            float_value=float_value,
            pattern=paint_seed,
        )
        for sold_at, sale_id, market_hash_name, price_cents, float_value, paint_seed in rows
    ]


async def latest_sales_sync(venue: str) -> datetime | None:
    """When the newest completed sync of `venue`'s sales history started: its newest baseline build, which the
    builder stores only after reading every title's sales up to that time. None before the first build."""
    async with async_engine.connect() as conn:
        result = await conn.execute(select(func.max(col(BaselineBuild.built_at))).where(col(BaselineBuild.venue) == venue))
        return result.scalar()


# The builds whose fees apply to listings recorded in [start, end]: the one in force at `start` and every later one.
_ITEM_FEES_QUERY = """
SELECT build.built_at, baseline.market_hash_name, baseline.seller_fee_bps
FROM baseline_builds AS build
JOIN venue_baselines AS baseline ON baseline.build_id = build.id
WHERE build.venue = :venue
  AND baseline.seller_fee_bps IS NOT NULL
  AND build.built_at <= :end
  AND build.built_at >= COALESCE(
      (SELECT MAX(built_at) FROM baseline_builds WHERE venue = :venue AND built_at <= :start), :start
  )
"""


async def fetch_item_fees(venue: str, start: datetime, end: datetime) -> ItemFees:
    """Each item's seller fee in every `venue` build that was in force for listings recorded in [start, end]."""
    async with async_engine.connect() as conn:
        result = await conn.execute(text(_ITEM_FEES_QUERY), {"venue": venue, "start": start, "end": end})
        rows = result.fetchall()
    builds: dict[datetime, dict[str, int]] = {}
    for built_at, market_hash_name, seller_fee_bps in rows:
        builds.setdefault(built_at, {})[market_hash_name] = seller_fee_bps
    return ItemFees.build(builds.items())


async def register_label_version(config: LabelConfig) -> None:
    """Stores the config of a new label version, or checks it against the stored one.

    Raises LabelVersionMismatchError when the version exists with other parameters: writing would mix two
    label definitions under one name, which nothing could detect afterwards."""
    table = LabelVersion.__table__  # type: ignore[attr-defined]
    contract = config.contract()
    async with async_engine.begin() as conn:
        await conn.execute(
            insert(table).values(version=config.version, config=contract).on_conflict_do_nothing(index_elements=["version"])
        )
        result = await conn.execute(select(table.c.config).where(table.c.version == config.version))
        stored = result.fetchall()[0][0]
    differences = contract_differences(stored, contract)
    if differences:
        raise LabelVersionMismatchError(
            f"Label version {config.version} is stored with other parameters ({'; '.join(differences)}). "
            "Give the new parameters a new version."
        )


async def load_label_contract(version: str) -> dict[str, Any]:
    """The stored definition of a label version. Raises LookupError for a version never registered."""
    table = LabelVersion.__table__  # type: ignore[attr-defined]
    async with async_engine.connect() as conn:
        result = await conn.execute(select(table.c.config).where(table.c.version == version))
        rows = result.fetchall()
    if not rows:
        raise LookupError(f"Label version {version} is not registered in label_versions")
    return dict(rows[0][0])


# Final labels only, and only rows written by `as_of`: a final label is never rewritten, so the same
# `as_of` reads the same rows on every run. Served by ix_listing_outcomes_version_available.
_FINAL_LABELS_QUERY = """
SELECT listing_id, market_hash_name, listed_at, listed_price_cents, comparable_sales, resale_price_cents,
       resale_net_margin_cents, is_profitable, listing_sold_within_s, sale_censored, label_available_at
FROM listing_outcomes
WHERE venue = :venue AND label_version = :label_version
  AND label_available_at <= :final_before AND labeled_at <= :as_of
ORDER BY label_available_at, listing_id
"""


async def fetch_final_labels(version: str, as_of: datetime, include_revisable: bool = False) -> FinalLabels:
    """Every final label of `version` as of `as_of`, for training and evaluation.

    The venue and the finality rule come from the stored contract, not from this code's defaults.
    `include_revisable` also reads labels that a later run may still revise: only for exploratory runs, which
    are not reproducible."""
    contract = await load_label_contract(version)
    cutoff = as_of if include_revisable else final_before(contract, as_of)
    params = {"venue": contract["venue"], "label_version": version, "final_before": cutoff, "as_of": as_of}
    async with async_engine.connect() as conn:
        result = await conn.execute(text(_FINAL_LABELS_QUERY), params)
        rows = [dict(row._mapping) for row in result.fetchall()]
    return FinalLabels(version=version, contract=contract, as_of=as_of, rows=rows)


async def save_outcomes(rows: list[dict[str, Any]], final_cutoff: datetime) -> int:
    """Upserts labels and returns how many rows were written. On conflict the earliest sighting of a
    listing wins, so runs over overlapping or out-of-order date ranges converge on the same row. A stored
    label with `label_available_at` at or before `final_cutoff` is final and kept as it is."""
    table = ListingOutcome.__table__  # type: ignore[attr-defined]
    updatable = [name for name in rows[0] if name not in ("venue", "listing_id", "label_version")] if rows else []
    written = 0
    async with async_engine.begin() as conn:
        for chunk_start in range(0, len(rows), _UPSERT_CHUNK_SIZE):
            chunk = rows[chunk_start : chunk_start + _UPSERT_CHUNK_SIZE]
            stmt = insert(table).values(chunk)
            set_: dict[str, Any] = {name: stmt.excluded[name] for name in updatable}
            set_["labeled_at"] = utc_now_naive()
            stmt = stmt.on_conflict_do_update(
                index_elements=["venue", "listing_id", "label_version"],
                set_=set_,
                where=(table.c.listed_at >= stmt.excluded.listed_at) & (table.c.label_available_at > final_cutoff),
            )
            result = await conn.execute(stmt)
            written += max(int(result.rowcount or 0), 0)
    return written
