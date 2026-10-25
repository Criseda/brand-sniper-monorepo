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

from outcome_labels import FeedSale, FinalLabels, LabelConfig, LabelVersionMismatchError, contract_differences, final_before
from shared_utils import utc_now_naive
from shared_utils.db_connection import async_engine
from shared_utils.models import LabelVersion, ListingOutcome
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert

_UPSERT_CHUNK_SIZE = 1000

# One row per listing-level tick of one venue and event type, by edge receive time. Rows written before
# observed_at existed carry NULL there and are not read (backfill_observed_at.py fills Skinport's). Served by
# ix_live_market_ticks_listing_observed, whose predicate the two IS NOT NULL conditions repeat.
_LISTING_EVENTS_QUERY = """
SELECT tick.observed_at, tick.listing_id, item.market_hash_name, tick.price_cents
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


async def fetch_listing_events(
    venue: str, event_type: str, start: datetime, end: datetime, names: Sequence[str] | None = None
) -> list[FeedSale]:
    """Listing events of one type seen on `venue` in [start, end), optionally limited to versioned item names."""
    query = _LISTING_EVENTS_QUERY + (_NAME_FILTER if names is not None else "") + "ORDER BY tick.observed_at, tick.id"
    params: dict[str, Any] = {"venue": venue, "event_type": event_type, "start": start, "end": end}
    if names is not None:
        params["names"] = list(names)
    async with async_engine.connect() as conn:
        result = await conn.execute(text(query), params)
        rows = result.fetchall()
    return [
        FeedSale(listing_id=listing_id, market_hash_name=market_hash_name, price_cents=price_cents, seen_at=observed_at)
        for observed_at, listing_id, market_hash_name, price_cents in rows
    ]


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
