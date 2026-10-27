"""
Market-outcome labeler: the Prefect flow and the CLI.

Labels every recorded listing of one venue whose horizon has passed and writes the labels to
`listing_outcomes` under a label version. The flow is idempotent: re-running it for any date range rewrites
the same rows, and never a final one. The labeling rule is in outcome_labels.py, the database access in
outcome_data.py. Label definition: docs/roadmap_proven_edge.md, section 4.3.

Usage (from apps/analytics):
    uv run python label_outcomes.py --start 2026-09-24 --end 2026-09-26
    uv run python label_outcomes.py --venue dmarket --label-version v1-dmarket

DMarket's sales come from its own sales history (`venue_sales`), not from the feed, so its labels wait for the
baseline builder's sales sync and price each resale with the item's own seller fee (#339).
"""

import asyncio
from datetime import datetime, timedelta

from shared_utils import setup_script_environment

setup_script_environment(__file__)

from outcome_data import (
    fetch_item_fees,
    fetch_listing_events,
    fetch_venue_sales,
    latest_sales_sync,
    register_label_version,
    save_outcomes,
)
from outcome_labels import (
    LABEL_VENUE,
    LABEL_VERSION,
    SALES_FROM_VENUE_SALES,
    ItemFees,
    LabelConfig,
    SoldIndex,
    default_start,
    iter_windows,
    label_listings,
    latest_labelable,
)
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from shared_utils import utc_now_naive, validate_required_env


# No cache: the sold index is large, and hashing it for a cache key would cost more than the task itself.
@task(retries=2, retry_delay_seconds=30, cache_policy=NO_CACHE)
async def label_window(
    start: datetime,
    end: datetime,
    as_of: datetime,
    sold: SoldIndex,
    config: LabelConfig,
    item_fees: ItemFees | None = None,
    sales_synced_at: datetime | None = None,
) -> int:
    """Labels listings first seen in [start, end). Returns the number of labels written."""
    logger = get_run_logger()
    if config.own_sale_by_item:
        listed = await fetch_listing_events(config.venue, "listed", start, end, with_next_listing=True)
    else:
        listed = await fetch_listing_events(config.venue, "listed", start, end)
    if not listed:
        logger.info("[LABELER] No %s listings in %s .. %s.", config.venue, start, end)
        return 0

    rows = label_listings(listed, sold, config, as_of, item_fees, sales_synced_at)
    written = await save_outcomes(rows, config.final_before(as_of)) if rows else 0
    profitable = sum(1 for row in rows if row["is_profitable"] is True)
    neutral = sum(1 for row in rows if row["is_profitable"] is None)
    censored = sum(1 for row in rows if row["sale_censored"])
    logger.info(
        "[LABELER] %s .. %s: %d labels (%d profitable, %d neutral, %d censored) from %d listing sightings; "
        "%d written, the others final or an earlier sighting.",
        start,
        end,
        len(rows),
        profitable,
        neutral,
        censored,
        len(listed),
        written,
    )
    return len(rows)


@flow(name="listing-outcome-labeler")
async def label_listing_outcomes(
    start: datetime,
    end: datetime,
    as_of: datetime | None = None,
    window_hours: int = 24,
    config: LabelConfig | None = None,
) -> int:
    """Labels every listing of `config.venue` first seen in [start, end) whose horizon has passed. Safe to re-run."""
    logger = get_run_logger()
    config = config or LabelConfig()
    as_of = as_of or utc_now_naive()
    if window_hours < 1:
        raise ValueError("window_hours must be at least 1")
    sales_synced_at = await latest_sales_sync(config.venue) if config.sales_source == SALES_FROM_VENUE_SALES else None
    # Nothing listed after this point can be labeled yet; do not scan it.
    end = min(end, latest_labelable(as_of, config, sales_synced_at))
    if end <= start:
        logger.info(
            "[LABELER] Nothing to label yet: horizon has not passed for any listing after %s (newest sales sync: %s).",
            start,
            sales_synced_at,
        )
        return 0
    await register_label_version(config)

    # Sales that can affect these labels: a listing's own sale from listed_at, comparable sales up to the horizon.
    # Read once for the whole run; nothing later is fetched, which is the look-ahead guard at the query level.
    sold_end = end + timedelta(seconds=config.horizon_seconds)
    if config.sales_source == SALES_FROM_VENUE_SALES:
        sold_sales = await fetch_venue_sales(config.venue, start, sold_end)
    else:
        sold_sales = await fetch_listing_events(config.venue, "sold", start, sold_end)
    sold = SoldIndex.build(sold_sales)
    logger.info("[LABELER] %d %s sold events up to %s.", len(sold_sales), config.venue, end)
    item_fees = await fetch_item_fees(config.venue, start, end) if config.item_seller_fees else None

    total = 0
    for window_start, window_end in iter_windows(start, end, timedelta(hours=window_hours)):
        total += await label_window(window_start, window_end, as_of, sold, config, item_fees, sales_synced_at)
    logger.info("[LABELER] Wrote %d labels (version %s) for %s .. %s.", total, config.version, start, end)
    return total


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
    parser.add_argument("--venue", default=LABEL_VENUE, help=f"Venue to label (default: {LABEL_VENUE})")
    parser.add_argument(
        "--label-version",
        default=LABEL_VERSION,
        help=f"Label version to write; another venue needs its own (default: {LABEL_VERSION})",
    )
    args = parser.parse_args()

    validate_required_env(["DATABASE_URL"])
    label_config = LabelConfig.for_venue(args.venue, args.label_version)
    now = utc_now_naive()
    start = args.start
    if start is None:
        synced_at = None
        if label_config.sales_source == SALES_FROM_VENUE_SALES:
            synced_at = asyncio.run(latest_sales_sync(label_config.venue))
            if synced_at is None:
                raise SystemExit(f"No {label_config.venue} sales sync yet: run the baseline builder first.")
        start = default_start(now, args.days_back, label_config, synced_at)
    asyncio.run(
        label_listing_outcomes(start=start, end=args.end or now, as_of=now, window_hours=args.window_hours, config=label_config)
    )
