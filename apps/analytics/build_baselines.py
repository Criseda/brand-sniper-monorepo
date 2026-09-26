"""
Baseline builder: current Skinport baselines from Skinport's sales history.

One request to `/v1/sales/history` returns recent sale statistics for every item. Each run turns them
into one baseline per item (see shared_utils.baselines) and stores the result as a new, dated build in
`baseline_builds` and `venue_baselines`. Builds are never overwritten. The backend serves the newest
build and the listener loads it into the edge Redis.

The edge PC is not always on, so nothing here depends on running at a fixed time. A run only builds
when the newest build is older than `--max-age-hours`, and `--loop` checks again every interval, so a
stack that starts after a day off catches up straight away.

Usage (from apps/analytics):
    uv run python build_baselines.py            # build if the newest build is older than 20 hours
    uv run python build_baselines.py --force    # build now
    uv run python build_baselines.py --loop     # keep checking every hour (the baseline-builder service)
"""

import asyncio
from collections.abc import Iterable, Mapping
from datetime import date, datetime, timedelta
from typing import Any

import aiohttp
from shared_utils import setup_script_environment
from sqlalchemy import func, insert, select
from sqlmodel import col

setup_script_environment(__file__)

from prefect import flow, get_run_logger, task
from shared_utils import (
    BASELINE_METHOD,
    ItemBaseline,
    SalesHistory,
    build_item_baseline,
    get_logger,
    utc_now_naive,
    validate_required_env,
)
from shared_utils.db_connection import async_engine
from shared_utils.models import BaselineBuild, VenueBaseline

logger = get_logger("analytics.build_baselines")

VENUE = "skinport"
SALES_HISTORY_URL = "https://api.skinport.com/v1/sales/history"
DEFAULT_MAX_AGE_HOURS = 20.0
DEFAULT_CHECK_INTERVAL_MINUTES = 60.0
# Earlier builds' 24 hour medians from this far back feed the volatility estimate.
DAILY_MEDIAN_LOOKBACK_DAYS = 30
_INSERT_CHUNK_SIZE = 1000


async def fetch_sales_history() -> list[dict[str, Any]]:
    """Every item's sales history in USD. Skinport requires Brotli and allows 8 requests per 5 minutes."""
    params: dict[str, str | int] = {"app_id": 730, "currency": "USD"}
    timeout = aiohttp.ClientTimeout(total=120, connect=10)
    async with aiohttp.ClientSession(headers={"Accept-Encoding": "br"}, timeout=timeout) as session:
        async with session.get(SALES_HISTORY_URL, params=params) as response:
            response.raise_for_status()
            data = await response.json()
    if not isinstance(data, list):
        raise ValueError(f"Expected a list from {SALES_HISTORY_URL}, got {type(data).__name__}")
    return [entry for entry in data if isinstance(entry, dict)]


def parse_sales_history(entries: Iterable[Mapping[str, Any]]) -> list[SalesHistory]:
    """Parse the entries that name an item and are priced in USD."""
    histories = []
    for entry in entries:
        history = SalesHistory.from_skinport(entry)
        if history is not None:
            histories.append(history)
    return histories


async def latest_build_time(venue: str) -> datetime | None:
    async with async_engine.connect() as conn:
        result = await conn.execute(select(func.max(col(BaselineBuild.built_at))).where(col(BaselineBuild.venue) == venue))
        return result.scalar()


async def load_daily_medians(venue: str, since: datetime) -> dict[str, dict[date, int]]:
    """Each item's 24 hour median per UTC day from builds since `since`; the last build of a day wins."""
    stmt = (
        select(col(BaselineBuild.built_at), col(VenueBaseline.market_hash_name), col(VenueBaseline.median_24h_cents))
        .join(BaselineBuild, col(BaselineBuild.id) == col(VenueBaseline.build_id))
        .where(col(BaselineBuild.venue) == venue)
        .where(col(BaselineBuild.built_at) >= since)
        .where(col(VenueBaseline.median_24h_cents).is_not(None))
        .order_by(col(BaselineBuild.built_at))
    )
    medians: dict[str, dict[date, int]] = {}
    async with async_engine.connect() as conn:
        result = await conn.execute(stmt)
        for built_at, name, median_cents in result:
            medians.setdefault(name, {})[built_at.date()] = median_cents
    return medians


def build_baselines(
    histories: Iterable[SalesHistory], earlier_daily_medians: Mapping[str, Mapping[date, int]], today: date
) -> list[ItemBaseline]:
    """Baselines for every item with enough sales, sorted by name. Today's 24 hour median counts as today's point."""
    baselines = []
    for history in histories:
        daily = dict(earlier_daily_medians.get(history.market_hash_name, {}))
        day = history.last_24_hours
        if day.has_median() and day.median_cents is not None:
            daily[today] = day.median_cents
        baseline = build_item_baseline(history, [daily[key] for key in sorted(daily)])
        if baseline is not None:
            baselines.append(baseline)
    return sorted(baselines, key=lambda baseline: baseline.market_hash_name)


def baseline_row(build_id: int, baseline: ItemBaseline) -> dict[str, Any]:
    return {
        "build_id": build_id,
        "market_hash_name": baseline.market_hash_name,
        "latest_price_cents": baseline.latest_price_cents,
        "rolling_30d_avg_cents": baseline.rolling_30d_avg_cents,
        "rolling_90d_avg_cents": baseline.rolling_90d_avg_cents,
        "volatility_cents": baseline.volatility_cents,
        "support_floor_cents": baseline.support_floor_cents,
        "avg_volume_30d": baseline.avg_volume_30d,
        "drift_percent": baseline.drift_percent,
        "volatility_method": baseline.volatility_method,
        "median_24h_cents": baseline.median_24h_cents,
        "volume_24h": baseline.volume_24h,
        "median_7d_cents": baseline.median_7d_cents,
        "volume_7d": baseline.volume_7d,
        "min_30d_cents": baseline.min_30d_cents,
        "volume_30d": baseline.volume_30d,
        "volume_90d": baseline.volume_90d,
    }


async def save_build(venue: str, built_at: datetime, baselines: list[ItemBaseline]) -> int:
    """Store one build and its rows in a single transaction; returns the build ID."""
    async with async_engine.begin() as conn:
        result = await conn.execute(
            insert(BaselineBuild).values(venue=venue, method=BASELINE_METHOD, built_at=built_at, item_count=len(baselines))
        )
        inserted_key = result.inserted_primary_key
        if inserted_key is None:
            raise RuntimeError("The database did not return the new build ID")
        build_id = inserted_key[0]
        rows = [baseline_row(build_id, baseline) for baseline in baselines]
        for start in range(0, len(rows), _INSERT_CHUNK_SIZE):
            await conn.execute(insert(VenueBaseline), rows[start : start + _INSERT_CHUNK_SIZE])
    return build_id


@task(retries=2, retry_delay_seconds=60)
async def fetch_sales_history_task() -> list[dict[str, Any]]:
    return await fetch_sales_history()


@flow(name="baseline-builder")
async def build_venue_baselines(max_age_hours: float = DEFAULT_MAX_AGE_HOURS, force: bool = False) -> int | None:
    """Build and store a new baseline build unless a recent one exists. Returns the new build ID, or None."""
    run_logger = get_run_logger()
    now = utc_now_naive()
    newest = await latest_build_time(VENUE)
    if not force and newest is not None and now - newest < timedelta(hours=max_age_hours):
        run_logger.info("[BASELINES] Newest %s build is from %s; nothing to do.", VENUE, newest.isoformat())
        return None

    entries = await fetch_sales_history_task()
    histories = parse_sales_history(entries)
    earlier = await load_daily_medians(VENUE, now - timedelta(days=DAILY_MEDIAN_LOOKBACK_DAYS))
    baselines = build_baselines(histories, earlier, now.date())
    if not baselines:
        raise RuntimeError(f"No {VENUE} item had enough sales for a baseline; refusing to store an empty build.")

    build_id = await save_build(VENUE, now, baselines)
    run_logger.info(
        "[BASELINES] Stored %s build %d: %d baselines from %d items in the sales history.",
        VENUE,
        build_id,
        len(baselines),
        len(histories),
    )
    return build_id


async def run_forever(max_age_hours: float, check_interval_minutes: float) -> None:  # pragma: no cover - service loop
    """The baseline-builder service: check on start, then every interval. A failed run is retried next time."""
    while True:
        try:
            await build_venue_baselines(max_age_hours=max_age_hours)
        # Broad on purpose: one failed run (network, database) must not stop the service.
        except Exception:
            logger.exception("[BASELINES] Build failed; retrying in %.0f minutes.", check_interval_minutes)
        await asyncio.sleep(check_interval_minutes * 60)


if __name__ == "__main__":  # pragma: no cover - entrypoint glue, covered via unit tests
    import argparse

    parser = argparse.ArgumentParser(description="Build current Skinport baselines from Skinport's sales history.")
    parser.add_argument("--force", action="store_true", help="Build even if a recent build exists")
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=DEFAULT_MAX_AGE_HOURS,
        help=f"Build only when the newest build is older than this (default: {DEFAULT_MAX_AGE_HOURS:g})",
    )
    parser.add_argument("--loop", action="store_true", help="Keep running and check again every interval")
    parser.add_argument(
        "--check-interval-minutes",
        type=float,
        default=DEFAULT_CHECK_INTERVAL_MINUTES,
        help=f"With --loop, minutes between checks (default: {DEFAULT_CHECK_INTERVAL_MINUTES:g})",
    )
    args = parser.parse_args()

    validate_required_env(["DATABASE_URL"])
    if args.loop:
        asyncio.run(run_forever(args.max_age_hours, args.check_interval_minutes))
    else:
        asyncio.run(build_venue_baselines(max_age_hours=args.max_age_hours, force=args.force))
