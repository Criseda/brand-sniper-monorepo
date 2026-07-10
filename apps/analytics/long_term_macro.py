"""
Orchestrate long-term macroeconomic trend analysis using Prefect 3.0.
Calculates historical rolling baselines, seasonality metrics, and monitors price drift.
"""

import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import select

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parents[2]

root_env = PROJECT_ROOT / ".env"
if root_env.exists():
    load_dotenv(dotenv_path=root_env)

analytics_env = PROJECT_ROOT / "apps" / "analytics" / ".env"
if analytics_env.exists():
    load_dotenv(dotenv_path=analytics_env, override=True)

from prefect import flow, get_run_logger, task
from shared_utils.db_connection import async_engine
from shared_utils.models import HistoricalPrice, ItemMacroBaseline, MarketItem
from sqlalchemy.dialects.postgresql import insert


@task(retries=3, retry_delay_seconds=10)
async def fetch_tracked_items() -> list[dict]:
    """
    Task to fetch all tracked market items from the database.
    """
    logger = get_run_logger()
    logger.info("Fetching market items from database...")
    async with async_engine.connect() as conn:
        result = await conn.execute(select(MarketItem.id, MarketItem.market_hash_name, MarketItem.item_type))
        items = [{"id": r[0], "market_hash_name": r[1], "item_type": r[2]} for r in result.fetchall()]
    logger.info("Retrieved %d market items from database.", len(items))
    return items


@task(retries=3, retry_delay_seconds=10)
async def fetch_historical_prices(item_id: int, market_hash_name: str) -> list[dict]:
    """
    Task to fetch long-term historical price data for a specific item.
    """
    logger = get_run_logger()
    logger.info("Fetching historical prices for: %s (ID: %d)", market_hash_name, item_id)

    async with async_engine.connect() as conn:
        stmt = (
            select(HistoricalPrice.sale_date, HistoricalPrice.median_price_cents, HistoricalPrice.volume_sold)
            .where(HistoricalPrice.item_id == item_id)
            .order_by(HistoricalPrice.sale_date.asc())
        )
        result = await conn.execute(stmt)
        rows = result.fetchall()

    logger.info("Fetched %d historical records for '%s'.", len(rows), market_hash_name)
    return [{"sale_date": r[0], "median_price_cents": r[1], "volume_sold": r[2]} for r in rows]


@task
async def calculate_macro_trends(item_id: int, market_hash_name: str, price_data: list[dict]) -> dict:
    """
    Task to compute moving averages and macro-trends (e.g. 30-day and 90-day baselines).
    """
    logger = get_run_logger()
    if not price_data:
        logger.warning("No price data available for trend analysis on: %s", market_hash_name)
        return {}

    logger.info("Analyzing macro trends for: %s", market_hash_name)
    df = pd.DataFrame(price_data)
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df = df.sort_values("sale_date")

    # Calculate rolling averages
    df["rolling_30d"] = df["median_price_cents"].rolling(window=30, min_periods=1).mean()
    df["rolling_90d"] = df["median_price_cents"].rolling(window=90, min_periods=1).mean()

    # Calculate seasonal monthly means to look for cyclical behavior
    df["month"] = df["sale_date"].dt.month
    monthly_seasonality = df.groupby("month")["median_price_cents"].mean().to_dict()

    latest_price = df["median_price_cents"].iloc[-1]
    latest_30d = df["rolling_30d"].iloc[-1]
    latest_90d = df["rolling_90d"].iloc[-1]

    # Simple drift / momentum calculation
    drift_percent = ((latest_30d - latest_90d) / latest_90d * 100) if latest_90d else 0.0

    # Advanced risk & liquidity indicators
    volatility = df["median_price_cents"].tail(30).std()
    volatility_cents = int(round(volatility)) if pd.notna(volatility) else 0

    avg_volume = df["volume_sold"].tail(30).mean()
    avg_volume_30d = float(avg_volume) if pd.notna(avg_volume) else 0.0

    support_floor = df["median_price_cents"].quantile(0.10)
    support_floor_cents = int(round(support_floor)) if pd.notna(support_floor) else int(latest_price)

    coefficient_of_variation = (
        round(volatility_cents / int(latest_price), 4) if int(latest_price) > 0 and volatility_cents > 0 else 0.0
    )

    analysis = {
        "item_id": item_id,
        "market_hash_name": market_hash_name,
        "latest_price_cents": int(latest_price),
        "rolling_30d_avg_cents": int(round(latest_30d)),
        "rolling_90d_avg_cents": int(round(latest_90d)),
        "drift_percent": drift_percent,
        "volatility_cents": volatility_cents,
        "coefficient_of_variation": coefficient_of_variation,
        "avg_volume_30d": avg_volume_30d,
        "support_floor_cents": support_floor_cents,
        "monthly_seasonality": {int(k): float(v) for k, v in monthly_seasonality.items()},
        "total_points_analyzed": len(df),
    }

    logger.info(
        "[%s] Done. Price: $%.2f | 30d Avg: $%.2f | 90d Avg: $%.2f | "
        "Drift: %.2f%% | Volatility: $%.2f | 30d Vol: %.1f/day | Support: $%.2f",
        market_hash_name,
        latest_price / 100,
        latest_30d / 100,
        latest_90d / 100,
        drift_percent,
        volatility_cents / 100,
        avg_volume_30d,
        support_floor_cents / 100,
    )
    return analysis


@task
async def save_macro_baselines_to_db(analysis_results: list[dict]):
    """
    Saves/upserts the computed macroeconomic baseline trends into the database.
    """
    logger = get_run_logger()
    logger.info("Saving macroeconomic baselines to database...")

    valid_results = [r for r in analysis_results if r and "item_id" in r]
    if not valid_results:
        logger.warning("No valid analysis results to save.")
        return

    async with async_engine.connect() as conn:
        async with conn.begin():
            # Build insert statements for each result
            for res in valid_results:
                stmt = (
                    insert(ItemMacroBaseline)
                    .values(
                        item_id=res["item_id"],
                        latest_price_cents=res["latest_price_cents"],
                        rolling_30d_avg_cents=res["rolling_30d_avg_cents"],
                        rolling_90d_avg_cents=res["rolling_90d_avg_cents"],
                        drift_percent=res["drift_percent"],
                        volatility_cents=res["volatility_cents"],
                        avg_volume_30d=res["avg_volume_30d"],
                        support_floor_cents=res["support_floor_cents"],
                    )
                    .on_conflict_do_update(
                        index_elements=["item_id"],
                        set_={
                            "latest_price_cents": res["latest_price_cents"],
                            "rolling_30d_avg_cents": res["rolling_30d_avg_cents"],
                            "rolling_90d_avg_cents": res["rolling_90d_avg_cents"],
                            "drift_percent": res["drift_percent"],
                            "volatility_cents": res["volatility_cents"],
                            "avg_volume_30d": res["avg_volume_30d"],
                            "support_floor_cents": res["support_floor_cents"],
                            "updated_at": datetime.now(UTC).replace(tzinfo=None),
                        },
                    )
                )
                await conn.execute(stmt)

    total_items = len(analysis_results)
    logger.info("Successfully saved %d / %d macro baselines to database.", len(valid_results), total_items)
    if valid_results:
        # Show top drift item
        top_drift = max(valid_results, key=lambda x: x["drift_percent"])
        logger.info("Highest upward trend: '%s' with %.2f%% drift.", top_drift["market_hash_name"], top_drift["drift_percent"])


@task
async def run_sync_baselines_to_edge():
    """
    Prefect task wrapper to trigger Edge Redis sync.
    """
    from update_baselines import sync_baselines_to_edge

    await sync_baselines_to_edge()


@flow(name="long-term-macro-pipeline")
async def analyze_long_term_macro(limit_items: int | None = None):
    """
    Orchestration flow for analyzing macro price trends of tracked digital assets.
    """
    logger = get_run_logger()
    logger.info("Starting Long Term Macro Analysis Pipeline...")

    items = await fetch_tracked_items()

    if limit_items is not None:
        items_to_process = items[:limit_items]
        logger.info("Processing a subset of %d items (Limit: %d).", len(items_to_process), limit_items)
    else:
        items_to_process = items
        logger.info("Processing all %d items.", len(items_to_process))

    results = []
    for item in items_to_process:
        price_data = await fetch_historical_prices(item["id"], item["market_hash_name"])
        analysis = await calculate_macro_trends(item["id"], item["market_hash_name"], price_data)
        results.append(analysis)

    await save_macro_baselines_to_db(results)
    # Sync calculated baselines back to Edge Redis cache
    await run_sync_baselines_to_edge()
    logger.info("Long Term Macro Analysis Pipeline completed successfully.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Calculate macro baselines and sync to Redis.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of items to process (default: all).")
    args = parser.parse_args()

    # Allow running directly for development/testing
    asyncio.run(analyze_long_term_macro(limit_items=args.limit))
