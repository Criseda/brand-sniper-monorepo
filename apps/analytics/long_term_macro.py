"""
Orchestrate long-term macroeconomic trend analysis using Prefect 3.0.
Calculates historical rolling baselines, seasonality metrics, and monitors price drift.
"""

import os
import sys
import asyncio
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import select
# Force standard streams to use UTF-8 to support Unicode characters on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Align workspace directories for imports
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

# Load environment variables from backend .env if available
backend_env = PROJECT_ROOT / "apps" / "backend" / ".env"
if backend_env.exists():
    load_dotenv(dotenv_path=backend_env)

# Load local analytics .env overrides if available
analytics_env = PROJECT_ROOT / "apps" / "analytics" / ".env"
if analytics_env.exists():
    load_dotenv(dotenv_path=analytics_env, override=True)

from prefect import flow, task, get_run_logger
from sqlalchemy.dialects.postgresql import insert
from shared_utils.db_connection import async_engine
from shared_utils.models import MarketItem, HistoricalPrice, ItemMacroBaseline

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
    logger.info(f"Retrieved {len(items)} market items from database.")
    return items

@task(retries=3, retry_delay_seconds=10)
async def fetch_historical_prices(item_id: int, market_hash_name: str) -> list[dict]:
    """
    Task to fetch long-term historical price data for a specific item.
    """
    logger = get_run_logger()
    logger.info(f"Fetching historical prices for: {market_hash_name} (ID: {item_id})")
    
    async with async_engine.connect() as conn:
        stmt = (
            select(HistoricalPrice.sale_date, HistoricalPrice.median_price_cents, HistoricalPrice.volume_sold)
            .where(HistoricalPrice.item_id == item_id)
            .order_by(HistoricalPrice.sale_date.asc())
        )
        result = await conn.execute(stmt)
        rows = result.fetchall()
        
    logger.info(f"Fetched {len(rows)} historical records for '{market_hash_name}'.")
    return [{"sale_date": r[0], "median_price_cents": r[1], "volume_sold": r[2]} for r in rows]

@task
async def calculate_macro_trends(item_id: int, market_hash_name: str, price_data: list[dict]) -> dict:
    """
    Task to compute moving averages and macro-trends (e.g. 30-day and 90-day baselines).
    """
    logger = get_run_logger()
    if not price_data:
        logger.warning(f"No price data available for trend analysis on: {market_hash_name}")
        return {}
        
    logger.info(f"Analyzing macro trends for: {market_hash_name}")
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

    analysis = {
        "item_id": item_id,
        "market_hash_name": market_hash_name,
        "latest_price_cents": int(latest_price),
        "rolling_30d_avg_cents": int(round(latest_30d)),
        "rolling_90d_avg_cents": int(round(latest_90d)),
        "drift_percent": drift_percent,
        "volatility_cents": volatility_cents,
        "avg_volume_30d": avg_volume_30d,
        "support_floor_cents": support_floor_cents,
        "monthly_seasonality": {int(k): float(v) for k, v in monthly_seasonality.items()},
        "total_points_analyzed": len(df)
    }
    
    logger.info(
        f"[{market_hash_name}] Done. Price: ${latest_price/100:.2f} | "
        f"30d Avg: ${latest_30d/100:.2f} | 90d Avg: ${latest_90d/100:.2f} | "
        f"Drift: {drift_percent:.2f}% | Volatility: ${volatility_cents/100:.2f} | "
        f"30d Vol: {avg_volume_30d:.1f}/day | Support: ${support_floor_cents/100:.2f}"
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
                stmt = insert(ItemMacroBaseline).values(
                    item_id=res["item_id"],
                    latest_price_cents=res["latest_price_cents"],
                    rolling_30d_avg_cents=res["rolling_30d_avg_cents"],
                    rolling_90d_avg_cents=res["rolling_90d_avg_cents"],
                    drift_percent=res["drift_percent"],
                    volatility_cents=res["volatility_cents"],
                    avg_volume_30d=res["avg_volume_30d"],
                    support_floor_cents=res["support_floor_cents"]
                ).on_conflict_do_update(
                    index_elements=["item_id"],
                    set_={
                        "latest_price_cents": res["latest_price_cents"],
                        "rolling_30d_avg_cents": res["rolling_30d_avg_cents"],
                        "rolling_90d_avg_cents": res["rolling_90d_avg_cents"],
                        "drift_percent": res["drift_percent"],
                        "volatility_cents": res["volatility_cents"],
                        "avg_volume_30d": res["avg_volume_30d"],
                        "support_floor_cents": res["support_floor_cents"],
                        "updated_at": datetime.now(timezone.utc).replace(tzinfo=None)
                    }
                )
                await conn.execute(stmt)
                
    total_items = len(analysis_results)
    logger.info(f"Successfully saved {len(valid_results)} / {total_items} macro baselines to database.")
    if valid_results:
        # Show top drift item
        top_drift = max(valid_results, key=lambda x: x["drift_percent"])
        logger.info(f"Highest upward trend: '{top_drift['market_hash_name']}' with {top_drift['drift_percent']:.2f}% drift.")

@flow(name="long-term-macro-pipeline")
async def analyze_long_term_macro(limit_items: int = 5):
    """
    Orchestration flow for analyzing macro price trends of tracked digital assets.
    """
    logger = get_run_logger()
    logger.info("Starting Long Term Macro Analysis Pipeline...")
    
    items = await fetch_tracked_items()
    
    # Process up to limit_items to be resource-friendly during testing/dev
    items_to_process = items[:limit_items]
    logger.info(f"Processing a subset of {len(items_to_process)} items (Limit: {limit_items}).")
    
    results = []
    for item in items_to_process:
        price_data = await fetch_historical_prices(item["id"], item["market_hash_name"])
        analysis = await calculate_macro_trends(item["id"], item["market_hash_name"], price_data)
        results.append(analysis)
        
    await save_macro_baselines_to_db(results)
    logger.info("Long Term Macro Analysis Pipeline completed successfully.")

if __name__ == "__main__":
    # Allow running directly for development/testing
    asyncio.run(analyze_long_term_macro())
