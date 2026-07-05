"""
Seed the database with historical prices from the CSV files.
Please run validate_historical.py first to ensure the data is clean.
"""

import argparse
import io
import sys
from pathlib import Path

import pandas as pd
from sqlmodel import select, text

# Force standard streams to use UTF-8 to support Unicode characters on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Dynamic path alignment to ensure the script can find the shared-utils package
sys.path.append(str(Path(__file__).resolve().parents[2]))

from shared_utils import get_logger, parse_item_meta
from shared_utils.db_connection import async_engine
from shared_utils.models import MarketItem

logger = get_logger("analytics.seed")

# Path pointing to where your Kaggle files live: /data/items/
DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "items"


async def seed_historical_data(truncate: bool = False):
    if not DATA_DIR.exists():
        logger.error("Data directory not found at target location: %s", DATA_DIR)
        return

    logger.info("Igniting High-Performance Bulk Ingestion Engine...")
    csv_files = list(DATA_DIR.glob("*.csv"))
    total_files = len(csv_files)
    logger.info("Found %d historical CSV files containing ~105M rows.", total_files)

    # Step 1: Temporarily drop index to maximize ingestion speed
    async with async_engine.begin() as conn:
        logger.info("Dropping index 'ix_historical_prices_item_date' to accelerate bulk load...")
        await conn.execute(text("DROP INDEX IF EXISTS ix_historical_prices_item_date"))
        if truncate:
            logger.info("Truncating table 'historical_prices'...")
            await conn.execute(text("TRUNCATE TABLE historical_prices RESTART IDENTITY"))

    # Step 2: Cache all existing MarketItems to prevent N+1 DB select queries
    logger.info("Caching existing MarketItems in memory...")
    item_cache = {}
    async with async_engine.connect() as conn:
        result = await conn.execute(select(MarketItem.market_hash_name, MarketItem.id))
        for name, item_id in result:
            item_cache[name] = item_id
    logger.info("Loaded %d market items into cache.", len(item_cache))

    # Track progress and execution statistics
    success_count = 0
    fail_count = 0

    # Step 3: Establish a single raw connection for the ingestion loop
    try:
        async with async_engine.connect() as conn:
            raw_connection = await conn.get_raw_connection()
            asyncpg_conn = raw_connection.driver_connection

            for idx, file_path in enumerate(csv_files, start=1):
                market_hash_name, item_type = parse_item_meta(file_path.name)

                # PROGRESS HEARTBEAT TICKER
                if idx % 500 == 0 or idx == 1 or idx == total_files:
                    logger.info(
                        "Seeding Progress: File %d/%d (%d%%) | Current: %s",
                        idx,
                        total_files,
                        int((idx / total_files) * 100),
                        market_hash_name,
                    )

                item_id = item_cache.get(market_hash_name)

                # Use a raw transaction/savepoint context to isolate failures per file
                try:
                    async with asyncpg_conn.transaction():
                        if not item_id:
                            # Insert and update cache using raw SQL to avoid ORM roundtrip overhead
                            item_id = await asyncpg_conn.fetchval(
                                "INSERT INTO market_items (market_hash_name, item_type) "
                                "VALUES ($1, $2) "
                                "ON CONFLICT (market_hash_name) DO UPDATE SET item_type = EXCLUDED.item_type "
                                "RETURNING id",
                                market_hash_name,
                                item_type,
                            )
                            item_cache[market_hash_name] = item_id

                        df = pd.read_csv(file_path)
                        if df.empty:
                            continue

                        # Coerce string corruptions safely to null and drop them instantly
                        df["unix timestamp"] = pd.to_numeric(df["unix timestamp"], errors="coerce")
                        df = df.dropna(subset=["unix timestamp"])

                        if df.empty:
                            continue

                        # Vectorized operations
                        df["item_id"] = item_id
                        df["sale_date"] = pd.to_datetime(df["unix timestamp"], unit="s", utc=True)
                        df["median_price_cents"] = (df["price"] * 100).round().astype(int)
                        df["volume_sold"] = df["quantity"].astype(int)

                        # Build in-memory buffer
                        csv_buffer = io.StringIO()
                        df[["item_id", "sale_date", "median_price_cents", "volume_sold"]].to_csv(
                            csv_buffer, index=False, header=False
                        )
                        csv_buffer.seek(0)
                        csv_data = csv_buffer.getvalue().encode("utf-8")

                        # Execute fast binary copy
                        await asyncpg_conn.copy_to_table(
                            "historical_prices",
                            source=io.BytesIO(csv_data),
                            columns=["item_id", "sale_date", "median_price_cents", "volume_sold"],
                            format="csv",
                        )
                        success_count += 1

                except Exception as e:
                    logger.warning("Skipping data for '%s' due to insertion failure: %s", market_hash_name, e)
                    fail_count += 1
                    continue

    finally:
        # Step 4: Always restore the index even if the main loop errored out
        logger.info("Recreating index 'ix_historical_prices_item_date' (this might take a few minutes for 100M+ rows)...")
        async with async_engine.begin() as conn:
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS ix_historical_prices_item_date ON historical_prices (item_id, sale_date)")
            )
        logger.info("Index restored successfully.")

    logger.info("Seeding completed. Successfully processed %d files. Failed files: %d.", success_count, fail_count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="High-performance Postgres historical price seeder.")
    parser.add_argument("--truncate", action="store_true", help="Truncate historical_prices before starting.")
    args = parser.parse_args()

    import asyncio

    asyncio.run(seed_historical_data(truncate=args.truncate))
