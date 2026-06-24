"""
Seed the database with historical prices from the CSV files.
Please run validate_historical.py first to ensure the data is clean.
"""

import os
import sys
import io
import argparse
from pathlib import Path
import urllib.parse
import pandas as pd
from sqlmodel import select, text

# Dynamic path alignment to ensure the script can find the shared-utils package
sys.path.append(str(Path(__file__).resolve().parents[2]))

from shared_utils.db_connection import async_engine
from shared_utils.models import MarketItem

# Path pointing to where your Kaggle files live: /data/items/
DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "items"

def parse_item_meta(filename: str) -> tuple[str, str]:
    """Advanced metadata decoding to correctly separate weapons, stickers, and agents."""
    clean_name = urllib.parse.unquote(filename.replace(".csv", ""))
    
    if "★" in clean_name:
        if any(w in clean_name for w in ["Gloves", "Wraps"]):
            return clean_name, "Glove"
        return clean_name, "Knife"
    
    if "Sticker |" in clean_name or clean_name.startswith("Sticker"):
        return clean_name, "Sticker"
    if "Music Kit |" in clean_name:
        return clean_name, "Music Kit"
    if "Patch |" in clean_name:
        return clean_name, "Patch"
        
    factions = ["NSWC SEAL", "Guerrilla Warfare", "Sabre", "TACP", "Professionals", "FBI", "SWAT", "Gendarmerie", "KSK"]
    if any(f in clean_name for f in factions) or "Agent" in clean_name:
        return clean_name, "Agent"
        
    wears = ["(Factory New)", "(Minimal Wear)", "(Field-Tested)", "(Well-Worn)", "(Battle-Scarred)"]
    if not any(w in clean_name for w in wears):
        if any(c in clean_name for c in ["Case", "Capsule", "Package", "Pin"]):
            return clean_name, "Container/Collectible"
        return clean_name, "Agent"
        
    return clean_name, "Weapon Skin"

async def seed_historical_data(truncate: bool = False):
    if not DATA_DIR.exists():
        print(f"Error: Data directory not found at target location: {DATA_DIR}")
        return

    print("Igniting High-Performance Bulk Ingestion Engine...")
    csv_files = list(DATA_DIR.glob("*.csv"))
    total_files = len(csv_files)
    print(f"Found {total_files} historical CSV files containing ~105M rows.\n")

    # Step 1: Temporarily drop index to maximize ingestion speed
    async with async_engine.begin() as conn:
        print("Dropping index 'ix_historical_prices_item_date' to accelerate bulk load...")
        await conn.execute(text("DROP INDEX IF EXISTS ix_historical_prices_item_date"))
        if truncate:
            print("Truncating table 'historical_prices'...")
            await conn.execute(text("TRUNCATE TABLE historical_prices RESTART IDENTITY"))

    # Step 2: Cache all existing MarketItems to prevent N+1 DB select queries
    print("Caching existing MarketItems in memory...")
    item_cache = {}
    async with async_engine.connect() as conn:
        result = await conn.execute(select(MarketItem.market_hash_name, MarketItem.id))
        for name, item_id in result:
            item_cache[name] = item_id
    print(f"Loaded {len(item_cache)} market items into cache.")

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
                    print(f"Seeding Progress: File {idx}/{total_files} ({int((idx/total_files)*100)}%) | Current: {market_hash_name}")
                    sys.stdout.flush()

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
                                market_hash_name, item_type
                            )
                            item_cache[market_hash_name] = item_id

                        df = pd.read_csv(file_path)
                        if df.empty:
                            continue
                        
                        # Coerce string corruptions safely to null and drop them instantly
                        df["unix timestamp"] = pd.to_numeric(df["unix timestamp"], errors='coerce')
                        df = df.dropna(subset=["unix timestamp"])

                        if df.empty:
                            continue

                        # Vectorized operations
                        df["item_id"] = item_id
                        df["sale_date"] = pd.to_datetime(df["unix timestamp"], unit='s', utc=True)
                        df["median_price_cents"] = (df["price"] * 100).round().astype(int)
                        df["volume_sold"] = df["quantity"].astype(int)

                        # Build in-memory buffer
                        csv_buffer = io.StringIO()
                        df[["item_id", "sale_date", "median_price_cents", "volume_sold"]].to_csv(
                            csv_buffer, index=False, header=False
                        )
                        csv_buffer.seek(0)
                        csv_data = csv_buffer.getvalue().encode('utf-8')

                        # Execute fast binary copy
                        await asyncpg_conn.copy_to_table(
                            "historical_prices",
                            source=io.BytesIO(csv_data),
                            columns=["item_id", "sale_date", "median_price_cents", "volume_sold"],
                            format="csv"
                        )
                        success_count += 1

                except Exception as e:
                    print(f"Skipping data for '{market_hash_name}' due to insertion failure: {e}")
                    fail_count += 1
                    continue

    finally:
        # Step 4: Always restore the index even if the main loop errored out
        print("Recreating index 'ix_historical_prices_item_date' (this might take a few minutes for 100M+ rows)...")
        async with async_engine.begin() as conn:
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS ix_historical_prices_item_date ON historical_prices (item_id, sale_date)")
            )
        print("Index restored successfully.")

    print(f"\nSeeding completed. Successfully processed {success_count} files. Failed files: {fail_count}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="High-performance Postgres historical price seeder.")
    parser.add_argument("--truncate", action="store_true", help="Truncate historical_prices before starting.")
    args = parser.parse_args()

    import asyncio
    asyncio.run(seed_historical_data(truncate=args.truncate))