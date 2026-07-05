"""
Verify the integrity of historical database seeding.
Run this script after seed_historical.py finishes.
"""

import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from sqlmodel import text

# Dynamic path alignment to find shared-utils package
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

from shared_utils import get_logger
from shared_utils.db_connection import async_engine

logger = get_logger("analytics.verify")


async def run_verification():
    logger.info("======================================================================")
    logger.info("              POST-FLIGHT DATABASE VERIFICATION SWEEP                 ")
    logger.info("======================================================================")

    start_time = time.time()

    async with async_engine.connect() as conn:
        # 1. Index health and validity check (Check this first as it takes <1ms)
        logger.info("Checking index health on 'historical_prices'...")
        index_query = text("""
            SELECT
                i.relname AS index_name,
                idx.indisvalid AS is_valid
            FROM
                pg_class t
            JOIN
                pg_index idx ON t.oid = idx.indrelid
            JOIN
                pg_class i ON i.oid = idx.indexrelid
            WHERE
                t.relname = 'historical_prices'
                AND i.relname = 'ix_historical_prices_item_date';
        """)
        index_result = await conn.execute(index_query)
        index_row = index_result.fetchone()

        if index_row:
            index_name, is_valid = index_row
            status = "VALID" if is_valid else "INVALID/CORRUPT"
            if is_valid:
                logger.info("  Result: Index '%s' is %s.", index_name, status)
            else:
                logger.warning("  Result: Index '%s' is %s. Rebuilding might be required.", index_name, status)
        else:
            logger.error("  Result: Index 'ix_historical_prices_item_date' was NOT found in system catalog!")

        # 2. Combined table statistics scan (Consolidates 4 separate sequential scans into 1)
        logger.info("Performing unified data integrity scan (row counts, nulls, invalid values, and date ranges)...")
        stats_query = text("""
            SELECT
                COUNT(*) as total_rows,
                MIN(sale_date) as min_date,
                MAX(sale_date) as max_date,
                COUNT(*) FILTER (
                    WHERE item_id IS NULL OR sale_date IS NULL
                    OR median_price_cents IS NULL OR volume_sold IS NULL
                ) as null_rows,
                COUNT(*) FILTER (WHERE median_price_cents <= 0 OR volume_sold < 0) as invalid_rows
            FROM historical_prices;
        """)
        stats_result = await conn.execute(stats_query)
        total_rows, min_date, max_date, null_count, invalid_val_count = stats_result.fetchone()

        logger.info("  Total rows found    : %s", f"{total_rows:,}")
        logger.info("  Null columns count  : %s", null_count)
        logger.info("  Invalid values count: %s (prices <= 0 or volumes < 0)", f"{invalid_val_count:,}")
        logger.info("  Date range bounds   : %s to %s", min_date, max_date)

        if total_rows == 0:
            logger.error("Failure: No records found in 'historical_prices'. Seeding failed.")
            return

        # 3. MarketItems mapping check (Optimized with EXISTS instead of COUNT DISTINCT)
        logger.info("Checking mapping coverage on market_items...")
        unique_items_in_prices = await conn.scalar(
            text("""
            SELECT COUNT(*) FROM market_items
            WHERE EXISTS (
                SELECT 1 FROM historical_prices
                WHERE historical_prices.item_id = market_items.id
            )
        """)
        )
        total_market_items = await conn.scalar(text("SELECT COUNT(*) FROM market_items"))
        logger.info(
            "  Result: %s items have historical prices (out of %s total market items).",
            f"{unique_items_in_prices:,}",
            f"{total_market_items:,}",
        )

        # 4. Duplication Check (Utilizes group aggregate scan on the index)
        logger.info("Checking for duplicate pricing keys (same item and timestamp)...")
        dup_query = text("""
            SELECT COUNT(*) FROM (
                SELECT item_id, sale_date
                FROM historical_prices
                GROUP BY item_id, sale_date
                HAVING COUNT(*) > 1
            ) as duplicates;
        """)
        duplicates = await conn.scalar(dup_query)
        if duplicates == 0:
            logger.info("  Result: Success! No duplicates found.")
        else:
            logger.warning("  Result: Found %s keys with duplicate dates. Data may have double-seeded.", f"{duplicates:,}")

        # 5. Print Sample data counts (Optimized grouping before join to avoid joining 105M rows)
        logger.info("Top 5 tracked items by volume count:")
        sample_query = text("""
            SELECT m.market_hash_name, t.count
            FROM (
                SELECT item_id, COUNT(*) as count
                FROM historical_prices
                GROUP BY item_id
                ORDER BY count DESC
                LIMIT 5
            ) t
            JOIN market_items m ON t.item_id = m.id
        """)
        samples = await conn.execute(sample_query)
        for name, count in samples:
            logger.info("  - %-50s : %s price points", name, f"{count:,}")

    elapsed = time.time() - start_time
    logger.info("======================================================================")
    logger.info("          VERIFICATION COMPLETE (Execution Time: %.2fs)       ", elapsed)
    logger.info("======================================================================")


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_verification())
