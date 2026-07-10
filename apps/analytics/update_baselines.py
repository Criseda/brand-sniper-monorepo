import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from redis.asyncio import Redis
from sqlalchemy import select

# Align workspace directories for imports
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

# Load root .env (shared) first, then analytics-specific overrides
root_env = PROJECT_ROOT / ".env"
if root_env.exists():
    load_dotenv(dotenv_path=root_env)

analytics_env = PROJECT_ROOT / "apps" / "analytics" / ".env"
if analytics_env.exists():
    load_dotenv(dotenv_path=analytics_env, override=True)

from shared_utils import get_logger
from shared_utils.db_connection import async_engine
from shared_utils.models import ItemMacroBaseline, MarketItem

logger = get_logger("analytics.update_baselines")


async def sync_baselines_to_edge():
    """
    Reads calculated macro baselines from PostgreSQL and pushes them
    to the Edge Node's Redis instance for zero-latency hot-path evaluation.
    Also syncs sticker baseline prices to Redis hashmap 'sticker_prices'.
    """
    edge_redis_url = os.getenv("EDGE_REDIS_URL", "redis://localhost:6380")
    logger.info("Connecting to Edge Redis at %s", edge_redis_url)
    redis_password = os.getenv("REDIS_PASSWORD")
    redis = Redis.from_url(edge_redis_url, username="default", password=redis_password)

    logger.info("Fetching baselines from PostgreSQL...")
    async with async_engine.connect() as conn:
        stmt = select(
            MarketItem.market_hash_name,
            ItemMacroBaseline.support_floor_cents,
            ItemMacroBaseline.latest_price_cents,
            ItemMacroBaseline.rolling_30d_avg_cents,
            ItemMacroBaseline.volatility_cents,
            ItemMacroBaseline.drift_percent,
        ).join(ItemMacroBaseline, MarketItem.id == ItemMacroBaseline.item_id)
        result = await conn.execute(stmt)
        rows = result.fetchall()

        # Query sticker prices to populate the DRE sticker premium logic
        stmt_stickers = (
            select(MarketItem.market_hash_name, ItemMacroBaseline.latest_price_cents)
            .join(ItemMacroBaseline, MarketItem.id == ItemMacroBaseline.item_id)
            .where(MarketItem.item_type == "Sticker")
        )
        result_stickers = await conn.execute(stmt_stickers)
        sticker_rows = result_stickers.fetchall()

    logger.info("Found %d baselines. Pushing to Edge Redis...", len(rows))

    async with redis.pipeline(transaction=False) as pipe:
        for market_hash_name, support_floor, latest_price, rolling_30d_avg, volatility, drift in rows:
            data = {
                "support_floor_cents": support_floor,
                "latest_price_cents": latest_price,
                "rolling_30d_avg_cents": rolling_30d_avg,
                "volatility_cents": volatility,
                "drift_percent": drift,
                "coefficient_of_variation": round(volatility / rolling_30d_avg, 4) if rolling_30d_avg and volatility else 0.0,
            }
            pipe.set(f"baseline:{market_hash_name}", json.dumps(data))

        # Execute pipeline
        if rows:
            await pipe.execute()

    if sticker_rows:
        logger.info("Found %d sticker prices. Pushing to Edge Redis hash 'sticker_prices'...", len(sticker_rows))
        mapping = {name: str(price) for name, price in sticker_rows}
        await redis.hset("sticker_prices", mapping=mapping)

    logger.info("Edge Redis sync complete!")
    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(sync_baselines_to_edge())
