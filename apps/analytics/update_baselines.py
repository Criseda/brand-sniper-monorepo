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

# Load environment variables
backend_env = PROJECT_ROOT / "apps" / "backend" / ".env"
if backend_env.exists():
    load_dotenv(dotenv_path=backend_env)

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
    """
    edge_redis_url = os.getenv("EDGE_REDIS_URL", "redis://localhost:6380")
    logger.info("Connecting to Edge Redis at %s", edge_redis_url)

    redis = Redis.from_url(edge_redis_url)

    logger.info("Fetching baselines from PostgreSQL...")
    async with async_engine.connect() as conn:
        stmt = select(
            MarketItem.market_hash_name,
            ItemMacroBaseline.support_floor_cents,
            ItemMacroBaseline.latest_price_cents,
        ).join(ItemMacroBaseline, MarketItem.id == ItemMacroBaseline.item_id)
        result = await conn.execute(stmt)
        rows = result.fetchall()

    logger.info("Found %d baselines. Pushing to Edge Redis...", len(rows))

    async with redis.pipeline(transaction=False) as pipe:
        for market_hash_name, support_floor, latest_price in rows:
            data = {"support_floor_cents": support_floor, "latest_price_cents": latest_price}
            pipe.set(f"baseline:{market_hash_name}", json.dumps(data))

        # Execute pipeline
        if rows:
            await pipe.execute()

    logger.info("Edge Redis sync complete!")
    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(sync_baselines_to_edge())
