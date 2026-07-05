import sys
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, status
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import SQLModel, select

# Dynamically resolve and load the backend's explicit environment file
backend_env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=backend_env_path)

# Force standard streams to use UTF-8 to support Unicode characters (like ★) on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from database import AsyncSessionLocal, engine
from queries import close_http_session
from schemas import BulkIngestionPayload, SimulatedTradePayload
from shared_utils import get_logger, parse_item_meta
from shared_utils.models import LiveMarketTick, MarketItem, SimulatedTrade
from telemetry import paper_trades_executed_total, paper_trading_estimated_profit_total

logger = get_logger("backend.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Guarantees the database schemas exist and seeds the local memory cache."""
    # Ensure tables are created
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    logger.info("Database schemas verified and mapped successfully.")

    # Load all existing market items into RAM cache
    async with AsyncSessionLocal() as session:
        stmt = select(MarketItem.market_hash_name, MarketItem.id)
        result = await session.execute(stmt)
        for name, item_id in result:
            item_cache[name] = item_id

    logger.info("Pre-cached %d market items in memory.", len(item_cache))
    yield
    # Graceful shutdown: clean up connections
    await close_http_session()
    await engine.dispose()
    logger.info("Connections closed, shutdown complete.")


app = FastAPI(
    title="Algorithmic Market Sniper Engine", description="Core Compute REST API Node", version="1.0.0", lifespan=lifespan
)

from prometheus_client import make_asgi_app

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Global in-memory cache mapping market_hash_name to item_id
item_cache: dict[str, int] = {}


async def get_or_create_item_id(session, name: str) -> int:
    """Resolves item_id using fast in-memory cache, falling back to DB insert if missing."""
    if name in item_cache:
        return item_cache[name]

    _, item_type = parse_item_meta(name)
    stmt = (
        insert(MarketItem)
        .values(market_hash_name=name, item_type=item_type)
        .on_conflict_do_update(index_elements=["market_hash_name"], set_={"item_type": item_type})
        .returning(MarketItem.id)
    )

    result = await session.execute(stmt)
    item_id = result.scalar()
    item_cache[name] = item_id
    return item_id


@app.post("/api/v1/ingest/trade", status_code=status.HTTP_201_CREATED)
async def ingest_simulated_trade(payload: SimulatedTradePayload):
    logger.info("Logging Simulated Trade: %s for $%.2f", payload.market_hash_name, payload.purchase_price_cents / 100)

    # Update Prometheus Telemetry
    paper_trades_executed_total.inc()
    paper_trading_estimated_profit_total.inc(payload.estimated_profit_cents)

    async with AsyncSessionLocal() as session:
        async with session.begin():
            item_id = await get_or_create_item_id(session, payload.market_hash_name)
            trade = SimulatedTrade(
                item_id=item_id,
                purchase_price_cents=payload.purchase_price_cents,
                estimated_profit_cents=payload.estimated_profit_cents,
                trigger_z_score=payload.trigger_z_score,
                simulated_buy_timestamp=datetime.now(UTC).replace(tzinfo=None),
            )
            session.add(trade)
    return {"status": "SUCCESS"}


@app.post("/api/v1/ingest/bulk", status_code=status.HTTP_201_CREATED)
async def process_bulk_ingestion(payload: BulkIngestionPayload):
    total_ticks = len(payload.ticks)
    logger.info("Bulk Ingestion Intercepted: %d elements from '%s'", total_ticks, payload.source)

    if total_ticks == 0:
        return {"status": "SKIPPED", "records_processed": 0}

    async with AsyncSessionLocal() as session:
        async with session.begin():
            insert_data = []
            for tick in payload.ticks:
                item_id = await get_or_create_item_id(session, tick.market_hash_name)
                insert_data.append(
                    {
                        "item_id": item_id,
                        "price_cents": tick.price_cents,
                        "marketplace_source": payload.source,
                        "inserted_at": datetime.fromtimestamp(tick.timestamp, tz=UTC).replace(tzinfo=None),
                    }
                )

            stmt = insert(LiveMarketTick)
            await session.execute(stmt, insert_data)

    logger.info("Bulk write complete. Committed %d ticks to 'live_market_ticks'.", total_ticks)
    return {"status": "SUCCESS", "records_processed": total_ticks}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
