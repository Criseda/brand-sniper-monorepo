import hashlib
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Integer, String, cast
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

# Load root .env (shared) first, then backend-specific overrides
project_root = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=project_root / ".env")
backend_env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=backend_env_path, override=True)

# Force standard streams to use UTF-8 to support Unicode characters (like ★) on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from api_errors import COMMON_ERROR_RESPONSES, problem_response, register_error_handlers
from database import engine, session_scope
from queries import close_http_session, get_item_market_context
from queries import search_macro_trends as query_macro_trends
from schemas import BulkIngestionPayload, SearchTrendsPayload, SimulatedTradePayload
from shared_utils import get_logger, parse_item_meta, utc_fromtimestamp_naive, utc_now_naive
from shared_utils.models import IngestionBatch, LiveMarketTick, MarketItem, SimulatedTrade
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
    async with session_scope() as session:
        stmt = select(MarketItem.market_hash_name, MarketItem.id)
        result = await session.exec(stmt)
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
register_error_handlers(app)

# Configure allowed CORS origins
cors_origins_raw = os.getenv("CORS_ORIGINS", "")
if cors_origins_raw:
    allow_origins = [origin.strip() for origin in cors_origins_raw.split(",") if origin.strip()]
else:
    allow_origins = [
        "http://localhost:3000",
        "http://localhost:8080",
        "http://localhost:5173",
        "http://localhost:4200",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:8080",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:4200",
    ]

if "*" in allow_origins:
    logger.warning("[AGENT] CORS wildcard '*' is configured with credentials enabled. This is a security risk.")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": "1.0.0"}


@app.get(
    "/api/v1/market/context/{market_hash_name:path}",
    responses={
        status.HTTP_404_NOT_FOUND: problem_response("Market item not found"),
        **COMMON_ERROR_RESPONSES,
    },
)
async def market_context(market_hash_name: str):
    context = await get_item_market_context(market_hash_name)
    if context is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found",
        )
    return context


@app.post("/api/v1/market/search-trends", responses=COMMON_ERROR_RESPONSES)
async def search_trends(payload: SearchTrendsPayload):
    results = await query_macro_trends(payload.query)
    return {"query": payload.query, "results": results}


from prometheus_client import make_asgi_app

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Global in-memory cache mapping market_hash_name to item_id
item_cache: dict[str, int] = {}


async def get_or_create_item_id(session: AsyncSession, name: str, pending_items: dict[str, int]) -> int:
    """Resolve an item ID without exposing it globally before transaction commit."""
    if name in item_cache:
        return item_cache[name]
    if name in pending_items:
        return pending_items[name]

    _, item_type = parse_item_meta(name)
    stmt = (
        insert(MarketItem)
        .values(market_hash_name=name, item_type=item_type)
        .on_conflict_do_update(index_elements=["market_hash_name"], set_={"item_type": item_type})
        .returning(cast(MarketItem.id, Integer))
    )

    result = await session.exec(stmt)
    item_id = result.scalar()
    if item_id is None:
        raise RuntimeError(f"Failed to resolve or create item_id for '{name}'")
    pending_items[name] = item_id
    return item_id


def _bulk_payload_digest(payload: BulkIngestionPayload) -> str:
    canonical_payload = {
        "source": payload.source,
        "ticks": [tick.model_dump(mode="json") for tick in payload.ticks],
    }
    encoded = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


async def _register_ingestion_batch(session: AsyncSession, payload: BulkIngestionPayload) -> bool:
    """Register a batch atomically, returning False for an identical replay."""
    if payload.batch_id is None:
        return True

    batch_id = str(payload.batch_id)
    digest = _bulk_payload_digest(payload)
    values = {
        "batch_id": batch_id,
        "source": payload.source,
        "record_count": len(payload.ticks),
        "payload_sha256": digest,
        "received_at": utc_now_naive(),
    }

    bind = session.get_bind()
    if bind.dialect.name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as dialect_insert

        stmt = (
            dialect_insert(IngestionBatch)
            .values(**values)
            .on_conflict_do_nothing(index_elements=["batch_id"])
            .returning(cast(IngestionBatch.batch_id, String))
        )
    else:
        stmt = (
            insert(IngestionBatch)
            .values(**values)
            .on_conflict_do_nothing(index_elements=["batch_id"])
            .returning(cast(IngestionBatch.batch_id, String))
        )
    result = await session.exec(stmt)
    if result.scalar_one_or_none() is not None:
        return True

    existing = await session.get(IngestionBatch, batch_id)
    if existing is None:
        raise RuntimeError(f"Failed to resolve ingestion batch '{batch_id}'")
    if existing.payload_sha256 != digest:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The batch_id has already been used with a different payload.",
        )
    return False


@app.post("/api/v1/ingest/trade", status_code=status.HTTP_201_CREATED)
async def ingest_simulated_trade(payload: SimulatedTradePayload):
    logger.info("Logging Simulated Trade: %s for $%.2f", payload.market_hash_name, payload.purchase_price_cents / 100)

    pending_items: dict[str, int] = {}
    async with session_scope() as session:
        item_id = await get_or_create_item_id(session, payload.market_hash_name, pending_items)
        trade = SimulatedTrade(
            item_id=item_id,
            purchase_price_cents=payload.purchase_price_cents,
            estimated_profit_cents=payload.estimated_profit_cents,
            trigger_z_score=payload.trigger_z_score,
            simulated_buy_timestamp=utc_now_naive(),
        )
        session.add(trade)

    item_cache.update(pending_items)

    paper_trades_executed_total.inc()
    paper_trading_estimated_profit_total.inc(payload.estimated_profit_cents)
    return {"status": "SUCCESS"}


@app.post(
    "/api/v1/ingest/bulk",
    status_code=status.HTTP_201_CREATED,
    responses=COMMON_ERROR_RESPONSES,
)
async def process_bulk_ingestion(payload: BulkIngestionPayload):
    total_ticks = len(payload.ticks)
    logger.info("Bulk Ingestion Intercepted: %d elements from '%s'", total_ticks, payload.source)

    if total_ticks == 0:
        if payload.batch_id is None:
            return {"status": "SKIPPED", "records_processed": 0}
        async with session_scope() as session:
            is_new_batch = await _register_ingestion_batch(session, payload)
        batch_status = "SKIPPED" if is_new_batch else "DUPLICATE"
        return {"status": batch_status, "records_processed": 0}

    pending_items: dict[str, int] = {}
    async with session_scope() as session:
        is_new_batch = await _register_ingestion_batch(session, payload)
        if not is_new_batch:
            logger.info("Bulk ingestion replay acknowledged for batch '%s'.", payload.batch_id)
            return {"status": "DUPLICATE", "records_processed": 0}

        insert_data = []
        for tick in payload.ticks:
            item_id = await get_or_create_item_id(session, tick.market_hash_name, pending_items)
            insert_data.append(
                {
                    "item_id": item_id,
                    "price_cents": tick.price_cents,
                    "marketplace_source": payload.source,
                    "inserted_at": utc_fromtimestamp_naive(tick.timestamp),
                }
            )

        stmt = insert(LiveMarketTick)
        await session.exec(stmt, params=insert_data)

    item_cache.update(pending_items)

    logger.info("Bulk write complete. Committed %d ticks to 'live_market_ticks'.", total_ticks)
    return {"status": "SUCCESS", "records_processed": total_ticks}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080)
