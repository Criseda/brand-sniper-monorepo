import hashlib
import json
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Response, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from shared_utils import setup_service_environment
from sqlalchemy import Integer, String, cast, func
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

# Load root .env (shared) first, then backend-specific overrides
setup_service_environment(__file__)

from api_errors import COMMON_ERROR_RESPONSES, problem_response, register_error_handlers
from database import engine, session_scope
from queries import close_http_session, get_item_market_context, get_latest_baseline_build
from queries import search_macro_trends as query_macro_trends
from schemas import BulkIngestionPayload, SearchTrendsPayload, SimulatedTradePayload, TradeAlertPayload
from shared_utils import (
    BACKEND_API_KEY_HEADER,
    BackendApiKeyConfigError,
    get_backend_api_key,
    get_logger,
    parse_item_meta,
    utc_fromtimestamp_naive,
    utc_now_naive,
)
from shared_utils.models import FeedEvent, IngestionBatch, LiveMarketTick, MarketItem, SimulatedTrade, TradeAlertRecord
from telemetry import paper_trades_executed_total, paper_trading_estimated_profit_total

logger = get_logger("backend.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Guarantees the database schemas exist and seeds the local memory cache."""
    get_backend_api_key()

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

api_key_header = APIKeyHeader(name=BACKEND_API_KEY_HEADER, auto_error=False, scheme_name="BackendApiKey")


async def require_backend_api_key(
    provided_api_key: Annotated[str | None, Security(api_key_header)],
) -> None:
    """Reject unauthenticated access to application API routes."""
    try:
        expected_api_key = get_backend_api_key()
    except BackendApiKeyConfigError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Backend API authentication is not configured.",
        ) from exc

    if provided_api_key is None or not secrets.compare_digest(provided_api_key, expected_api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="A valid backend API key is required.",
        )


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
    dependencies=[Depends(require_backend_api_key)],
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


@app.post(
    "/api/v1/market/search-trends",
    dependencies=[Depends(require_backend_api_key)],
    responses=COMMON_ERROR_RESPONSES,
)
async def search_trends(payload: SearchTrendsPayload):
    results = await query_macro_trends(payload.query)
    return {"query": payload.query, "results": results}


@app.get(
    "/api/v1/baselines/{venue}/latest",
    dependencies=[Depends(require_backend_api_key)],
    responses={
        status.HTTP_204_NO_CONTENT: {"description": "The newest build is not newer than after_build_id"},
        status.HTTP_404_NOT_FOUND: problem_response("The venue has no baseline build yet"),
        **COMMON_ERROR_RESPONSES,
    },
)
async def latest_baselines(venue: str, after_build_id: Annotated[int | None, Query(ge=0)] = None):
    """The newest baseline build for a venue, for the listener to load into the edge Redis."""
    build = await get_latest_baseline_build(venue, after_build_id)
    if build is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No baseline build for venue '{venue}'")
    if isinstance(build, int):
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return build


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
    # Defaults are excluded so a batch recorded before the listing-level fields (#232) existed
    # still hashes identically on replay. The venue is hashed under its original key, "source", so a batch
    # recorded before the rename to `venue` still matches its ledger entry when it is delivered again.
    canonical_payload: dict = {
        "source": payload.venue,
        "ticks": [tick.model_dump(mode="json", exclude_defaults=True) for tick in payload.ticks],
    }
    if payload.feed_events:
        canonical_payload["feed_events"] = [event.model_dump(mode="json") for event in payload.feed_events]
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
        "venue": payload.venue,
        "record_count": len(payload.ticks) + len(payload.feed_events),
        "payload_sha256": digest,
        "received_at": utc_now_naive(),
    }

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


async def lock_purchase(session: AsyncSession, venue: str, item_id: int, purchase_price_cents: int) -> None:
    """Serializes buys of one item at one price on one venue until the transaction ends (PostgreSQL only).

    Without it a REST snapshot's repeat check could run before a concurrent listing buy commits, and
    both would be recorded. The in-memory SQLite test engine has a single writer, so it needs no lock.
    """
    if session.get_bind().dialect.name != "postgresql":
        return
    key = f"purchase:{venue}:{item_id}:{purchase_price_cents}"
    await session.exec(select(func.pg_advisory_xact_lock(func.hashtext(key))))


async def snapshot_already_bought(
    session: AsyncSession, payload: SimulatedTradePayload, item_id: int, bought_at: datetime
) -> bool:
    """A REST snapshot buy repeats any buy of the item at that price on the venue the same UTC day.

    The snapshot unique index covers repeated snapshots. This also covers a listing bought earlier that
    day, which the next REST poll shows as the item's lowest ask.
    """
    day_start = bought_at.replace(hour=0, minute=0, second=0, microsecond=0)
    stmt = (
        select(SimulatedTrade.id)
        .where(
            SimulatedTrade.venue == payload.venue,
            SimulatedTrade.item_id == item_id,
            SimulatedTrade.purchase_price_cents == payload.purchase_price_cents,
            SimulatedTrade.simulated_buy_timestamp >= day_start,
            SimulatedTrade.simulated_buy_timestamp < day_start + timedelta(days=1),
        )
        .limit(1)
    )
    result = await session.exec(stmt)
    return result.first() is not None


@app.post(
    "/api/v1/ingest/trade",
    dependencies=[Depends(require_backend_api_key)],
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_200_OK: {"description": "The purchase was already recorded; nothing was stored"},
        **COMMON_ERROR_RESPONSES,
    },
)
async def ingest_simulated_trade(payload: SimulatedTradePayload, response: Response):
    logger.info("Logging Simulated Trade: %s for $%.2f", payload.market_hash_name, payload.purchase_price_cents / 100)

    pending_items: dict[str, int] = {}
    bought_at = utc_now_naive()
    async with session_scope() as session:
        item_id = await get_or_create_item_id(session, payload.market_hash_name, pending_items)
        trade_id: int | None = None
        await lock_purchase(session, payload.venue, item_id, payload.purchase_price_cents)
        if payload.listing_id is not None or not await snapshot_already_bought(session, payload, item_id, bought_at):
            # The unique purchase indexes on simulated_trades decide; a repeat inserts nothing.
            stmt = (
                insert(SimulatedTrade)
                .values(
                    item_id=item_id,
                    venue=payload.venue,
                    purchase_price_cents=payload.purchase_price_cents,
                    estimated_profit_cents=payload.estimated_profit_cents,
                    profit_estimate_basis=payload.profit_estimate_basis,
                    trigger_z_score=payload.trigger_z_score,
                    listing_id=payload.listing_id,
                    float_value=payload.float_value,
                    simulated_buy_timestamp=bought_at,
                )
                .on_conflict_do_nothing()
                .returning(cast(SimulatedTrade.id, Integer))
            )
            trade_id = (await session.exec(stmt)).scalar()

    item_cache.update(pending_items)
    if trade_id is None:
        # A listener that restarted, or any executor, sent a purchase already recorded: report it, do not count it.
        logger.info(
            "Trade already recorded: %s for $%.2f on %s (listing %s)",
            payload.market_hash_name,
            payload.purchase_price_cents / 100,
            payload.venue,
            payload.listing_id or "REST snapshot",
        )
        response.status_code = status.HTTP_200_OK
        return {"status": "ALREADY_RECORDED"}

    paper_trades_executed_total.inc()
    if payload.estimated_profit_cents is not None:
        paper_trading_estimated_profit_total.inc(payload.estimated_profit_cents)
    return {"status": "SUCCESS"}


@app.post(
    "/api/v1/ingest/alert",
    dependencies=[Depends(require_backend_api_key)],
    status_code=status.HTTP_201_CREATED,
    responses=COMMON_ERROR_RESPONSES,
)
async def ingest_trade_alert(payload: TradeAlertPayload):
    """Stores one alert attempt. Every attempt is kept, repeats included: each one reached (or failed to reach) a person."""
    record = TradeAlertRecord(
        venue=payload.venue,
        listing_id=payload.listing_id,
        market_hash_name=payload.market_hash_name,
        price_cents=payload.price_cents,
        estimated_net_margin_cents=payload.estimated_net_margin_cents,
        decision_source=payload.decision_source,
        approval_reason=payload.approval_reason,
        z_source=payload.z_source,
        z_score=payload.z_score,
        channel=payload.channel,
        outcome=payload.outcome,
        received_at=utc_fromtimestamp_naive(payload.received_at),
        alerted_at=utc_fromtimestamp_naive(payload.alerted_at_ms / 1000),
        latency_ms=payload.latency_ms,
    )
    async with session_scope() as session:
        session.add(record)
    return {"status": "SUCCESS"}


@app.post(
    "/api/v1/ingest/bulk",
    dependencies=[Depends(require_backend_api_key)],
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_409_CONFLICT: problem_response("Batch ID already used with different content"),
        **COMMON_ERROR_RESPONSES,
    },
)
async def process_bulk_ingestion(payload: BulkIngestionPayload):
    total_ticks = len(payload.ticks)
    total_feed_events = len(payload.feed_events)
    logger.info(
        "Bulk Ingestion Intercepted: %d elements and %d feed events from '%s'",
        total_ticks,
        total_feed_events,
        payload.venue,
    )

    if total_ticks == 0 and total_feed_events == 0:
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

        tick_rows = []
        for tick in payload.ticks:
            item_id = await get_or_create_item_id(session, tick.market_hash_name, pending_items)
            tick_rows.append(
                {
                    "item_id": item_id,
                    "price_cents": tick.price_cents,
                    "venue": payload.venue,
                    "inserted_at": utc_fromtimestamp_naive(tick.timestamp),
                    "event_type": tick.event_type,
                    "listing_id": tick.listing_id,
                    "float_value": tick.float_value,
                    "pattern": tick.pattern,
                    "paint_index": tick.paint_index,
                    "stickers": tick.stickers,
                    "listing_url": tick.listing_url,
                }
            )
        if tick_rows:
            await session.exec(insert(LiveMarketTick), params=tick_rows)

        feed_event_rows = [
            {
                "venue": payload.venue,
                "event_type": event.event_type,
                "received_at": utc_fromtimestamp_naive(event.received_at_ms / 1000),
                "payload": event.payload,
            }
            for event in payload.feed_events
        ]
        if feed_event_rows:
            await session.exec(insert(FeedEvent), params=feed_event_rows)

    item_cache.update(pending_items)

    logger.info(
        "Bulk write complete. Committed %d ticks to 'live_market_ticks' and %d events to 'feed_events'.",
        total_ticks,
        total_feed_events,
    )
    return {"status": "SUCCESS", "records_processed": total_ticks, "feed_events_processed": total_feed_events}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080)
