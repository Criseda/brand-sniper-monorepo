import sys
import uvicorn
from fastapi import FastAPI, status
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timezone
from sqlmodel import select, SQLModel

# Force standard streams to use UTF-8 to support Unicode characters (like ★) on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from schemas import AnomalyAlertPayload, BulkIngestionPayload
from database import AsyncSessionLocal, engine
from shared_utils.models import MarketItem, LiveMarketTick, HistoricalPrice

app = FastAPI(
    title="Algorithmic Market Sniper Engine",
    description="Core Compute REST API Node",
    version="1.0.0"
)

# Global in-memory cache mapping market_hash_name to item_id
item_cache = {}

def deduce_item_type(name: str) -> str:
    """Helper to classify items when creating new market item entries."""
    if "★" in name:
        if any(w in name for w in ["Gloves", "Wraps"]):
            return "Glove"
        return "Knife"
    if "Sticker |" in name or name.startswith("Sticker"):
        return "Sticker"
    if "Music Kit |" in name:
        return "Music Kit"
    if "Patch |" in name:
        return "Patch"
    factions = ["NSWC SEAL", "Guerrilla Warfare", "Sabre", "TACP", "Professionals", "FBI", "SWAT", "Gendarmerie", "KSK"]
    if any(f in name for f in factions) or "Agent" in name:
        return "Agent"
    wears = ["(Factory New)", "(Minimal Wear)", "(Field-Tested)", "(Well-Worn)", "(Battle-Scarred)"]
    if not any(w in name for w in wears):
        if any(c in name for c in ["Case", "Capsule", "Package", "Pin"]):
            return "Container/Collectible"
        return "Agent"
    return "Weapon Skin"

async def get_or_create_item_id(session, name: str) -> int:
    """Resolves item_id using fast in-memory cache, falling back to DB insert if missing."""
    if name in item_cache:
        return item_cache[name]
    
    item_type = deduce_item_type(name)
    stmt = insert(MarketItem).values(
        market_hash_name=name,
        item_type=item_type
    ).on_conflict_do_update(
        index_elements=["market_hash_name"],
        set_={"item_type": item_type}
    ).returning(MarketItem.id)
    
    result = await session.execute(stmt)
    item_id = result.scalar()
    item_cache[name] = item_id
    return item_id

@app.on_event("startup")
async def startup_event():
    """Guarantees the database schemas exist and seeds the local memory cache."""
    # Ensure tables are created
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    print("[CORE COMPUTE] Database schemas verified and mapped successfully.")
    
    # Load all existing market items into RAM cache
    async with AsyncSessionLocal() as session:
        stmt = select(MarketItem.market_hash_name, MarketItem.id)
        result = await session.execute(stmt)
        for name, item_id in result:
            item_cache[name] = item_id
            
    print(f"[CORE COMPUTE] Pre-cached {len(item_cache)} market items in memory.")

@app.post("/api/v1/alerts/anomaly", status_code=status.HTTP_202_ACCEPTED)
async def process_edge_anomaly(payload: AnomalyAlertPayload):
    print(f"\n[CORE COMPUTE] Anomaly Intercepted: {payload.market_hash_name} dropped to ${payload.price_usd:.2f} (Z={payload.z_score})")
    return {"status": "QUEUED"}

@app.post("/api/v1/ingest/bulk", status_code=status.HTTP_201_CREATED)
async def process_bulk_ingestion(payload: BulkIngestionPayload):
    total_ticks = len(payload.ticks)
    print(f"\n[CORE COMPUTE] Bulk Ingestion Intercepted: {total_ticks} elements from '{payload.source}'")
    
    if total_ticks == 0:
        return {"status": "SKIPPED", "records_processed": 0}

    async with AsyncSessionLocal() as session:
        async with session.begin():
            insert_data = []
            for tick in payload.ticks:
                item_id = await get_or_create_item_id(session, tick.market_hash_name)
                insert_data.append({
                    "item_id": item_id,
                    "price_cents": tick.price_cents,
                    "marketplace_source": payload.source,
                    "inserted_at": datetime.fromtimestamp(tick.timestamp, tz=timezone.utc).replace(tzinfo=None)
                })
            
            stmt = insert(LiveMarketTick)
            await session.execute(stmt, insert_data)
            
    print(f"   [POSTGRES] Bulk write complete. Committed {total_ticks} ticks to 'live_market_ticks'.")
    return {"status": "SUCCESS", "records_processed": total_ticks}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)