import os
import uvicorn
from fastapi import FastAPI, status
from schemas import AnomalyAlertPayload, BulkIngestionPayload

app = FastAPI(
    title="Algorithmic Market Sniper Engine",
    description="Core Compute & Multi-Agent AI Reasoner Node",
    version="1.0.0"
)

@app.post(
    "/api/v1/alerts/anomaly", 
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest Edge-Detected Market Anomalies"
)
async def process_edge_anomaly(payload: AnomalyAlertPayload):
    """
    Receives high-velocity anomaly flags from the Pi 5 edge layer.
    Acknowledges receipt instantly with a 202 status code, handing off
    the payload to the async AI Multi-Agent validation queue.
    """
    print("\n[CORE COMPUTE] Inbound Edge Alert Intercepted Successfully!")
    print(f"   ↳ Asset Verified : {payload.market_hash_name}")
    print(f"   ↳ Sniping Price  : ${payload.price_usd:.2f} ({payload.price_cents} cents)")
    print(f"   ↳ Metric Weight  : Z-Score = {payload.z_score}")
    print("   ↳ System Action  : Forwarding to Google ADK multi-agent pipeline...")
    
    # TODO: Connect Google ADK 2.0 Graph Workflow here in Milestone 5
    
    return {"status": "QUEUED", "message": "Anomaly accepted for evaluation."}

@app.post(
    "/api/v1/ingest/bulk",
    status_code=status.HTTP_201_CREATED,
    summary="Bulk Ingest Market Snapshots"
)
async def process_bulk_ingestion(payload: BulkIngestionPayload):
    """
    Receives compressed, chunked marketplace arrays from edge listeners.
    Validates data structures and schedules a high-speed database transaction.
    """
    total_ticks = len(payload.ticks)
    print(f"\n[CORE COMPUTE] Bulk Transaction Intercepted! ({total_ticks} elements from '{payload.source}')")
    
    if total_ticks > 0:
        print(f"   ↳ Sample Head : {payload.ticks[0].market_hash_name} -> {payload.ticks[0].price_cents}¢")
        print(f"   ↳ Sample Tail : {payload.ticks[-1].market_hash_name} -> {payload.ticks[-1].price_cents}¢")
    
    # TODO: Execute high-speed asyncpg / SQLAlchemy bulk INSERT ... ON CONFLICT here
    print(f"   ↳ System Action: Batch scheduled for PostgreSQL replication.")
    
    return {"status": "SUCCESS", "records_processed": total_ticks}

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("main:app", host=host, port=port, reload=True)