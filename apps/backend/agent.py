import os
import sys
import asyncio
import time
import json
from pathlib import Path
from redis import Redis

# Add project root and apps/backend to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / "apps" / "backend"))

from schemas import AnomalyAlertPayload
from rules_engine import evaluate_opportunity
from executor import PaperExecutor
from database import AsyncSessionLocal
from sqlmodel import select
from shared_utils.models import MarketItem

# MLflow logging in background
import mlflow
from mlflow.client import MlflowClient
tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(tracking_uri)
client_mlflow = MlflowClient(tracking_uri=tracking_uri)
try:
    exp = client_mlflow.get_experiment_by_name("sniper-verifier")
    experiment_id = exp.experiment_id if exp else client_mlflow.create_experiment("sniper-verifier")
except:
    experiment_id = "1"

# Redis client for DRE baseline lookups
redis_client = Redis(host="localhost", port=6379, decode_responses=True)

async def async_log_to_mlflow(payload: AnomalyAlertPayload, latency: float, approved: bool, error_msg: str = None):
    """Offloads the MLflow HTTP tracking so the hot path stays fast."""
    def _log():
        try:
            run = client_mlflow.create_run(experiment_id=experiment_id, run_name=f"verify_{payload.market_hash_name}")
            run_id = run.info.run_id
            client_mlflow.log_param(run_id, "market_hash_name", payload.market_hash_name)
            client_mlflow.log_param(run_id, "alert_price_cents", str(payload.price_cents))
            client_mlflow.log_metric(run_id, "dre_latency_seconds", latency)
            client_mlflow.log_metric(run_id, "alert_approved", 1 if approved else 0)
            client_mlflow.set_tag(run_id, "status", "APPROVED" if approved else ("FAILED" if error_msg else "REJECTED"))
            if error_msg:
                client_mlflow.log_param(run_id, "error", error_msg)
            client_mlflow.set_terminated(run_id, status="FINISHED" if not error_msg else "FAILED")
        except Exception as e:
            print(f"[MLFLOW ERROR] {e}")
            
    # Run synchronous MLFlow SDK calls in a background thread to avoid blocking asyncio event loop
    await asyncio.to_thread(_log)


async def run_verification_loop(payload: AnomalyAlertPayload, float_val: float = None):
    start_time = time.time()
    approved = False
    error_msg = None
    
    try:
        # 1. Deterministic Rules Engine (DRE) evaluation - instantly returns True/False
        approved = evaluate_opportunity(payload, redis_client)
        
        latency = time.time() - start_time
        print(f"[DRE] Verification for '{payload.market_hash_name}' completed in {latency:.4f}s. Result: {'APPROVED' if approved else 'REJECTED'}")
        
        # 2. If approved, execute immediately
        if approved:
            async with AsyncSessionLocal() as session:
                # Resolve item ID
                stmt = select(MarketItem.id).where(MarketItem.market_hash_name == payload.market_hash_name)
                result = await session.execute(stmt)
                item_id = result.scalar()
                
                if not item_id:
                    print(f"[ERROR] Cannot find item_id for {payload.market_hash_name}")
                    return
                
                # Fetch baseline to calculate estimated profit
                estimated_profit_cents = 0
                baseline_raw = redis_client.get(f"baseline:{payload.market_hash_name}")
                if baseline_raw:
                    baseline = json.loads(baseline_raw)
                    latest_price = baseline.get("latest_price_cents", payload.price_cents)
                    estimated_profit_cents = latest_price - payload.price_cents
                
                # Trigger Paper Execution
                executor = PaperExecutor(session)
                await executor.execute(
                    item_id=item_id,
                    purchase_price_cents=payload.price_cents,
                    estimated_profit_cents=max(0, estimated_profit_cents),
                    z_score=payload.z_score
                )
                
    except Exception as e:
        error_msg = str(e)
        print(f"[AGENT] Failed during fast verification execution: {e}")
        raise e
    finally:
        # 3. Fire-and-forget MLFlow logging completely detached from hot path
        asyncio.create_task(async_log_to_mlflow(payload, time.time() - start_time, approved, error_msg))
