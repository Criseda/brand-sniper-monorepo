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

from telemetry import rules_engine_latency_seconds, paper_trading_estimated_profit_total, paper_trades_executed_total

# Redis client for DRE baseline lookups
redis_client = Redis(host="localhost", port=6379, decode_responses=True)

async def run_verification_loop(payload: AnomalyAlertPayload, float_val: float = None):
    start_time = time.time()
    approved = False
    
    try:
        # 1. Deterministic Rules Engine (DRE) evaluation
        approved = evaluate_opportunity(payload, redis_client)
        
        latency = time.time() - start_time
        rules_engine_latency_seconds.observe(latency)
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
                
                profit_to_record = max(0, estimated_profit_cents)
                
                # Trigger Paper Execution
                executor = PaperExecutor(session)
                await executor.execute(
                    item_id=item_id,
                    purchase_price_cents=payload.price_cents,
                    estimated_profit_cents=profit_to_record,
                    z_score=payload.z_score
                )
                
                # Increment metrics
                paper_trades_executed_total.inc()
                paper_trading_estimated_profit_total.inc(profit_to_record)
                
    except Exception as e:
        print(f"[AGENT] Failed during fast verification execution: {e}")
        raise e
