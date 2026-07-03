import pytest
import sys
import os
import asyncio
from sqlmodel import select
from shared_utils.models import SimulatedTrade, MarketItem

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agent import run_verification_loop
from schemas import AnomalyAlertPayload
from database import AsyncSessionLocal

@pytest.mark.asyncio
async def test_agent_integration_dre_and_executor(mocker):
    # Mock DRE to return True
    mocker.patch('agent.evaluate_opportunity', return_value=True)
    
    # Mock MLFlow to avoid HTTP calls in tests
    mocker.patch('agent.async_log_to_mlflow', return_value=None)
    
    # Ensure item exists in DB
    async with AsyncSessionLocal() as session:
        item = MarketItem(market_hash_name="Test Anomaly Item", item_type="Knife")
        session.add(item)
        await session.commit()
    
    payload = AnomalyAlertPayload(
        market_hash_name="Test Anomaly Item",
        price_cents=1000,
        price_usd=10.00,
        z_score=-4.0,
        float_value=0.15,
        stickers=[],
        triggered_at=1688385600
    )
    
    # Run the agent
    await run_verification_loop(payload)
    
    # Wait for background task if any (in this case run_verification_loop awaits the executor, only mlflow is background)
    await asyncio.sleep(0.1) 
    
    # Verify trade was inserted
    async with AsyncSessionLocal() as session:
        stmt = select(SimulatedTrade).join(MarketItem).where(MarketItem.market_hash_name == "Test Anomaly Item")
        result = await session.execute(stmt)
        trade = result.scalar()
        
        assert trade is not None
        assert trade.purchase_price_cents == 1000
        assert trade.trigger_z_score == -4.0
