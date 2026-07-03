import sys
import os
import pytest
from datetime import datetime, timezone
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, create_engine, Session, select

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from executor import PaperExecutor
from shared_utils.models import MarketItem, SimulatedTrade

@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

@pytest.mark.asyncio
async def test_paper_executor_inserts_trade(session: Session):
    # Setup market item
    item = MarketItem(market_hash_name="AWP | Asiimov (Field-Tested)", item_type="Sniper Rifle")
    session.add(item)
    session.commit()
    session.refresh(item)
    
    executor = PaperExecutor(session)
    
    # Execute paper trade
    await executor.execute(
        item_id=item.id,
        purchase_price_cents=4500,
        estimated_profit_cents=1500,
        z_score=-3.2
    )
    
    # Verify DB insertion
    stmt = select(SimulatedTrade).where(SimulatedTrade.item_id == item.id)
    trade = session.exec(stmt).first()
    
    assert trade is not None
    assert trade.purchase_price_cents == 4500
    assert trade.estimated_profit_cents == 1500
    assert trade.trigger_z_score == -3.2
