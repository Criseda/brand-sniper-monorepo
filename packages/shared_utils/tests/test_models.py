import pytest
from datetime import datetime, timezone
from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy.pool import StaticPool

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

def test_create_simulated_trade(session: Session):
    # Setup market item
    item = MarketItem(market_hash_name="AK-47 | Redline (Field-Tested)", item_type="Rifle")
    session.add(item)
    session.commit()
    session.refresh(item)
    
    # Create Simulated Trade
    trade = SimulatedTrade(
        item_id=item.id,
        purchase_price_cents=1050,
        estimated_profit_cents=250,
        trigger_z_score=-2.85,
        simulated_buy_timestamp=datetime.now(timezone.utc)
    )
    session.add(trade)
    session.commit()
    session.refresh(trade)
    
    # Assertions
    assert trade.id is not None
    assert trade.item_id == item.id
    assert trade.purchase_price_cents == 1050
    assert trade.estimated_profit_cents == 250
    assert trade.trigger_z_score == -2.85
    assert isinstance(trade.simulated_buy_timestamp, datetime)
