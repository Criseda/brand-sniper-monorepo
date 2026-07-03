import abc
from datetime import datetime, timezone
from sqlmodel import Session
from shared_utils.models import SimulatedTrade

class ExecutionService(abc.ABC):
    """
    Base interface for all trade executors (Live, Paper, Backtest).
    """
    @abc.abstractmethod
    async def execute(self, item_id: int, purchase_price_cents: int, estimated_profit_cents: int, z_score: float) -> None:
        pass

class PaperExecutor(ExecutionService):
    """
    Simulates a trade by logging it to the database instead of making a real marketplace API call.
    """
    def __init__(self, db_session: Session):
        self.session = db_session

    async def execute(self, item_id: int, purchase_price_cents: int, estimated_profit_cents: int, z_score: float) -> None:
        trade = SimulatedTrade(
            item_id=item_id,
            purchase_price_cents=purchase_price_cents,
            estimated_profit_cents=estimated_profit_cents,
            trigger_z_score=z_score,
            simulated_buy_timestamp=datetime.now(timezone.utc).replace(tzinfo=None)
        )
        self.session.add(trade)
        await self.session.commit()
        
        # Log to stdout for observability
        print(f"[PAPER TRADE] Simulated Buy | Item ID: {item_id} | Price: ${purchase_price_cents/100:.2f} | Est. Profit: ${estimated_profit_cents/100:.2f} | Z-Score: {z_score}")
