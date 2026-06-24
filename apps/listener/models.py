from pydantic import BaseModel, Field
from datetime import datetime, timezone

class MarketTick(BaseModel):
    """Strict edge validation schema for real-time asset pricing ticks."""
    market_hash_name: str = Field(..., description="The exact decoded identifier string of the asset")
    price_usd: float = Field(..., gt=0, description="Raw listing price in USD float format")
    timestamp: int = Field(
        default_factory=lambda: int(datetime.now(timezone.utc).timestamp()),
        description="Unix timestamp of when the tick was parsed"
    )

    @property
    def price_cents(self) -> int:
        """Vector optimization converter to completely eliminate floating-point math rounding errors."""
        return int(round(self.price_usd * 100))