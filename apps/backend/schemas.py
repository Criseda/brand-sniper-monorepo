from pydantic import BaseModel, Field

class AnomalyAlertPayload(BaseModel):
    """Strict data validation for inbound edge-calculated sniper targets."""
    market_hash_name: str = Field(..., description="The asset identifier")
    price_usd: float = Field(..., gt=0)
    price_cents: int = Field(..., gt=0)
    z_score: float = Field(..., description="The edge-computed volatility metric")
    triggered_at: int = Field(..., description="Unix timestamp of the edge trigger event")