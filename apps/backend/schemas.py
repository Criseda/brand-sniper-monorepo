from pydantic import BaseModel, Field


class AnomalyAlertPayload(BaseModel):
    """Strict data validation for inbound edge-calculated sniper targets."""

    market_hash_name: str = Field(..., description="The asset identifier")
    price_usd: float = Field(..., gt=0)
    price_cents: int = Field(..., gt=0)
    z_score: float = Field(..., description="The edge-computed volatility metric")
    triggered_at: int = Field(..., description="Unix timestamp of the edge trigger event")
    float_value: float | None = Field(default=None, description="Asset wear float value if available")
    stickers: list[dict] = Field(default_factory=list, description="List of applied stickers on the asset")


class SimulatedTradePayload(BaseModel):
    """Schema for a simulated trade executed by an edge node."""

    market_hash_name: str
    purchase_price_cents: int
    estimated_profit_cents: int
    trigger_z_score: float


class BulkPriceTick(BaseModel):
    """Schema for an individual item vector within a bulk operation snapshot."""

    market_hash_name: str = Field(..., description="The asset identifier")
    price_cents: int = Field(..., gt=0, description="Item price normalized to integer cents")
    timestamp: int = Field(..., description="Unix timestamp of the ingestion event")


class SearchTrendsPayload(BaseModel):
    """Schema for the macro trend search query."""

    query: str = Field(..., description="The search query for macro trend analysis")


class BulkIngestionPayload(BaseModel):
    """Container schema for high-throughput multi-venue price uploads sent from edge nodes."""

    source: str = Field(..., description="The platform origin, e.g., 'skinport' or 'steam'")
    ticks: list[BulkPriceTick] = Field(..., description="Array of collected market snapshot blocks")
