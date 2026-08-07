from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints

NonEmptyText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


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

    query: NonEmptyText = Field(..., description="The search query for macro trend analysis")


class BulkIngestionPayload(BaseModel):
    """Container schema for high-throughput multi-venue price uploads sent from edge nodes."""

    source: NonEmptyText = Field(..., description="The platform origin, e.g., 'skinport' or 'steam'")
    ticks: list[BulkPriceTick] = Field(..., description="Array of collected market snapshot blocks")
