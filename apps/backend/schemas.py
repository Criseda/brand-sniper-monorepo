from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, Field, StringConstraints

NonEmptyText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class SimulatedTradePayload(BaseModel):
    """Schema for a simulated trade executed by an edge node."""

    market_hash_name: str
    purchase_price_cents: int
    estimated_profit_cents: int | None = Field(
        ..., description="Estimated resale profit in cents; null when there was no baseline price to estimate from"
    )
    profit_estimate_basis: str | None = Field(
        default=None, max_length=32, description="How the estimate was computed, e.g. 'net_of_seller_fee'"
    )
    trigger_z_score: float
    listing_id: str | None = Field(default=None, max_length=64, description="Venue identifier of the bought listing")
    float_value: float | None = Field(default=None, ge=0, le=1, description="Float of the bought listing")


class BulkPriceTick(BaseModel):
    """Schema for an individual item vector within a bulk operation snapshot."""

    market_hash_name: str = Field(..., description="The asset identifier")
    price_cents: int = Field(..., gt=0, description="Item price normalized to integer cents")
    timestamp: int = Field(..., description="Unix timestamp of the ingestion event")

    # Listing-level context (#232); absent for aggregate REST snapshots.
    event_type: str | None = Field(default=None, max_length=32, description="Feed event type, e.g. 'listed' or 'sold'")
    listing_id: str | None = Field(default=None, max_length=64, description="Venue identifier of the listing")
    float_value: float | None = Field(default=None, ge=0, le=1, description="Asset wear float")
    pattern: int | None = Field(default=None, ge=0, description="Paint seed")
    paint_index: int | None = Field(default=None, ge=0, description="Finish (skin) identifier")
    stickers: list[dict[str, Any]] | None = Field(default=None, description="Applied stickers as reported by the venue")
    listing_url: str | None = Field(default=None, max_length=512, description="Deep link that opens the listing")


class BulkFeedEvent(BaseModel):
    """One raw venue feed event, persisted verbatim to the append-only feed_events table."""

    event_type: NonEmptyText = Field(..., max_length=32, description="Feed event type as reported by the venue")
    received_at_ms: int = Field(..., gt=0, description="Edge receive time as Unix epoch milliseconds")
    payload: dict[str, Any] = Field(..., description="Untouched venue payload")


class SearchTrendsPayload(BaseModel):
    """Schema for the macro trend search query."""

    query: NonEmptyText = Field(..., description="The search query for macro trend analysis")


class BulkIngestionPayload(BaseModel):
    """Container schema for high-throughput multi-venue price uploads sent from edge nodes."""

    batch_id: UUID | None = Field(
        default=None,
        description="Stable idempotency key for retries of the same batch",
    )
    source: NonEmptyText = Field(..., description="The platform origin, e.g., 'skinport' or 'steam'")
    ticks: list[BulkPriceTick] = Field(..., description="Array of collected market snapshot blocks")
    feed_events: list[BulkFeedEvent] = Field(default_factory=list, description="Raw feed events captured at the edge")
