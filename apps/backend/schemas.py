from typing import Annotated, Any
from uuid import UUID

from pydantic import AliasChoices, BaseModel, Field, StringConstraints

NonEmptyText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class SimulatedTradePayload(BaseModel):
    """Schema for a simulated trade executed by an edge node."""

    market_hash_name: str
    venue: NonEmptyText = Field(..., max_length=32, description="Venue the listing was bought on, e.g. 'skinport' or 'waxpeer'")
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


class TradeAlertPayload(BaseModel):
    """One attempt by an edge node to alert a person about an approved listing on one channel (#18)."""

    venue: NonEmptyText = Field(..., max_length=32, description="Venue the listing is on, e.g. 'skinport' or 'waxpeer'")
    listing_id: str | None = Field(
        default=None, max_length=64, description="Venue identifier of the listing; null for a REST snapshot"
    )
    market_hash_name: NonEmptyText
    price_cents: int = Field(..., gt=0)
    estimated_net_margin_cents: int | None = Field(
        ..., description="Fee aware margin estimate the alert showed; null when there was no baseline price"
    )
    decision_source: NonEmptyText = Field(..., max_length=32, description="What approved the listing, e.g. 'rules'")
    approval_reason: NonEmptyText = Field(..., max_length=32, description="DRE rule that approved it")
    z_source: NonEmptyText = Field(..., max_length=16, description="local, hybrid, or macro")
    z_score: float
    channel: NonEmptyText = Field(..., max_length=16, description="discord or telegram")
    outcome: NonEmptyText = Field(..., max_length=16, description="sent, rate_limited, or failed")
    received_at: int = Field(..., gt=0, description="Unix time the edge received the tick")
    alerted_at_ms: int = Field(..., gt=0, description="Unix epoch milliseconds when the attempt ended on the edge")
    latency_ms: int = Field(..., ge=0, description="From building the tick to the end of the attempt")


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
    # Sent as `source` before the venue columns were renamed (#261); still accepted, so a listener that is not
    # upgraded yet, or a batch it stored in Redis, is not rejected.
    venue: NonEmptyText = Field(
        ...,
        max_length=32,
        validation_alias=AliasChoices("venue", "source"),
        description="Venue the batch came from, e.g. 'skinport' or 'waxpeer'",
    )
    ticks: list[BulkPriceTick] = Field(..., description="Array of collected market snapshot blocks")
    feed_events: list[BulkFeedEvent] = Field(default_factory=list, description="Raw feed events captured at the edge")
