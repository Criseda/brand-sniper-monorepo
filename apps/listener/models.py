from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, Field, model_validator

# Feed event types the listener knows. Only `listed` is a live offer that feeds the Z-score price window;
# `sold` (and any other type) is recorded for labeling but never drives a trading decision.
LISTED_EVENT_TYPE = "listed"
SOLD_EVENT_TYPE = "sold"

# Limits shared with the backend bulk-ingest schema (apps/backend/schemas.py). The backend rejects a
# whole batch with a non-retryable 422 when one record breaks them, so the edge enforces them first.
MAX_EVENT_TYPE_LENGTH = 32
MAX_VENUE_LENGTH = 32
MAX_LISTING_ID_LENGTH = 64
MAX_LISTING_URL_LENGTH = 512
# Venue names are the first part of edge Redis keys (`price_window:<venue>:<item>`), so they hold no colon.
VENUE_PATTERN = r"^[a-z0-9_-]+$"


class TickKind(StrEnum):
    """
    What a tick is. The code that builds a tick states it; it is never inferred from `event_type`, so a new
    feed parser that forgets the event type fails validation instead of passing as a REST snapshot (#270).
    """

    REST_SNAPSHOT = "rest_snapshot"  # An item's lowest ask from a REST poll; names no listing
    LISTED = "listed"  # A listing on the live feed: a price someone can buy now
    SOLD = "sold"  # A sale on the live feed: a market outcome, recorded only
    OTHER_FEED_EVENT = "other_feed_event"  # A feed event type the listener does not know: recorded only

    @classmethod
    def of_feed_event(cls, event_type: str) -> "TickKind":
        if event_type == LISTED_EVENT_TYPE:
            return cls.LISTED
        if event_type == SOLD_EVENT_TYPE:
            return cls.SOLD
        return cls.OTHER_FEED_EVENT


class MarketTick(BaseModel):
    """Strict edge validation schema for real-time asset pricing ticks."""

    venue: str = Field(
        ...,
        min_length=1,
        max_length=MAX_VENUE_LENGTH,
        pattern=VENUE_PATTERN,
        description="Marketplace the tick came from (e.g. skinport); selects its fee schedule",
    )
    kind: TickKind = Field(..., description="REST snapshot, or the kind of feed event the tick came from")
    market_hash_name: str = Field(..., description="The exact decoded identifier string of the asset")
    price_usd: float = Field(..., gt=0, description="Raw listing price in USD float format")
    timestamp: int = Field(
        default_factory=lambda: int(datetime.now(UTC).timestamp()), description="Unix timestamp of when the tick was parsed"
    )
    float_value: float | None = Field(default=None, ge=0, le=1, description="Asset wear float value if available")
    stickers: list[dict] = Field(default_factory=list, description="List of applied stickers on the asset")

    # Listing-level context. All None for aggregate REST snapshots.
    event_type: str | None = Field(
        default=None,
        max_length=MAX_EVENT_TYPE_LENGTH,
        description="Feed event type (listed, sold); None for REST snapshots",
    )
    listing_id: str | None = Field(
        default=None, max_length=MAX_LISTING_ID_LENGTH, description="Venue identifier of the individual listing"
    )
    pattern: int | None = Field(default=None, ge=0, description="Paint seed of the listed asset")
    paint_index: int | None = Field(default=None, ge=0, description="Finish (skin) identifier of the listed asset")
    listing_url: str | None = Field(
        default=None,
        max_length=MAX_LISTING_URL_LENGTH,
        description="Link that opens the listing (for Skinport, the item page)",
    )

    @model_validator(mode="after")
    def _kind_matches_event_type(self) -> Self:
        """A REST snapshot carries no event type, and a feed tick carries the type its kind names."""
        expected = TickKind.REST_SNAPSHOT if self.event_type is None else TickKind.of_feed_event(self.event_type)
        if self.kind != expected:
            raise ValueError(f"A {self.kind} tick cannot carry event_type {self.event_type!r}")
        return self

    @property
    def price_cents(self) -> int:
        """Vector optimization converter to completely eliminate floating-point math rounding errors."""
        return int(round(self.price_usd * 100))

    @property
    def is_rest_snapshot(self) -> bool:
        """True for an aggregate REST snapshot: an item's lowest ask, naming no listing."""
        return self.kind == TickKind.REST_SNAPSHOT

    @property
    def feeds_price_window(self) -> bool:
        """True for REST snapshots and live listings: the only ticks the Z-score/DRE path may see."""
        return self.kind in (TickKind.REST_SNAPSHOT, TickKind.LISTED)

    def to_batch_record(self) -> dict[str, Any]:
        """Serialize for the durable bulk-ingest batch; listing fields are omitted when absent.

        The venue is not repeated per record: the batch carries it once as its `source`."""
        record: dict[str, Any] = {
            "market_hash_name": self.market_hash_name,
            "price_cents": self.price_cents,
            "timestamp": self.timestamp,
        }
        optional_fields = {
            "event_type": self.event_type,
            "listing_id": self.listing_id,
            "float_value": self.float_value,
            "pattern": self.pattern,
            "paint_index": self.paint_index,
            "listing_url": self.listing_url,
        }
        for field_name, value in optional_fields.items():
            if value is not None:
                record[field_name] = value
        # Listing-level ticks always carry their sticker list ([] = none), so NULL keeps meaning "unknown".
        if self.stickers or not self.is_rest_snapshot:
            record["stickers"] = self.stickers
        return record


class FeedEvent(BaseModel):
    """One raw venue feed event, kept verbatim for the append-only `feed_events` table."""

    event_type: str = Field(
        ..., min_length=1, max_length=MAX_EVENT_TYPE_LENGTH, description="Feed event type as reported by the venue"
    )
    received_at_ms: int = Field(..., gt=0, description="Edge receive time as Unix epoch milliseconds")
    payload: dict[str, Any] = Field(..., description="Untouched venue payload")

    def to_batch_record(self) -> dict[str, Any]:
        return {"event_type": self.event_type, "received_at_ms": self.received_at_ms, "payload": self.payload}
