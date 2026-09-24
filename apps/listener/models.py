from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

# Feed event types that represent a live offer and therefore feed the Z-score price window.
# Anything else (e.g. `sold`) is recorded for labeling but never drives a trading decision.
LISTED_EVENT_TYPE = "listed"


class MarketTick(BaseModel):
    """Strict edge validation schema for real-time asset pricing ticks."""

    market_hash_name: str = Field(..., description="The exact decoded identifier string of the asset")
    price_usd: float = Field(..., gt=0, description="Raw listing price in USD float format")
    timestamp: int = Field(
        default_factory=lambda: int(datetime.now(UTC).timestamp()), description="Unix timestamp of when the tick was parsed"
    )
    float_value: float | None = Field(default=None, description="Asset wear float value if available")
    stickers: list[dict] = Field(default_factory=list, description="List of applied stickers on the asset")

    # Listing-level context (#232). All None for aggregate REST snapshots.
    event_type: str | None = Field(default=None, description="Feed event type (listed, sold); None for REST snapshots")
    listing_id: str | None = Field(default=None, description="Venue identifier of the individual listing")
    pattern: int | None = Field(default=None, description="Paint seed of the listed asset")
    paint_index: int | None = Field(default=None, description="Finish (skin) identifier of the listed asset")
    listing_url: str | None = Field(default=None, description="Deep link that opens the listing on the venue")

    @property
    def price_cents(self) -> int:
        """Vector optimization converter to completely eliminate floating-point math rounding errors."""
        return int(round(self.price_usd * 100))

    @property
    def feeds_price_window(self) -> bool:
        """True for REST snapshots and live listings: the only ticks the Z-score/DRE path may see."""
        return self.event_type is None or self.event_type == LISTED_EVENT_TYPE

    def to_batch_record(self) -> dict[str, Any]:
        """Serialize for the durable bulk-ingest batch; listing fields are omitted when absent."""
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
        if self.stickers or self.event_type is not None:
            record["stickers"] = self.stickers
        return record


class FeedEvent(BaseModel):
    """One raw venue feed event, kept verbatim for the append-only `feed_events` table."""

    event_type: str = Field(..., min_length=1, description="Feed event type as reported by the venue")
    received_at_ms: int = Field(..., gt=0, description="Edge receive time as Unix epoch milliseconds")
    payload: dict[str, Any] = Field(..., description="Untouched venue payload")

    def to_batch_record(self) -> dict[str, Any]:
        return {"event_type": self.event_type, "received_at_ms": self.received_at_ms, "payload": self.payload}
