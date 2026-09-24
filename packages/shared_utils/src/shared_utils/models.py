from datetime import datetime
from typing import Any

from sqlalchemy import JSON, BigInteger, Column, Integer, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, Index, SQLModel

# JSONB on PostgreSQL, plain JSON elsewhere (the backend tests run on in-memory SQLite).
# none_as_null stores Python None as SQL NULL rather than the JSON literal 'null'.
JsonDocument = JSON(none_as_null=True).with_variant(JSONB(none_as_null=True), "postgresql")
# BIGINT identity on PostgreSQL; SQLite only autoincrements an INTEGER primary key.
BigIdentity = BigInteger().with_variant(Integer(), "sqlite")


class MarketItem(SQLModel, table=True):
    """
    Master reference directory for all tracked digital assets.
    """

    __tablename__: str = "market_items"

    id: int | None = Field(default=None, primary_key=True)
    market_hash_name: str = Field(index=True, unique=True, nullable=False)
    item_type: str = Field(index=True)  # e.g., Knife, Rifle, Glove
    rarity: str | None = Field(default=None)  # e.g., Covert, Classified


class LiveMarketTick(SQLModel, table=True):
    """
    High-velocity storage tracking real-time price updates from live endpoints.
    """

    __tablename__: str = "live_market_ticks"

    id: int | None = Field(default=None, primary_key=True)

    # Added ondelete="CASCADE" so if an item is deleted, its ticks clear out automatically
    item_id: int = Field(foreign_key="market_items.id", ondelete="CASCADE", index=True)

    price_cents: int = Field(nullable=False)
    marketplace_source: str = Field(default="steam")  # e.g., steam, csfloat, skinport

    float_value: float | None = Field(default=None, index=True)  # Exact item wear (0.0 - 1.0)
    paint_index: int | None = Field(default=None)  # Finish (skin) identifier, Skinport `finish`
    pattern: int | None = Field(default=None)  # Paint seed, Skinport `pattern`

    # Listing-level context. All NULL on aggregate REST snapshot rows, which carry no listing.
    listing_id: str | None = Field(default=None, max_length=64)
    event_type: str | None = Field(default=None, max_length=32)  # e.g. listed, sold
    stickers: list[dict[str, Any]] | None = Field(default=None, sa_column=Column(JsonDocument, nullable=True))
    # Link that opens the listing. For Skinport this is the item page (the feed sends no sale ID).
    listing_url: str | None = Field(default=None, max_length=512)

    # Let the PostgreSQL server safely generate the UTC timestamp natively
    inserted_at: datetime = Field(sa_column_kwargs={"server_default": text("TIMEZONE('utc', NOW())")}, index=True)

    # Partial index: most rows are REST snapshots with a NULL listing_id, which would only bloat
    # the index and slow the hot ingest path.
    __table_args__ = (
        Index(
            "ix_live_market_ticks_listing_id",
            "listing_id",
            postgresql_where=text("listing_id IS NOT NULL"),
        ),
    )


class IngestionBatch(SQLModel, table=True):
    """Idempotency ledger for listener bulk-ingestion requests."""

    __tablename__: str = "ingestion_batches"

    batch_id: str = Field(primary_key=True, max_length=36)
    source: str = Field(nullable=False)
    record_count: int = Field(nullable=False)
    payload_sha256: str = Field(nullable=False, max_length=64)
    received_at: datetime = Field(
        sa_column_kwargs={"server_default": text("TIMEZONE('utc', NOW())")},
        index=True,
    )


class FeedEvent(SQLModel, table=True):
    """
    Append-only capture of every raw venue feed event (listed, sold, ...), stored verbatim.
    Written through the durable listener batch path, so batch-level idempotency applies.
    """

    __tablename__: str = "feed_events"

    id: int | None = Field(default=None, sa_column=Column(BigIdentity, primary_key=True, autoincrement=True))
    source: str = Field(nullable=False, max_length=32)  # e.g. skinport
    event_type: str = Field(nullable=False, max_length=32)
    received_at: datetime = Field(nullable=False, index=True)  # Edge receive time (UTC)
    payload: dict[str, Any] = Field(sa_column=Column(JsonDocument, nullable=False))


class HistoricalPrice(SQLModel, table=True):
    """
    Data warehouse table holding long-term aggregate historical timelines (Kaggle).
     Optimized with a composite index for fast chronological time-series retrieval.
    """

    __tablename__: str = "historical_prices"

    id: int | None = Field(default=None, primary_key=True)
    item_id: int = Field(foreign_key="market_items.id", ondelete="CASCADE")
    sale_date: datetime = Field(nullable=False)
    median_price_cents: int = Field(nullable=False)
    volume_sold: int = Field(nullable=False)

    # Composite index: binds item_id and sale_date together for fast macro-analytics
    __table_args__ = (Index("ix_historical_prices_item_date", "item_id", "sale_date"),)


class ItemMacroBaseline(SQLModel, table=True):
    """
    Persisted results of the long-term macro trend pipeline, allowing FastAPI
    and the AI reasoning agents to perform dynamic, safety-weighted pricing checks.
    """

    __tablename__: str = "item_macro_baselines"

    id: int | None = Field(default=None, primary_key=True)
    item_id: int = Field(foreign_key="market_items.id", ondelete="CASCADE", unique=True, index=True)

    latest_price_cents: int = Field(nullable=False)
    rolling_30d_avg_cents: int = Field(nullable=False)
    rolling_90d_avg_cents: int = Field(nullable=False)
    drift_percent: float = Field(nullable=False)
    volatility_cents: int = Field(nullable=False)
    avg_volume_30d: float = Field(nullable=False)
    support_floor_cents: int = Field(nullable=False)

    updated_at: datetime = Field(sa_column_kwargs={"server_default": text("TIMEZONE('utc', NOW())")}, index=True)


class SimulatedTrade(SQLModel, table=True):
    """
    Paper trading log for the Deterministic Rules Engine to evaluate strategy profitability.
    """

    __tablename__: str = "simulated_trades"

    id: int | None = Field(default=None, primary_key=True)
    item_id: int = Field(foreign_key="market_items.id", ondelete="CASCADE", index=True)

    purchase_price_cents: int = Field(nullable=False)
    estimated_profit_cents: int = Field(nullable=False)
    trigger_z_score: float = Field(nullable=False)

    # The exact listing that was bought, when the trigger came from a listing-level tick.
    # NULL for trades triggered by REST snapshots and for trades recorded before this column existed.
    listing_id: str | None = Field(default=None, max_length=64)
    float_value: float | None = Field(default=None)  # Float of the bought listing, not of the item in general

    simulated_buy_timestamp: datetime = Field(sa_column_kwargs={"server_default": text("TIMEZONE('utc', NOW())")}, index=True)
