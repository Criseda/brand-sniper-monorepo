from typing import Optional
from datetime import datetime
from sqlmodel import Field, SQLModel, Index
from sqlalchemy import text

class MarketItem(SQLModel, table=True):
    """
    Master reference directory for all tracked digital assets.
    """
    __tablename__: str = "market_items"

    id: Optional[int] = Field(default=None, primary_key=True)
    market_hash_name: str = Field(index=True, unique=True, nullable=False)
    item_type: str = Field(index=True)                  # e.g., Knife, Rifle, Glove
    rarity: Optional[str] = Field(default=None)         # e.g., Covert, Classified


class LiveMarketTick(SQLModel, table=True):
    """
    High-velocity storage tracking real-time price updates from live endpoints.
    """
    __tablename__: str = "live_market_ticks"

    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Added ondelete="CASCADE" so if an item is deleted, its ticks clear out automatically
    item_id: int = Field(foreign_key="market_items.id", ondelete="CASCADE", index=True)
    
    price_cents: int = Field(nullable=False) 
    marketplace_source: str = Field(default="steam")   # e.g., steam, csfloat, skinport
    
    float_value: Optional[float] = Field(default=None, index=True)  # Exact item wear (0.0 - 1.0)
    paint_index: Optional[int] = Field(default=None)                # Pattern identifier
    
    # Let the PostgreSQL server safely generate the UTC timestamp natively
    inserted_at: datetime = Field(
        sa_column_kwargs={"server_default": text("TIMEZONE('utc', NOW())")},
        index=True
    )


class HistoricalPrice(SQLModel, table=True):
    """
    Data warehouse table holding long-term aggregate historical timelines (Kaggle).
     Optimized with a composite index for fast chronological time-series retrieval.
    """
    __tablename__: str = "historical_prices"

    id: Optional[int] = Field(default=None, primary_key=True)
    item_id: int = Field(foreign_key="market_items.id", ondelete="CASCADE")
    sale_date: datetime = Field(nullable=False)
    median_price_cents: int = Field(nullable=False)
    volume_sold: int = Field(nullable=False)

    # Composite index: binds item_id and sale_date together for fast macro-analytics
    __table_args__ = (
        Index("ix_historical_prices_item_date", "item_id", "sale_date"),
    )