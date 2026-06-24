from sqlalchemy import select, func
from database import AsyncSessionLocal
from shared_utils.models import MarketItem, HistoricalPrice, LiveMarketTick

async def get_item_market_context(market_hash_name: str) -> dict:
    """
    Queries historical data across normalized models and applies cash 
    discount corridors to establish baseline premiums for an asset.
    """
    async with AsyncSessionLocal() as session:
        # 1. Resolve the item_id and type first to avoid costly joins on the 105M-row table
        item_stmt = (
            select(MarketItem.id, MarketItem.item_type)
            .where(MarketItem.market_hash_name == market_hash_name)
        )
        item_res = await session.execute(item_stmt)
        item_row = item_res.fetchone()
        
        if not item_row:
            return {}
            
        item_id, item_type = item_row
        
        # 2. Fetch the long-term Steam baseline from historical Kaggle aggregates
        steam_stmt = (
            select(func.avg(HistoricalPrice.median_price_cents))
            .where(HistoricalPrice.item_id == item_id)
        )
        
        # 3. Fetch the recent stable Skinport cash baseline from live market ticks
        skinport_stmt = (
            select(func.avg(LiveMarketTick.price_cents))
            .where(
                LiveMarketTick.item_id == item_id,
                LiveMarketTick.marketplace_source == "skinport"
            )
        )
        
        steam_res = await session.execute(steam_stmt)
        skinport_res = await session.execute(skinport_stmt)
        
        raw_steam = steam_res.scalar()
        raw_skinport = skinport_res.scalar()
        
        avg_steam = float(raw_steam) if raw_steam is not None else None
        avg_skinport = float(raw_skinport) if raw_skinport is not None else None
        
        # 4. Apply market-specific cash discount corridors (relative to Steam list price)
        if item_type in ["Knife", "Glove"]:
            discount_factor = 0.25  # Knives/Gloves trade at lower cash discounts
        elif item_type in ["Sticker", "Patch"]:
            discount_factor = 0.35  # Cosmetic items carry higher cash discounts
        else:
            discount_factor = 0.30  # Default baseline discount
            
        cash_equivalent_avg_cents = None
        snipe_threshold_cents = None
        
        if avg_steam is not None:
            cash_equivalent_avg_cents = round(avg_steam * (1.0 - discount_factor))
            # Flag a snipe opportunity if the live listing sits 15% below the cash baseline
            snipe_threshold_cents = round(cash_equivalent_avg_cents * 0.85)
            
        return {
            "historical_steam_avg_cents": round(avg_steam) if avg_steam is not None else None,
            "historical_skinport_avg_cents": round(avg_skinport) if avg_skinport is not None else None,
            "cash_equivalent_avg_cents": cash_equivalent_avg_cents,
            "snipe_threshold_cents": snipe_threshold_cents,
            "item_type": item_type
        }