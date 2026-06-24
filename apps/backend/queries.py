from sqlalchemy import select, func
from database import AsyncSessionLocal
from shared_utils.models import MarketItem, HistoricalPrice, LiveMarketTick, ItemMacroBaseline

async def get_item_market_context(market_hash_name: str) -> dict:
    """
    Queries historical data and macro baselines across normalized models,
    applying liquidity checks and cash corridors with dynamic fallbacks
    for structural market regime shifts (e.g., the 2025 trade-up update).
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
        
        # 4. Fetch the persisted macro baseline metrics
        macro_stmt = (
            select(ItemMacroBaseline)
            .where(ItemMacroBaseline.item_id == item_id)
        )
        
        steam_res = await session.execute(steam_stmt)
        skinport_res = await session.execute(skinport_stmt)
        macro_res = await session.execute(macro_stmt)
        
        raw_steam = steam_res.scalar()
        raw_skinport = skinport_res.scalar()
        macro_baseline = macro_res.scalar_one_or_none()
        
        avg_steam = float(raw_steam) if raw_steam is not None else None
        avg_skinport = float(raw_skinport) if raw_skinport is not None else None
        
        # 5. Apply market-specific cash discount corridors (relative to Steam list price)
        if item_type in ["Knife", "Glove"]:
            discount_factor = 0.25  # Knives/Gloves trade at lower cash discounts
        elif item_type in ["Sticker", "Patch"]:
            discount_factor = 0.35  # Cosmetic items carry higher cash discounts
        else:
            discount_factor = 0.30  # Default baseline discount
            
        cash_equivalent_avg_cents = None
        snipe_threshold_cents = None
        
        # Liquidity guardrail
        is_liquid = True
        avg_volume_30d = None
        if macro_baseline is not None:
            avg_volume_30d = macro_baseline.avg_volume_30d
            # If the item has extremely low daily sales (e.g. less than 0.5 units/day), mark as illiquid
            if avg_volume_30d is not None and avg_volume_30d < 0.5:
                is_liquid = False

        # Concept drift guardrail (regime shift detection, e.g., 2025 knife trade-ups update)
        regime_shift_detected = False
        use_steam_baseline = True

        if avg_steam is not None and avg_skinport is not None:
            expected_cash_steam = avg_steam * (1.0 - discount_factor)
            deviation = abs(expected_cash_steam - avg_skinport) / avg_skinport if avg_skinport > 0 else 0
            
            # If the historical Steam baseline cash-value deviates from recent live Skinport prices by >35%,
            # assume historical averages are drift-corrupted and fall back to the live Skinport baseline.
            if deviation > 0.35:
                regime_shift_detected = True
                use_steam_baseline = False

        # Calculate final baselines and purchase thresholds
        if is_liquid:
            if avg_steam is not None and use_steam_baseline:
                cash_equivalent_avg_cents = round(avg_steam * (1.0 - discount_factor))
                # Snipe trigger if price is 15% below the cash equivalent
                snipe_threshold_cents = round(cash_equivalent_avg_cents * 0.85)
            elif avg_skinport is not None:
                # Fallback to direct Skinport live average (no discount needed as Skinport is already cash-value)
                cash_equivalent_avg_cents = round(avg_skinport)
                snipe_threshold_cents = round(cash_equivalent_avg_cents * 0.85)
            
        return {
            "historical_steam_avg_cents": round(avg_steam) if avg_steam is not None else None,
            "historical_skinport_avg_cents": round(avg_skinport) if avg_skinport is not None else None,
            "cash_equivalent_avg_cents": cash_equivalent_avg_cents,
            "snipe_threshold_cents": snipe_threshold_cents,
            "item_type": item_type,
            "is_liquid": is_liquid,
            "avg_volume_30d": avg_volume_30d,
            "drift_percent": macro_baseline.drift_percent if macro_baseline else 0.0,
            "volatility_cents": macro_baseline.volatility_cents if macro_baseline else 0,
            "support_floor_cents": macro_baseline.support_floor_cents if macro_baseline else None,
            "regime_shift_detected": regime_shift_detected
        }