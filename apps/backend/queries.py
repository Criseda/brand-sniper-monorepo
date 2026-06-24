import aiohttp
from sqlalchemy import select, func
from database import AsyncSessionLocal
from shared_utils.models import MarketItem, HistoricalPrice, LiveMarketTick, ItemMacroBaseline
from shared_utils import parse_version_from_name

async def fetch_skinport_sales_history(market_hash_name: str, version: str | None = None) -> dict:
    """
    Queries the Skinport Sales History API (/v1/sales/history) for a specific item.
    Uses 'Accept-Encoding': 'br' Brotli encoding as required by the endpoint.
    Filters the returned list by the version string if provided, returning the matching dict.
    Returns the first item dictionary from the response list, or an empty dict on failure.
    """
    url = "https://api.skinport.com/v1/sales/history"
    params = {
        "app_id": 730,
        "currency": "USD",
        "market_hash_name": market_hash_name
    }
    headers = {
        "Accept-Encoding": "br"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers, timeout=5.0) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list) and len(data) > 0:
                        if version:
                            for entry in data:
                                if entry.get("version") == version:
                                    return entry
                        return data[0]
                else:
                    print(f"[SKINPORT API] Non-200 status fetching history for '{market_hash_name}': {response.status}")
    except Exception as e:
        print(f"[SKINPORT API] Error fetching sales history for '{market_hash_name}': {e}")
    return {}

async def get_item_market_context(market_hash_name: str) -> dict:
    """
    Queries historical data, macro baselines, and live API sales history,
    applying liquidity checks, cash corridors, and active downtrend penalties
    to protect trading capital from structural price crashes (e.g. 2025 updates).
    """
    base_name, version = parse_version_from_name(market_hash_name)
    
    async with AsyncSessionLocal() as session:
        # 1. Resolve the item_id and type first (query base name for metadata/steam defaults)
        item_stmt = (
            select(MarketItem.id, MarketItem.item_type)
            .where(MarketItem.market_hash_name == base_name)
        )
        item_res = await session.execute(item_stmt)
        item_row = item_res.fetchone()
        
        if not item_row:
            return {}
            
        base_item_id, item_type = item_row
        
        # Resolve specific versioned item_id if it exists, to leverage clean versioned ticks and baselines
        versioned_item_id = base_item_id
        if version:
            versioned_stmt = (
                select(MarketItem.id)
                .where(MarketItem.market_hash_name == market_hash_name)
            )
            versioned_res = await session.execute(versioned_stmt)
            versioned_row = versioned_res.fetchone()
            if versioned_row:
                versioned_item_id = versioned_row[0]
        
        # 2. Fetch the long-term Steam baseline from historical Kaggle aggregates (using base_item_id)
        steam_stmt = (
            select(func.avg(HistoricalPrice.median_price_cents))
            .where(HistoricalPrice.item_id == base_item_id)
        )
        
        # 3. Fetch the recent stable Skinport cash baseline from live market ticks (using versioned_item_id)
        skinport_stmt = (
            select(func.avg(LiveMarketTick.price_cents))
            .where(
                LiveMarketTick.item_id == versioned_item_id,
                LiveMarketTick.marketplace_source == "skinport"
            )
        )
        
        # 4. Fetch the persisted macro baseline metrics (using versioned_item_id)
        macro_stmt = (
            select(ItemMacroBaseline)
            .where(ItemMacroBaseline.item_id == versioned_item_id)
        )
        
        steam_res = await session.execute(steam_stmt)
        skinport_res = await session.execute(skinport_stmt)
        macro_res = await session.execute(macro_stmt)
        
        raw_steam = steam_res.scalar()
        raw_skinport = skinport_res.scalar()
        macro_baseline = macro_res.scalar_one_or_none()
        
        avg_steam = float(raw_steam) if raw_steam is not None else None
        avg_skinport = float(raw_skinport) if raw_skinport is not None else None
        
        # 5. Fetch real-time sales history from Skinport API (using base name and version)
        skinport_history = await fetch_skinport_sales_history(base_name, version)
        
        # 6. Analyze real-time sales history for active downtrends or values
        real_time_median_cents = None
        downtrend_detected = False
        downtrend_severity = 0.0
        
        if skinport_history:
            h24 = skinport_history.get("last_24_hours", {})
            h7 = skinport_history.get("last_7_days", {})
            h30 = skinport_history.get("last_30_days", {})
            h90 = skinport_history.get("last_90_days", {})
            
            def to_cents(val):
                return round(float(val) * 100) if val is not None else None
                
            m24 = to_cents(h24.get("median"))
            m7 = to_cents(h7.get("median"))
            m30 = to_cents(h30.get("median"))
            m90 = to_cents(h90.get("median"))
            
            # Resolve recent median (with active volume)
            if m24 and h24.get("volume", 0) > 0:
                real_time_median_cents = m24
            elif m7 and h7.get("volume", 0) > 0:
                real_time_median_cents = m7
            elif m30 and h30.get("volume", 0) > 0:
                real_time_median_cents = m30
            else:
                real_time_median_cents = m90
                
            # Downtrend check (compare 7-day median vs 30-day, and check 24h vs 7d for short-term panic)
            ref_recent = m7 if m7 else m24
            ref_older = m30 if m30 else m90
            
            if ref_recent and ref_older and ref_recent < ref_older:
                downtrend_detected = True
                downtrend_severity += (ref_older - ref_recent) / ref_older
                
            # Short-term panic check (24h median lower than 7-day average)
            if m24 and m7 and m24 < m7:
                downtrend_detected = True
                downtrend_severity += (m7 - m24) / m7
        
        # 7. Apply market-specific cash discount corridors (relative to Steam list price)
        if item_type in ["Knife", "Glove"]:
            discount_factor = 0.25  # Knives/Gloves trade at lower cash discounts
        elif item_type in ["Sticker", "Patch"]:
            discount_factor = 0.35  # Cosmetic items carry higher cash discounts
        else:
            discount_factor = 0.30  # Default baseline discount
            
        cash_equivalent_avg_cents = None
        snipe_threshold_cents = None
        
        # Liquidity guardrail (scaled by asset value / class)
        is_liquid = True
        avg_volume_30d = None
        if macro_baseline is not None:
            avg_volume_30d = macro_baseline.avg_volume_30d
            latest_price = macro_baseline.latest_price_cents
            
            # High-tier items/Knives/Gloves (> $150 or type Knives/Gloves) only require a 0.05 daily sales floor (1 sale every 20 days).
            # Low-tier items (< $150) must be actively traded and require at least a 0.5 sales/day floor.
            if latest_price > 15000 or item_type in ["Knife", "Glove"]:
                liquidity_floor = 0.05
            else:
                liquidity_floor = 0.5
                
            if avg_volume_30d is not None and avg_volume_30d < liquidity_floor:
                is_liquid = False

        # Concept drift guardrail (regime shift detection, e.g., 2025 knife trade-ups update)
        regime_shift_detected = False
        use_steam_baseline = True

        # Compare Steam cash-equivalent against active live averages
        baseline_comparison = real_time_median_cents if real_time_median_cents is not None else avg_skinport
        
        if avg_steam is not None and baseline_comparison is not None:
            expected_cash_steam = avg_steam * (1.0 - discount_factor)
            deviation = abs(expected_cash_steam - baseline_comparison) / baseline_comparison if baseline_comparison > 0 else 0
            
            # If the historical Steam baseline cash-value deviates from recent prices by >35%,
            # assume historical averages are drift-corrupted and fall back.
            if deviation > 0.35:
                regime_shift_detected = True
                use_steam_baseline = False

        # Calculate final baselines and purchase thresholds
        if is_liquid:
            if real_time_median_cents is not None:
                # Real-time Skinport median is our most accurate cash baseline reference
                cash_equivalent_avg_cents = real_time_median_cents
            elif avg_steam is not None and use_steam_baseline:
                cash_equivalent_avg_cents = round(avg_steam * (1.0 - discount_factor))
            elif avg_skinport is not None:
                cash_equivalent_avg_cents = round(avg_skinport)
                
            if cash_equivalent_avg_cents is not None:
                # Base is 15% discount (factor of 0.85)
                # If price is actively downtrending, require a steeper discount corridor (up to 30% discount / 0.70 factor)
                base_discount = 0.85
                if downtrend_detected:
                    penalty = min(0.15, downtrend_severity)
                    applied_discount = base_discount - penalty
                else:
                    applied_discount = base_discount
                    
                snipe_threshold_cents = round(cash_equivalent_avg_cents * applied_discount)
            
        return {
            "historical_steam_avg_cents": round(avg_steam) if avg_steam is not None else None,
            "historical_skinport_avg_cents": round(avg_skinport) if avg_skinport is not None else None,
            "real_time_skinport_median_cents": real_time_median_cents,
            "cash_equivalent_avg_cents": cash_equivalent_avg_cents,
            "snipe_threshold_cents": snipe_threshold_cents,
            "item_type": item_type,
            "is_liquid": is_liquid,
            "avg_volume_30d": avg_volume_30d,
            "drift_percent": macro_baseline.drift_percent if macro_baseline else 0.0,
            "volatility_cents": macro_baseline.volatility_cents if macro_baseline else 0,
            "support_floor_cents": macro_baseline.support_floor_cents if macro_baseline else None,
            "regime_shift_detected": regime_shift_detected,
            "downtrend_detected": downtrend_detected,
            "downtrend_severity": downtrend_severity
        }