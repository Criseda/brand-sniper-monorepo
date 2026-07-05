import time

import aiohttp
from database import AsyncSessionLocal
from shared_utils import detect_downtrend, parse_version_from_name, resolve_recent_median, to_cents
from shared_utils.models import HistoricalPrice, ItemMacroBaseline, LiveMarketTick, MarketItem
from sqlalchemy import func, select

# Shared aiohttp session for backend API requests
_backend_session: aiohttp.ClientSession | None = None


async def _get_session() -> aiohttp.ClientSession:
    """Returns a shared aiohttp session for backend API calls."""
    global _backend_session
    if _backend_session is None or _backend_session.closed:
        _backend_session = aiohttp.ClientSession(
            headers={"Accept-Encoding": "br"}, timeout=aiohttp.ClientTimeout(total=10, connect=3)
        )
    return _backend_session


# Global in-memory cache to prevent sales history API rate limit exhaustion
# Map: (market_hash_name, version) -> (inserted_timestamp, entry_dict)
sales_history_cache: dict[tuple[str, str | None], tuple[float, dict]] = {}
CACHE_TTL_SECONDS = 600  # 10 minutes cache


async def fetch_skinport_sales_history(market_hash_name: str, version: str | None = None) -> dict:
    """
    Queries the Skinport Sales History API (/v1/sales/history) for a specific item.
    Uses 'Accept-Encoding': 'br' Brotli encoding as required by the endpoint.
    Filters the returned list by the version string if provided, returning the matching dict.
    Returns the first item dictionary from the response list, or an empty dict on failure.
    """
    now = time.time()
    cache_key = (market_hash_name, version)
    if cache_key in sales_history_cache:
        ts, cached_entry = sales_history_cache[cache_key]
        if now - ts < CACHE_TTL_SECONDS:
            print(f"[SKINPORT API] Returning cached sales history for '{market_hash_name}' (version: {version})")
            return cached_entry

    url = "https://api.skinport.com/v1/sales/history"
    params = {"app_id": 730, "currency": "USD", "market_hash_name": market_hash_name}
    try:
        session = await _get_session()
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5, connect=2)) as response:
            if response.status == 200:
                data = await response.json()
                if isinstance(data, list) and len(data) > 0:
                    resolved_entry = data[0]
                    if version:
                        for entry in data:
                            if entry.get("version") == version:
                                resolved_entry = entry
                                break

                    # Store in cache with current timestamp
                    sales_history_cache[cache_key] = (now, resolved_entry)
                    return resolved_entry
            else:
                print(f"[SKINPORT API] Non-200 status fetching history for '{market_hash_name}': {response.status}")
    except Exception as e:
        print(f"[SKINPORT API] Error fetching sales history for '{market_hash_name}': {e}")
    return {}


async def get_sticker_price_cents(sticker_name: str) -> int | None:
    """
    Resolves the real-time average price of a sticker (in USD cents).
    First tries to fetch the live Sales History API (which resolves age discrepancies),
    then falls back to querying the database market_items and historical_prices tables.
    Uses caching (via sales_history_cache) to honor the 8 requests/5 mins rate limit.
    """
    # 1. Try to fetch from live Skinport history API (automatically caches)
    history = await fetch_skinport_sales_history(sticker_name)
    if history:
        median_usd = resolve_recent_median(history)
        if median_usd is not None:
            return to_cents(median_usd)

    # 2. Database Fallback (if API is rate-limited or offline)
    print(f"[SKINPORT API] Fallback to database lookup for sticker '{sticker_name}'")
    try:
        async with AsyncSessionLocal() as session:
            # Resolve item_id for the sticker
            item_stmt = select(MarketItem.id).where(MarketItem.market_hash_name == sticker_name)
            item_res = await session.execute(item_stmt)
            item_row = item_res.fetchone()
            if not item_row:
                return None
            sticker_item_id = item_row[0]

            # Query historical average price
            hist_stmt = select(func.avg(HistoricalPrice.median_price_cents)).where(HistoricalPrice.item_id == sticker_item_id)
            hist_res = await session.execute(hist_stmt)
            avg_hist = hist_res.scalar()
            if avg_hist is not None:
                return round(float(avg_hist))

            # Query live ticks average price
            live_stmt = select(func.avg(LiveMarketTick.price_cents)).where(LiveMarketTick.item_id == sticker_item_id)
            live_res = await session.execute(live_stmt)
            avg_live = live_res.scalar()
            if avg_live is not None:
                return round(float(avg_live))
    except Exception as dbe:
        print(f"[DATABASE] Error during sticker database fallback lookup: {dbe}")

    return None


async def get_item_market_context(market_hash_name: str) -> dict:
    """
    Queries historical data, macro baselines, and live API sales history,
    applying liquidity checks, cash corridors, and active downtrend penalties
    to protect trading capital from structural price crashes (e.g. 2025 updates).
    """
    base_name, version = parse_version_from_name(market_hash_name)

    async with AsyncSessionLocal() as session:
        # 1. Resolve the item_id and type first (query base name for metadata/steam defaults)
        item_stmt = select(MarketItem.id, MarketItem.item_type).where(MarketItem.market_hash_name == base_name)
        item_res = await session.execute(item_stmt)
        item_row = item_res.fetchone()

        if not item_row:
            return {}

        base_item_id, item_type = item_row

        # Resolve specific versioned item_id if it exists, to leverage clean versioned ticks and baselines
        versioned_item_id = base_item_id
        if version:
            versioned_stmt = select(MarketItem.id).where(MarketItem.market_hash_name == market_hash_name)
            versioned_res = await session.execute(versioned_stmt)
            versioned_row = versioned_res.fetchone()
            if versioned_row:
                versioned_item_id = versioned_row[0]

        # 2. Fetch the long-term Steam baseline from historical Kaggle aggregates (using base_item_id)
        steam_stmt = select(func.avg(HistoricalPrice.median_price_cents)).where(HistoricalPrice.item_id == base_item_id)

        # 3. Fetch the recent stable Skinport cash baseline from live market ticks (using versioned_item_id)
        skinport_stmt = select(func.avg(LiveMarketTick.price_cents)).where(
            LiveMarketTick.item_id == versioned_item_id, LiveMarketTick.marketplace_source == "skinport"
        )

        # 4. Fetch the persisted macro baseline metrics (using versioned_item_id)
        macro_stmt = select(ItemMacroBaseline).where(ItemMacroBaseline.item_id == versioned_item_id)

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
            median_usd = resolve_recent_median(skinport_history)
            if median_usd is not None:
                real_time_median_cents = to_cents(median_usd)

            downtrend_detected, downtrend_severity = detect_downtrend(skinport_history)

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

            # High-tier items/Knives/Gloves (> $150 or type Knives/Gloves)
            # only require a 0.05 daily sales floor (1 sale every 20 days).
            # Low-tier items (< $150) must be actively traded and require
            # at least a 0.5 sales/day floor.
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
            "downtrend_severity": downtrend_severity,
            "item_page": skinport_history.get("item_page") if skinport_history else None,
            "market_page": skinport_history.get("market_page") if skinport_history else None,
        }
