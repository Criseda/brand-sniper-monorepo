import time
from typing import Any, TypedDict

import aiohttp
from database import session_scope
from shared_utils import detect_downtrend, get_logger, parse_version_from_name, resolve_recent_median, to_cents
from shared_utils.models import HistoricalPrice, ItemMacroBaseline, LiveMarketTick, MarketItem
from sqlalchemy import Integer, String, cast, func, select
from sqlalchemy.engine import Result
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.selectable import Select
from sqlmodel.ext.asyncio.session import AsyncSession

logger = get_logger("backend.queries")


async def _exec_result(session: AsyncSession, stmt: Select[Any]) -> Result[Any]:
    """Runs a SELECT via SQLModel's exec() and returns a plain SQLAlchemy Result.

    exec() only unwraps to a ScalarResult for statements built with SQLModel's
    own select(); for SQLAlchemy-built selects (cast/func/join) it returns a
    normal Result, which is what every caller here consumes. The type-ignore is
    required because SQLModel's exec() overloads only accept its own Select
    subclass.
    """
    return await session.exec(stmt)  # type: ignore[call-overload]


# Shared aiohttp session for backend API requests
_backend_session: aiohttp.ClientSession | None = None


async def close_http_session() -> None:
    """Closes the shared aiohttp session on graceful shutdown."""
    global _backend_session
    if _backend_session is not None and not _backend_session.closed:
        await _backend_session.close()
        _backend_session = None


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
            logger.info("Returning cached sales history for '%s' (version: %s)", market_hash_name, version)
            return cached_entry

    url = "https://api.skinport.com/v1/sales/history"
    params: dict[str, str | int] = {"app_id": 730, "currency": "USD", "market_hash_name": market_hash_name}
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
                logger.warning("Non-200 status fetching history for '%s': %s", market_hash_name, response.status)
    except (aiohttp.ClientError, TimeoutError, ValueError) as e:
        logger.error("Error fetching sales history for '%s': %s", market_hash_name, e)
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
    logger.info("Fallback to database lookup for sticker '%s'", sticker_name)
    try:
        async with session_scope() as session:
            # Resolve item_id for the sticker
            item_stmt = select(cast(MarketItem.id, Integer)).where(cast(MarketItem.market_hash_name, String) == sticker_name)
            item_res = await _exec_result(session, item_stmt)
            item_row = item_res.fetchone()
            if not item_row:
                return None
            sticker_item_id = item_row[0]

            # Query historical average price
            hist_stmt = select(func.avg(HistoricalPrice.median_price_cents)).where(
                cast(HistoricalPrice.item_id, Integer) == sticker_item_id
            )
            hist_res = await _exec_result(session, hist_stmt)
            avg_hist = hist_res.scalar()
            if avg_hist is not None:
                return round(float(avg_hist))

            # Query live ticks average price
            live_stmt = select(func.avg(LiveMarketTick.price_cents)).where(
                cast(LiveMarketTick.item_id, Integer) == sticker_item_id
            )
            live_res = await _exec_result(session, live_stmt)
            avg_live = live_res.scalar()
            if avg_live is not None:
                return round(float(avg_live))
    except SQLAlchemyError as dbe:
        logger.error("Error during sticker database fallback lookup: %s", dbe)

    return None


class MarketContext(TypedDict):
    """Typed shape of the market context returned to clients for a resolved item."""

    historical_steam_avg_cents: int | None
    historical_skinport_avg_cents: int | None
    real_time_skinport_median_cents: int | None
    cash_equivalent_avg_cents: int | None
    snipe_threshold_cents: int | None
    item_type: str
    is_liquid: bool
    avg_volume_30d: float | None
    drift_percent: float
    volatility_cents: int
    support_floor_cents: int | None
    regime_shift_detected: bool
    downtrend_detected: bool
    downtrend_severity: float
    item_page: str | None
    market_page: str | None


async def _resolve_item(
    session: AsyncSession,
    market_hash_name: str,
    base_name: str,
    version: str | None,
) -> tuple[int, str, int] | None:
    """
    Resolves the base item row (id + type) and the versioned item id.
    Falls back to the base item id when no versioned row exists.

    Returns None when the item is unknown or the lookup fails (logged).
    """
    try:
        item_stmt = select(cast(MarketItem.id, Integer), cast(MarketItem.item_type, String)).where(
            cast(MarketItem.market_hash_name, String) == base_name
        )
        item_res = await _exec_result(session, item_stmt)
        item_row = item_res.fetchone()

        if not item_row:
            return None

        base_item_id, item_type = item_row

        versioned_item_id = base_item_id
        if version:
            versioned_stmt = select(cast(MarketItem.id, Integer)).where(
                cast(MarketItem.market_hash_name, String) == market_hash_name
            )
            versioned_res = await _exec_result(session, versioned_stmt)
            versioned_row = versioned_res.fetchone()
            if versioned_row:
                versioned_item_id = versioned_row[0]

        return base_item_id, item_type, versioned_item_id
    except SQLAlchemyError as e:
        logger.error("Error resolving item '%s': %s", market_hash_name, e)
        return None


async def _fetch_steam_baseline(session: AsyncSession, market_hash_name: str, item_id: int) -> float | None:
    """
    Fetches the long-term Steam baseline from historical Kaggle aggregates.
    Returns None when no data exists or the query fails (logged).
    """
    try:
        stmt = select(func.avg(HistoricalPrice.median_price_cents)).where(cast(HistoricalPrice.item_id, Integer) == item_id)
        res = await _exec_result(session, stmt)
        raw = res.scalar()
        return float(raw) if raw is not None else None
    except SQLAlchemyError as e:
        logger.error("Error fetching Steam baseline for '%s': %s", market_hash_name, e)
        return None


async def _fetch_skinport_baseline(session: AsyncSession, market_hash_name: str, item_id: int) -> float | None:
    """
    Fetches the recent stable Skinport cash baseline from live market ticks.
    Returns None when no data exists or the query fails (logged).
    """
    try:
        stmt = select(func.avg(LiveMarketTick.price_cents)).where(
            cast(LiveMarketTick.item_id, Integer) == item_id,
            cast(LiveMarketTick.marketplace_source, String) == "skinport",
        )
        res = await _exec_result(session, stmt)
        raw = res.scalar()
        return float(raw) if raw is not None else None
    except SQLAlchemyError as e:
        logger.error("Error fetching Skinport baseline for '%s': %s", market_hash_name, e)
        return None


async def _fetch_macro_baseline(session: AsyncSession, market_hash_name: str, item_id: int) -> ItemMacroBaseline | None:
    """
    Fetches the persisted macro baseline metrics for an item.
    Returns None when no baseline exists or the query fails (logged).
    """
    try:
        stmt = select(ItemMacroBaseline).where(cast(ItemMacroBaseline.item_id, Integer) == item_id)
        res = await _exec_result(session, stmt)
        return res.scalar_one_or_none()
    except SQLAlchemyError as e:
        logger.error("Error fetching macro baseline for '%s': %s", market_hash_name, e)
        return None


def _compute_discount_corridor(item_type: str) -> float:
    """Returns the market-specific cash discount factor relative to Steam list price."""
    if item_type in ["Knife", "Glove"]:
        return 0.25  # Knives/Gloves trade at lower cash discounts
    if item_type in ["Sticker", "Patch"]:
        return 0.35  # Cosmetic items carry higher cash discounts
    return 0.30  # Default baseline discount


def _apply_liquidity_guardrails(macro_baseline: ItemMacroBaseline | None, item_type: str) -> tuple[bool, float | None]:
    """
    Applies the liquidity guardrail (scaled by asset value / class).

    High-tier items/Knives/Gloves (> $150 or type Knives/Gloves) only require
    a 0.05 daily sales floor (1 sale every 20 days). Low-tier items (< $150)
    must be actively traded and require at least a 0.5 sales/day floor.
    """
    if macro_baseline is None:
        return True, None

    avg_volume_30d = macro_baseline.avg_volume_30d
    latest_price = macro_baseline.latest_price_cents

    if latest_price > 15000 or item_type in ["Knife", "Glove"]:
        liquidity_floor = 0.05
    else:
        liquidity_floor = 0.5

    is_liquid = not (avg_volume_30d is not None and avg_volume_30d < liquidity_floor)
    return is_liquid, avg_volume_30d


def _detect_regime_shift(
    avg_steam: float | None,
    baseline_comparison: float | None,
    discount_factor: float,
) -> tuple[bool, bool]:
    """
    Concept drift guardrail (regime shift detection, e.g., 2025 knife trade-ups update).

    Returns (regime_shift_detected, use_steam_baseline). If the historical Steam
    baseline cash-value deviates from recent prices by >35%, assume historical
    averages are drift-corrupted and fall back.
    """
    if avg_steam is None or baseline_comparison is None:
        return False, True

    expected_cash_steam = avg_steam * (1.0 - discount_factor)
    deviation = abs(expected_cash_steam - baseline_comparison) / baseline_comparison if baseline_comparison > 0 else 0

    if deviation > 0.35:
        return True, False
    return False, True


def _analyze_real_time_history(history: dict) -> tuple[int | None, bool, float]:
    """
    Extracts the real-time Skinport median and active downtrend signal
    from a sales history entry.
    """
    real_time_median_cents = None
    downtrend_detected = False
    downtrend_severity = 0.0

    if history:
        median_usd = resolve_recent_median(history)
        if median_usd is not None:
            real_time_median_cents = to_cents(median_usd)

        downtrend_detected, downtrend_severity = detect_downtrend(history)

    return real_time_median_cents, downtrend_detected, downtrend_severity


def _compute_snipe_threshold(
    cash_equivalent_avg_cents: int,
    downtrend_detected: bool,
    downtrend_severity: float,
) -> int:
    """
    Computes the snipe threshold. Base is 15% discount (factor of 0.85).
    If price is actively downtrending, require a steeper discount corridor
    (up to 30% discount / 0.70 factor).
    """
    base_discount = 0.85
    if downtrend_detected:
        penalty = min(0.15, downtrend_severity)
        applied_discount = base_discount - penalty
    else:
        applied_discount = base_discount
    return round(cash_equivalent_avg_cents * applied_discount)


async def get_item_market_context(market_hash_name: str) -> MarketContext | None:
    """
    Queries historical data, macro baselines, and live API sales history,
    applying liquidity checks, cash corridors, and active downtrend penalties
    to protect trading capital from structural price crashes (e.g. 2025 updates).

    Returns None when the item cannot be resolved (unknown item or lookup
    failure) so callers can surface a not-found response instead of silently
    reading an empty context. Each data-fetch sub-step degrades gracefully:
    failures are logged inside the step and the remaining sources still
    produce a partial context, so a downstream outage never results in a 500
    for the caller.
    """
    base_name, version = parse_version_from_name(market_hash_name)

    async with session_scope() as session:
        resolved = await _resolve_item(session, market_hash_name, base_name, version)
        if resolved is None:
            return None

        base_item_id, item_type, versioned_item_id = resolved

        avg_steam = await _fetch_steam_baseline(session, market_hash_name, base_item_id)
        avg_skinport = await _fetch_skinport_baseline(session, market_hash_name, versioned_item_id)
        macro_baseline = await _fetch_macro_baseline(session, market_hash_name, versioned_item_id)

    skinport_history = await fetch_skinport_sales_history(base_name, version)
    real_time_median_cents, downtrend_detected, downtrend_severity = _analyze_real_time_history(skinport_history)

    discount_factor = _compute_discount_corridor(item_type)
    is_liquid, avg_volume_30d = _apply_liquidity_guardrails(macro_baseline, item_type)

    # Compare Steam cash-equivalent against active live averages
    baseline_comparison = real_time_median_cents if real_time_median_cents is not None else avg_skinport
    regime_shift_detected, use_steam_baseline = _detect_regime_shift(avg_steam, baseline_comparison, discount_factor)

    cash_equivalent_avg_cents = None
    snipe_threshold_cents = None

    if is_liquid:
        if real_time_median_cents is not None:
            # Real-time Skinport median is our most accurate cash baseline reference
            cash_equivalent_avg_cents = real_time_median_cents
        elif avg_steam is not None and use_steam_baseline:
            cash_equivalent_avg_cents = round(avg_steam * (1.0 - discount_factor))
        elif avg_skinport is not None:
            cash_equivalent_avg_cents = round(avg_skinport)

        if cash_equivalent_avg_cents is not None:
            snipe_threshold_cents = _compute_snipe_threshold(cash_equivalent_avg_cents, downtrend_detected, downtrend_severity)

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


async def search_macro_trends(query: str) -> list[dict]:
    """
    Searches tracked items matching the query and returns their macro baseline data.
    """
    if not query:
        return []

    async with session_scope() as session:
        stmt = (
            select(cast(MarketItem.market_hash_name, String), ItemMacroBaseline)
            .join(
                ItemMacroBaseline,
                cast(MarketItem.id, Integer) == cast(ItemMacroBaseline.item_id, Integer),
                isouter=True,
            )
            .where(func.lower(cast(MarketItem.market_hash_name, String)).contains(query.lower()))
            .limit(10)
        )
        result = await _exec_result(session, stmt)
        rows = result.fetchall()

    if not rows:
        return []

    results = []
    for name, baseline in rows:
        if baseline is None:
            continue
        results.append(
            {
                "market_hash_name": name,
                "drift_percent": baseline.drift_percent,
                "volatility_cents": baseline.volatility_cents,
                "avg_volume_30d": baseline.avg_volume_30d,
                "support_floor_cents": baseline.support_floor_cents,
                "latest_price_cents": baseline.latest_price_cents,
            }
        )

    return results
