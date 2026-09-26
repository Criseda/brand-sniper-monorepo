import json

from models import MarketTick
from redis.asyncio import Redis
from shared_utils import edge_baselines_key, edge_sticker_prices_key

# Why the DRE approved an opportunity (see dre_approval_reason).
REASON_SUPPORT_FLOOR = "support_floor"
REASON_MACRO_SIGMA = "macro_sigma"
REASON_STICKERS_BELOW_BASE = "stickers_below_base"
REASON_STICKER_PREMIUM = "sticker_premium"
APPROVAL_REASONS = (REASON_SUPPORT_FLOOR, REASON_MACRO_SIGMA, REASON_STICKERS_BELOW_BASE, REASON_STICKER_PREMIUM)


async def dre_approval_reason(tick: MarketTick, redis_client: Redis, baseline: dict | None = None) -> str | None:
    """
    Evaluates a market anomaly deterministically using macro baselines and sticker valuations.
    Returns the rule that approved it, or None when it is rejected.

    Three-tiered guard:
      1.  Hard floor check  (price <= support_floor_cents)
      2.  Volatility-aware macro floor  (price is 2+ sigma below 30d avg)  [Layer 3, #31]
      3.  Sticker Premium Percentage (SP%) logic
    """
    # 1. Fetch the venue's baseline from Redis (or use pre-fetched copy)
    if baseline is None:
        baseline_raw = await redis_client.hget(edge_baselines_key(tick.venue), tick.market_hash_name)
        if not baseline_raw:
            return None
        baseline = json.loads(baseline_raw)

    support_floor_cents = baseline.get("support_floor_cents", 0)
    latest_price_cents = baseline.get("latest_price_cents", 0)
    rolling_30d_avg_cents = baseline.get("rolling_30d_avg_cents")
    volatility_cents = baseline.get("volatility_cents")

    # 2. Hard Floor Check
    if tick.price_cents <= support_floor_cents:
        return REASON_SUPPORT_FLOOR

    # 3. Volatility-Aware Macro Floor  (Layer 3 fix for #31)
    # Catches illiquid items that drop far below their long-term average
    # even when the local sliding window is too sparse for a Z-score.
    if rolling_30d_avg_cents is not None and volatility_cents is not None and volatility_cents > 0:
        sigma_distance = (rolling_30d_avg_cents - tick.price_cents) / volatility_cents
        if sigma_distance >= 2.0:
            return REASON_MACRO_SIGMA

    # 4. Sticker Premium Valuation
    total_sticker_value_cents = 0

    if hasattr(tick, "stickers") and tick.stickers:
        for sticker in tick.stickers:
            name = sticker.get("name")
            if name:
                # Fetch the sticker's price on this venue from the Redis hash
                price_str = await redis_client.hget(edge_sticker_prices_key(tick.venue), name)
                if price_str:
                    try:
                        total_sticker_value_cents += int(price_str)
                    except ValueError:
                        pass

    # 5. Sticker Premium Percentage (SP%) Logic
    if total_sticker_value_cents > 10000:  # Minimum $100 sticker value required to care
        premium_cents = tick.price_cents - latest_price_cents

        # If the premium is negative, we are getting stickers for free below base price
        if premium_cents <= 0:
            return REASON_STICKERS_BELOW_BASE

        sp_percentage = (premium_cents / total_sticker_value_cents) * 100

        if sp_percentage <= 3.0:
            return REASON_STICKER_PREMIUM

    return None


async def evaluate_opportunity(tick: MarketTick, redis_client: Redis, baseline: dict | None = None) -> bool:
    """True when the DRE approves the anomaly (see dre_approval_reason for the rules)."""
    return await dre_approval_reason(tick, redis_client, baseline) is not None
