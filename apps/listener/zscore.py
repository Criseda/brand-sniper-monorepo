import math
import os

from models import MarketTick

# --- Tunable Detection Parameters (configurable via .env) ---
MIN_SAVINGS_USD = float(os.getenv("MIN_SAVINGS_USD", "0.50"))
MIN_SAVINGS_CENTS = round(MIN_SAVINGS_USD * 100)
Z_SCORE_THRESHOLD = float(os.getenv("Z_SCORE_THRESHOLD", "-2.0"))
Z_SCORE_STICKER_THRESHOLD = float(os.getenv("Z_SCORE_STICKER_THRESHOLD", "-1.0"))
MIN_STD_DEV_FACTOR = float(os.getenv("MIN_STD_DEV_FACTOR", "0.04"))
MIN_HISTORY_POINTS = int(os.getenv("MIN_HISTORY_POINTS", "4"))
MACRO_ZSCORE_FALLBACK = os.getenv("MACRO_ZSCORE_FALLBACK", "true").lower() in ("true", "1", "yes")
MACRO_PRIOR_WEIGHT = float(os.getenv("MACRO_PRIOR_WEIGHT", "5.0"))


def calculate_z_score(
    prices: list[int],
    macro_rolling_avg_cents: int | None = None,
    macro_volatility_cents: int | None = None,
    macro_cv: float | None = None,
) -> tuple[float, float, str] | None:
    """
    Calculates the Z-score of the most recent price against the historical window.

    Uses Bayesian shrinkage to blend local and macro volatility for robust
    detection even on illiquid items. Returns (z_score, mean_cents, source)
    where source is 'local', 'hybrid', or 'macro', or None if insufficient data.

    When local data is scarce (< MIN_HISTORY_POINTS) and macro params exist,
    falls back to a macro Z-score using long-term volatility (Layer 1 fix for #31).
    When local data exists, blends estimates using a Bayesian prior (Layer 2).
    """
    macro_available = (
        MACRO_ZSCORE_FALLBACK
        and macro_rolling_avg_cents is not None
        and macro_volatility_cents is not None
        and macro_volatility_cents > 0
        and macro_rolling_avg_cents > 0
    )
    current_tick_price = prices[-1]

    # Layer 1: Macro fallback when local window is too sparse
    if len(prices) < MIN_HISTORY_POINTS:
        if macro_available and macro_rolling_avg_cents is not None and macro_volatility_cents is not None:
            min_vol = max(macro_volatility_cents, macro_rolling_avg_cents * 0.01)
            z_score = (current_tick_price - macro_rolling_avg_cents) / min_vol
            return z_score, float(macro_rolling_avg_cents), "macro"
        return None

    # Layer 2: Local + Bayesian shrinkage hybrid
    historical_prices = prices[:-1]
    n = len(historical_prices)

    mean_cents = sum(historical_prices) / n
    variance = sum((x - mean_cents) ** 2 for x in historical_prices) / (n - 1) if n > 1 else 0.0
    std_dev = math.sqrt(variance)

    if macro_available and macro_cv is not None and macro_cv > 0:
        # Bayesian shrinkage: blend local stddev toward macro prior
        macro_std_estimate = mean_cents * macro_cv
        blended_variance = (n * variance + MACRO_PRIOR_WEIGHT * macro_std_estimate**2) / (n + MACRO_PRIOR_WEIGHT)
        effective_std_dev = math.sqrt(blended_variance)
        source = "hybrid"
    else:
        # Fall back to MIN_STD_DEV_FACTOR regularization when no macro prior
        effective_std_dev = max(std_dev, mean_cents * MIN_STD_DEV_FACTOR)
        source = "local"

    z_score = (current_tick_price - mean_cents) / effective_std_dev
    return z_score, mean_cents, source


def should_trigger_anomaly(z_score: float, mean_cents: float, tick: MarketTick, source: str = "local") -> bool:
    """
    Determines if a Z-score outlier should proceed to history verification,
    applying sticker-aware thresholds and the absolute savings floor.
    source is logged for observability but does not alter thresholds.
    """
    sticker_count = len(tick.stickers)
    threshold_z = Z_SCORE_STICKER_THRESHOLD if sticker_count > 0 else Z_SCORE_THRESHOLD

    if z_score >= threshold_z:
        return False

    # Enforce absolute savings floor on non-stickered items to filter micro-value spam
    if sticker_count == 0:
        savings_cents = mean_cents - tick.price_cents
        if savings_cents < MIN_SAVINGS_CENTS:
            return False

    return True
