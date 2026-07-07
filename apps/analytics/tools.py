import json
import os
import urllib.error
import urllib.parse
import urllib.request

from pydantic import BaseModel, Field
from shared_utils import get_logger

logger = get_logger("analytics.tools")

BACKEND_URL: str = os.getenv("COMPUTE_NODE_URL", "http://localhost:8080")

_FLOAT_TIERS: list[tuple[float, float, str]] = [
    (0.00, 0.07, "Factory New"),
    (0.07, 0.15, "Minimal Wear"),
    (0.15, 0.38, "Field-Tested"),
    (0.38, 0.45, "Well-Worn"),
    (0.45, 1.01, "Battle-Scarred"),
]


class FetchMarketFloorArgs(BaseModel):
    market_hash_name: str = Field(description="The market hash name of the item (e.g. 'AK-47 | Redline (Field-Tested)')")


class SearchTrendsArgs(BaseModel):
    query: str = Field(description="Search query for the trend to look up (e.g. 'CS:GO market crash', 'new weapon case').")


class VerifyFloatArgs(BaseModel):
    market_hash_name: str = Field(description="The market hash name of the item (e.g. 'AK-47 | Redline (Field-Tested)')")
    float_value: float = Field(description="The item's exact float wear value (0.0 to 1.0). Lower = cleaner.")


def _classify_float(float_value: float) -> tuple[str, float, str]:
    if float_value < 0.0 or float_value > 1.0:
        return ("Standard", 1.0, "Invalid float value.")

    if float_value < 0.01:
        return ("Exceptional", 1.5, "Top percentile float — significant premium over standard FN.")
    if float_value < 0.03:
        return ("Excellent", 1.3, "Excellent low float — clean appearance commands noticeable premium.")
    if float_value < 0.07:
        return ("Good", 1.1, "Good Factory New float — above average but not exceptional.")
    if float_value < 0.08:
        return ("Good", 1.15, "FN-look Minimal Wear — visually indistinguishable from Factory New, mild premium.")
    if float_value < 0.10:
        return ("Decent", 1.05, "Low float Minimal Wear — cleaner than average, small premium.")
    if float_value < 0.16:
        return ("Good", 1.1, "Low float Field-Tested — close to Minimal Wear appearance, mild premium.")
    if float_value < 0.20:
        return ("Decent", 1.03, "Lower-end Field-Tested — slightly cleaner than typical FT.")
    if float_value >= 0.95:
        return ("Exceptional", 1.3, "Extreme high float — potential collector value for certain patterns.")
    if float_value >= 0.90:
        return ("Notable", 1.15, "High float Battle-Scarred — may carry a niche premium for specific items.")
    return ("Standard", 1.0, "Standard float — falls in typical wear corridor, no extra premium.")


def _wear_tier(float_value: float) -> str:
    for lo, hi, label in _FLOAT_TIERS:
        if lo <= float_value < hi:
            return label
    return "Unknown"


def fetch_live_market_floor(market_hash_name: str) -> str:
    logger.info("[CFO] Fetching live market floor for: %s", market_hash_name)
    try:
        url = f"{BACKEND_URL}/api/v1/market/context/{urllib.parse.quote(market_hash_name, safe='')}"
        with urllib.request.urlopen(url, timeout=5) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode())
                # Use undiscounted cash equivalent average or real-time median as the true live floor price
                live_floor = (
                    data.get("cash_equivalent_avg_cents")
                    or data.get("real_time_skinport_median_cents")
                    or data.get("snipe_threshold_cents")
                    or 0
                )
                return json.dumps(
                    {
                        "market_hash_name": market_hash_name,
                        "live_floor_cents": live_floor,
                        "recent_sales_cents": [data.get("real_time_skinport_median_cents", 0)],
                        "liquidity": "HIGH" if data.get("is_liquid") else "LOW",
                        "message": "Live market context fetched from backend database.",
                    }
                )
    except Exception as e:
        logger.warning("Failed to fetch live market floor from backend: %s. Returning error payload.", e)

    return json.dumps(
        {
            "market_hash_name": market_hash_name,
            "live_floor_cents": None,
            "recent_sales_cents": [],
            "liquidity": "UNKNOWN",
            "message": "ERROR: Live backend database unavailable. Valuation verification skipped.",
            "error": "Failed to fetch live market floor due to backend server connection timeout or failure.",
        }
    )


def search_macro_trends(query: str) -> str:
    logger.info("[CFO] Macro trend search requested: %s", query)
    data: dict = {}
    try:
        url = f"{BACKEND_URL}/api/v1/market/search-trends"
        req = urllib.request.Request(
            url,
            data=json.dumps({"query": query}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode())
    except Exception as e:
        logger.warning("Failed to fetch macro trends from backend: %s", e)

    if not data:
        return "No major macroeconomic news detected."
    return json.dumps(data)


def verify_float_value(market_hash_name: str, float_value: float) -> str:
    logger.info("[CFO] Verifying float value for %s: %.4f", market_hash_name, float_value)
    quality, multiplier, description = _classify_float(float_value)
    return json.dumps(
        {
            "market_hash_name": market_hash_name,
            "float_value": float_value,
            "wear_tier": _wear_tier(float_value),
            "float_quality": quality,
            "premium_multiplier": multiplier,
            "description": description,
        }
    )


from openai import pydantic_function_tool  # noqa: E402

_TOOL_DEFS: list[tuple[str, str, type[BaseModel], str]] = [
    (
        "fetch_live_market_floor",
        "Fetch live market floor by item hash name. Check if bot's buy is above or below market.",
        FetchMarketFloorArgs,
        fetch_live_market_floor.__name__,
    ),
    (
        "search_macro_trends",
        "Search recent macro trends or news affecting item prices (crashes, new cases, tournaments).",
        SearchTrendsArgs,
        search_macro_trends.__name__,
    ),
    (
        "verify_float_value",
        "Evaluate if an item's float wear value carries a price premium. "
        "Returns a multiplier (1.0 = standard, >1.0 = premium) based on CS2 wear tier thresholds. "
        "Call this alongside fetch_live_market_floor to determine if the float justifies the purchase price.",
        VerifyFloatArgs,
        verify_float_value.__name__,
    ),
]

TOOL_SCHEMAS = [pydantic_function_tool(args_model, name=name, description=desc) for name, desc, args_model, _ in _TOOL_DEFS]

AVAILABLE_FUNCTIONS = {name: fn for name, _, _, fn_name in _TOOL_DEFS for fn in [globals()[fn_name]]}
