import sys
from pathlib import Path
from datetime import datetime
from fastmcp import FastMCP

# Align workspace directories for imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

from queries import get_item_market_context
from telemetry import run_telemetry

# Initialize the Model Context Protocol server instance
mcp = FastMCP("BrandSniperVerifier")

@mcp.tool()
async def get_market_context(market_hash_name: str) -> dict:
    """
    Queries historical database averages and calculates target cash-equivalent 
    baselines and discount snipe thresholds for a given asset name.
    """
    context = await get_item_market_context(market_hash_name)
    try:
        t_dict = run_telemetry.get()
        if t_dict is not None:
            t_dict.update({
                "historical_steam_avg_cents": context.get("historical_steam_avg_cents"),
                "historical_skinport_avg_cents": context.get("historical_skinport_avg_cents"),
                "real_time_skinport_median_cents": context.get("real_time_skinport_median_cents"),
                "cash_equivalent_avg_cents": context.get("cash_equivalent_avg_cents"),
                "snipe_threshold_cents": context.get("snipe_threshold_cents"),
                "is_liquid": context.get("is_liquid"),
                "downtrend_detected": context.get("downtrend_detected"),
                "downtrend_severity": context.get("downtrend_severity"),
                "regime_shift_detected": context.get("regime_shift_detected"),
                "item_page": context.get("item_page"),
                "market_page": context.get("market_page"),
            })
    except LookupError:
        pass
    return context

@mcp.tool()
def verify_float_value(market_hash_name: str, float_value: float) -> str:
    """
    Evaluates if the item's float wear value carries a price premium or clean appearance.
    Supports standard wear thresholds (FN < 0.03, FT < 0.20, and high BS > 0.90 for Rust Coat).
    """
    try:
        t_dict = run_telemetry.get()
        if t_dict is not None:
            t_dict["float_value"] = float_value
    except LookupError:
        pass

    if float_value < 0.03:
        return f"Excellent low float ({float_value:.4f}) - clean Factory New item. Desirable premium value."
    if 0.15 <= float_value <= 0.20:
        return f"Low float Field-Tested item ({float_value:.4f}) - desirable. Close to Minimal Wear look."
    if float_value >= 0.90:
        if "Rust Coat" in market_hash_name:
            return f"Extreme high float ({float_value:.4f}) - desirable 'rust' pattern for Rust Coat items. Premium value."
    return f"Standard float ({float_value:.4f}) - falls in standard wear corridor, no extra wear premium."

@mcp.tool()
def confirm_alert_approval(market_hash_name: str, item_page: str) -> dict:
    """
    Flags the anomaly alert as a verified, genuine deep discount and registers the direct marketplace purchase link.
    Call this ONLY after verifying a true arbitrage opportunity.
    """
    print(f"\n📢 [ALERT APPROVED] Snipe opportunity verified for {market_hash_name}!")
    print(f"   Direct Purchase Link: {item_page}")
    
    res = {
        "status": "APPROVED",
        "asset": market_hash_name,
        "item_page": item_page,
        "timestamp": int(datetime.now().timestamp())
    }
    
    try:
        t_dict = run_telemetry.get()
        if t_dict is not None:
            t_dict["alert_approved"] = True
            t_dict["item_page"] = item_page
    except LookupError:
        pass
        
    return res

