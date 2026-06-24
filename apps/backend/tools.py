import sys
from pathlib import Path
from datetime import datetime
from fastmcp import FastMCP

# Align workspace directories for imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

from queries import get_item_market_context

# Initialize the Model Context Protocol server instance
mcp = FastMCP("BrandSniperVerifier")

@mcp.tool()
async def get_market_context(market_hash_name: str) -> dict:
    """
    Queries historical database averages and calculates target cash-equivalent 
    baselines and discount snipe thresholds for a given asset name.
    """
    context = await get_item_market_context(market_hash_name)
    return context

@mcp.tool()
def verify_float_value(market_hash_name: str, float_value: float) -> str:
    """
    Evaluates if the item's float wear value carries a price premium or clean appearance.
    Supports standard wear thresholds (FN < 0.03, FT < 0.20, and high BS > 0.90 for Rust Coat).
    """
    if float_value < 0.03:
        return f"Excellent low float ({float_value:.4f}) - clean Factory New item. Desirable premium value."
    if 0.15 <= float_value <= 0.20:
        return f"Low float Field-Tested item ({float_value:.4f}) - desirable. Close to Minimal Wear look."
    if float_value >= 0.90:
        if "Rust Coat" in market_hash_name:
            return f"Extreme high float ({float_value:.4f}) - desirable 'rust' pattern for Rust Coat items. Premium value."
    return f"Standard float ({float_value:.4f}) - falls in standard wear corridor, no extra wear premium."

@mcp.tool()
def simulate_checkout_payload(market_hash_name: str, price_cents: int) -> dict:
    """
    Dispatches a checkout execution payload to purchase the asset. 
    Call this ONLY after confirming a true arbitrage opportunity.
    """
    price_usd = price_cents / 100.0
    print(f"\n🚀 [CHECKOUT TRIGGERED] Executing purchase authorization for: {market_hash_name} at ${price_usd:.2f}!")
    
    return {
        "status": "APPROVED",
        "transaction_id": f"txn_{int(datetime.now().timestamp())}",
        "execution_status": "COMMITTED",
        "asset": market_hash_name,
        "amount_usd": price_usd,
        "timestamp": int(datetime.now().timestamp())
    }
