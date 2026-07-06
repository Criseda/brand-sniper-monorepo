import json
import os
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from fastmcp import FastMCP

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")
load_dotenv(dotenv_path=PROJECT_ROOT / "apps" / "analytics" / ".env", override=True)

from shared_utils import get_logger

logger = get_logger("analytics.tools")

mcp = FastMCP("adversarial_cfo")

BACKEND_URL = os.getenv("COMPUTE_NODE_URL", "http://localhost:8080")
_session: aiohttp.ClientSession | None = None


async def _get_http_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10, connect=5))
    return _session


@mcp.tool()
async def fetch_live_market_floor(market_hash_name: str) -> str:
    try:
        session = await _get_http_session()
        url = f"{BACKEND_URL}/api/v1/market/context/{market_hash_name}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return json.dumps(
                    {
                        "market_hash_name": market_hash_name,
                        "live_floor_cents": data.get("snipe_threshold_cents") or data.get("cash_equivalent_avg_cents", 0),
                        "recent_sales_cents": [data.get("real_time_skinport_median_cents", 0)],
                        "liquidity": "HIGH" if data.get("is_liquid") else "LOW",
                        "message": "Live market context fetched from backend database.",
                    }
                )
    except Exception as e:
        logger.warning("Failed to fetch live market floor from backend: %s. Using simulated data.", e)

    if "AK-47" in market_hash_name:
        return json.dumps(
            {
                "market_hash_name": market_hash_name,
                "live_floor_cents": 1100,
                "recent_sales_cents": [1150, 1120, 1100, 1080],
                "liquidity": "HIGH",
                "message": "SIMULATED: Market is crashing for AK-47s right now.",
            }
        )
    return json.dumps(
        {
            "market_hash_name": market_hash_name,
            "live_floor_cents": 5000,
            "recent_sales_cents": [5000, 4900],
            "liquidity": "LOW",
            "message": "SIMULATED: Normal market conditions.",
        }
    )


@mcp.tool()
async def search_macro_trends(query: str) -> str:
    logger.info("Macro trend search requested: %s", query)
    data = {}
    try:
        session = await _get_http_session()
        url = f"{BACKEND_URL}/api/v1/market/search-trends"
        async with session.post(url, json={"query": query}, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
    except Exception as e:
        logger.warning("Failed to fetch macro trends from backend: %s", e)

    if not data:
        return "No major macroeconomic news detected."
    return json.dumps(data)


if __name__ == "__main__":
    mcp.run()
