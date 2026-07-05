import abc
import asyncio

import aiohttp
from shared_utils import get_logger

logger = get_logger("listener.executor")


class ExecutionService(abc.ABC):
    """
    Base interface for all autonomous edge trade executors (Live, Paper).
    """

    @abc.abstractmethod
    async def execute(
        self,
        market_hash_name: str,
        purchase_price_cents: int,
        estimated_profit_cents: int,
        z_score: float,
    ) -> None:
        pass


class PaperExecutor(ExecutionService):
    """
    Simulates a trade on the Edge node by sending a fire-and-forget payload
    to the Command Center (Backend) to securely log the SimulatedTrade in Postgres.
    """

    def __init__(self, backend_url: str):
        self.trade_ingest_url = f"{backend_url.rstrip('/')}/api/v1/ingest/trade"

    async def execute(
        self,
        market_hash_name: str,
        purchase_price_cents: int,
        estimated_profit_cents: int,
        z_score: float,
    ) -> None:
        payload = {
            "market_hash_name": market_hash_name,
            "purchase_price_cents": purchase_price_cents,
            "estimated_profit_cents": estimated_profit_cents,
            "trigger_z_score": round(z_score, 4),
        }

        logger.info(
            "Simulated Buy | Item: %s | Price: $%.2f | Est. Profit: $%.2f | Z-Score: %.2f",
            market_hash_name, purchase_price_cents / 100, estimated_profit_cents / 100, z_score,
        )

        # Fire and forget HTTP POST (wrapped in a background task so it never blocks the hot path)
        asyncio.create_task(self._send_to_backend(payload))

    async def _send_to_backend(self, payload: dict) -> None:
        try:
            timeout = aiohttp.ClientTimeout(total=5, connect=2)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(self.trade_ingest_url, json=payload) as resp:
                    if resp.status not in (201, 202):
                        logger.warning("Backend rejected trade log with status %s", resp.status)
        except Exception as e:
            logger.error("Failed to reach Command Center to log trade: %s", e)
