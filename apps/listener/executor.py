import abc

import aiohttp
from listener_telemetry import trade_submissions_total
from shared_utils import backend_api_headers, get_logger

logger = get_logger("listener.executor")

_session: aiohttp.ClientSession | None = None


async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        timeout = aiohttp.ClientTimeout(total=5, connect=2)
        _session = aiohttp.ClientSession(headers=backend_api_headers(), timeout=timeout)
    return _session


async def close_http_session() -> None:
    global _session
    if _session is not None and not _session.closed:
        await _session.close()
        _session = None


class ExecutionService(abc.ABC):
    @abc.abstractmethod
    async def execute(
        self,
        market_hash_name: str,
        purchase_price_cents: int,
        estimated_profit_cents: int,
        z_score: float,
        listing_id: str | None = None,
        float_value: float | None = None,
    ) -> None:
        pass


class ExecutionError(RuntimeError):
    """Raised when an execution cannot be submitted to the backend."""


class PaperExecutor(ExecutionService):
    def __init__(self, backend_url: str):
        self.trade_ingest_url = f"{backend_url.rstrip('/')}/api/v1/ingest/trade"

    async def execute(
        self,
        market_hash_name: str,
        purchase_price_cents: int,
        estimated_profit_cents: int,
        z_score: float,
        listing_id: str | None = None,
        float_value: float | None = None,
    ) -> None:
        payload = {
            "market_hash_name": market_hash_name,
            "purchase_price_cents": purchase_price_cents,
            "estimated_profit_cents": estimated_profit_cents,
            "trigger_z_score": round(z_score, 4),
            # Identify the exact listing bought so audits and outcome labels use its own attributes.
            "listing_id": listing_id,
            "float_value": float_value,
        }

        logger.info(
            "[PAPER TRADE] Simulated Buy | Item: %s | Price: $%.2f | Est. Profit: $%.2f | Z-Score: %.2f",
            market_hash_name,
            purchase_price_cents / 100,
            estimated_profit_cents / 100,
            z_score,
        )

        await self._send_to_backend(payload)

    async def _send_to_backend(self, payload: dict) -> None:
        try:
            session = await _get_session()
            async with session.post(self.trade_ingest_url, json=payload) as resp:
                if resp.status not in (201, 202):
                    trade_submissions_total.labels(outcome="rejected").inc()
                    raise ExecutionError(f"Backend rejected trade submission with HTTP {resp.status}")
        except (TimeoutError, aiohttp.ClientError) as e:
            trade_submissions_total.labels(outcome="error").inc()
            raise ExecutionError("Failed to reach the backend for trade submission") from e
        trade_submissions_total.labels(outcome="success").inc()
