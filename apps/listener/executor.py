import abc
from collections import OrderedDict

import aiohttp
from listener_telemetry import trade_submissions_total
from shared_utils import backend_api_headers, get_logger

logger = get_logger("listener.executor")

_session: aiohttp.ClientSession | None = None

# Purchases the paper executor remembers, to buy each one once. Tens of buys an hour fit for weeks.
BOUGHT_MEMORY_SIZE = 10_000


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
        estimated_profit_cents: int | None,
        z_score: float,
        listing_id: str | None = None,
        float_value: float | None = None,
        profit_estimate_basis: str | None = None,
    ) -> None:
        pass


class ExecutionError(RuntimeError):
    """Raised when an execution cannot be submitted to the backend."""


def purchase_key(market_hash_name: str, purchase_price_cents: int, listing_id: str | None) -> str:
    """What a paper buy bought. A REST snapshot names no listing, so its item and price stand in for one."""
    if listing_id is not None:
        return f"listing:{market_hash_name}:{listing_id}"
    return f"snapshot:{market_hash_name}:{purchase_price_cents}"


class PaperExecutor(ExecutionService):
    def __init__(self, backend_url: str):
        self.trade_ingest_url = f"{backend_url.rstrip('/')}/api/v1/ingest/trade"
        # Purchases already recorded, so a listing re-sent by the feed is not bought twice (LRU, in memory).
        self._bought: OrderedDict[str, None] = OrderedDict()

    async def execute(
        self,
        market_hash_name: str,
        purchase_price_cents: int,
        estimated_profit_cents: int | None,
        z_score: float,
        listing_id: str | None = None,
        float_value: float | None = None,
        profit_estimate_basis: str | None = None,
    ) -> None:
        key = purchase_key(market_hash_name, purchase_price_cents, listing_id)
        if key in self._bought:
            logger.info(
                "[PAPER TRADE] Skipped repeat buy | Item: %s | Price: $%.2f | Listing: %s",
                market_hash_name,
                purchase_price_cents / 100,
                listing_id or "REST snapshot",
            )
            return

        payload = {
            "market_hash_name": market_hash_name,
            "purchase_price_cents": purchase_price_cents,
            # None when there was no baseline price to estimate a resale from.
            "estimated_profit_cents": estimated_profit_cents,
            "profit_estimate_basis": profit_estimate_basis,
            "trigger_z_score": round(z_score, 4),
            # Identify the exact listing bought so audits and outcome labels use its own attributes.
            "listing_id": listing_id,
            "float_value": float_value,
        }

        profit_text = f"${estimated_profit_cents / 100:.2f}" if estimated_profit_cents is not None else "n/a"
        logger.info(
            "[PAPER TRADE] Simulated Buy | Item: %s | Price: $%.2f | Est. Profit: %s | Z-Score: %.2f",
            market_hash_name,
            purchase_price_cents / 100,
            profit_text,
            z_score,
        )

        await self._send_to_backend(payload)
        # Remembered only once recorded, so a buy the backend did not take can happen again.
        self._remember_purchase(key)

    def _remember_purchase(self, key: str) -> None:
        self._bought[key] = None
        while len(self._bought) > BOUGHT_MEMORY_SIZE:
            self._bought.popitem(last=False)

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
