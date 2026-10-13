import abc
from collections import OrderedDict
from dataclasses import dataclass

import aiohttp
from listener_telemetry import trade_submissions_total
from shared_utils import backend_api_headers, get_logger, purchase_keys, utc_now_naive

logger = get_logger("listener.executor")

_session: aiohttp.ClientSession | None = None

# Purchases an executor remembers, to skip a repeat without asking the backend. Tens of buys an hour fit
# for weeks. The memory is empty after a restart; the backend's unique purchase indexes are the guarantee.
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


class ExecutionError(RuntimeError):
    """Raised when an execution cannot be submitted to the backend."""


@dataclass(frozen=True)
class Purchase:
    venue: str
    market_hash_name: str
    purchase_price_cents: int
    # None when there was no baseline price to estimate a resale from.
    estimated_profit_cents: int | None
    profit_estimate_basis: str | None
    z_score: float
    # The exact listing bought; None for a REST snapshot.
    listing_id: str | None
    float_value: float | None


class ExecutionService(abc.ABC):
    """Buys what the rules engine approved, each purchase once.

    execute() skips a purchase this process already made; an executor only places the order. That memory
    is lost on restart, so place_order must let the venue or the backend refuse a repeat.
    """

    def __init__(self) -> None:
        # Purchases already made, so a listing re-sent by the feed is not bought twice (LRU, in memory).
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
        *,
        venue: str,
    ) -> None:
        # The backend's unique purchase indexes and the scorecard apply the same once-only rule.
        key, snapshot_key = purchase_keys(venue, market_hash_name, purchase_price_cents, listing_id, utc_now_naive().date())
        if key in self._bought:
            logger.info(
                "[PAPER TRADE] Skipped repeat buy | Venue: %s | Item: %s | Price: $%.2f | Listing: %s",
                venue,
                market_hash_name,
                purchase_price_cents / 100,
                listing_id or "REST snapshot",
            )
            return

        purchase = Purchase(
            venue=venue,
            market_hash_name=market_hash_name,
            purchase_price_cents=purchase_price_cents,
            estimated_profit_cents=estimated_profit_cents,
            profit_estimate_basis=profit_estimate_basis,
            z_score=z_score,
            listing_id=listing_id,
            float_value=float_value,
        )
        await self.place_order(purchase)
        # Remembered only once the order was taken, so a failed buy is not blocked if the tick is scored again.
        self._remember_purchase(key)
        if key != snapshot_key:
            # The next REST poll usually shows this listing as the item's lowest ask; do not buy it again.
            self._remember_purchase(snapshot_key)

    @abc.abstractmethod
    async def place_order(self, purchase: Purchase) -> None:
        """Buy it, or learn it was already bought. Raise ExecutionError when the order was not taken."""

    def _remember_purchase(self, key: str) -> None:
        self._bought[key] = None
        while len(self._bought) > BOUGHT_MEMORY_SIZE:
            self._bought.popitem(last=False)


class PaperExecutor(ExecutionService):
    def __init__(self, backend_url: str):
        super().__init__()
        self.trade_ingest_url = f"{backend_url.rstrip('/')}/api/v1/ingest/trade"

    async def place_order(self, purchase: Purchase) -> None:
        payload = {
            "market_hash_name": purchase.market_hash_name,
            "venue": purchase.venue,
            "purchase_price_cents": purchase.purchase_price_cents,
            "estimated_profit_cents": purchase.estimated_profit_cents,
            "profit_estimate_basis": purchase.profit_estimate_basis,
            "trigger_z_score": round(purchase.z_score, 4),
            # Identify the exact listing bought so audits and outcome labels use its own attributes.
            "listing_id": purchase.listing_id,
            "float_value": purchase.float_value,
        }

        profit_cents = purchase.estimated_profit_cents
        profit_text = f"${profit_cents / 100:.2f}" if profit_cents is not None else "n/a"
        logger.info(
            "[PAPER TRADE] Simulated Buy | Venue: %s | Item: %s | Price: $%.2f | Est. Profit: %s | Z-Score: %.2f",
            purchase.venue,
            purchase.market_hash_name,
            purchase.purchase_price_cents / 100,
            profit_text,
            purchase.z_score,
        )

        await self._send_to_backend(payload)

    async def _send_to_backend(self, payload: dict) -> None:
        try:
            session = await _get_session()
            async with session.post(self.trade_ingest_url, json=payload) as resp:
                if resp.status == 200:
                    # Recorded before, e.g. by this listener before a restart. The backend stored nothing new.
                    trade_submissions_total.labels(outcome="already_recorded").inc()
                    logger.info(
                        "[PAPER TRADE] Already recorded | Item: %s | Listing: %s",
                        payload.get("market_hash_name"),
                        payload.get("listing_id") or "REST snapshot",
                    )
                    return
                if resp.status not in (201, 202):
                    trade_submissions_total.labels(outcome="rejected").inc()
                    raise ExecutionError(f"Backend rejected trade submission with HTTP {resp.status}")
        except (TimeoutError, aiohttp.ClientError) as e:
            trade_submissions_total.labels(outcome="error").inc()
            raise ExecutionError("Failed to reach the backend for trade submission") from e
        trade_submissions_total.labels(outcome="success").inc()
