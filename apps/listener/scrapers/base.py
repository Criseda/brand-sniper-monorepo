from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator

from models import MarketTick


class BaseScraper(ABC):
    """Abstract Base Class establishing the programmatic contract for all market ingestion nodes."""

    def __init__(self, platform_name: str):
        self.platform_name = platform_name
        self.sidecar_script_path = None  # Override in subclass if a Node.js sidecar is needed

    @abstractmethod
    async def poll_market_stream(self) -> AsyncGenerator[MarketTick, None]:
        """
        Continuous non-blocking generator that polls the target platform API
        and yields verified, normalized MarketTick objects.
        """
        pass

    async def close(self) -> None:
        """Releases any platform-specific resources (e.g. HTTP sessions). Override in subclass."""
        return

    async def listen_websocket_stream(self) -> AsyncGenerator[MarketTick, None]:
        """
        Optional non-blocking generator that subscribes to the platform's
        WebSocket feed (e.g. via Redis Pub/Sub relay) and yields MarketTick objects.
        """
        return
        yield  # pragma: no cover
