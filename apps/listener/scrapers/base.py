from abc import ABC, abstractmethod
from typing import AsyncGenerator
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

    async def verify_anomaly_with_history(self, market_hash_name: str, price_usd: float) -> bool:
        """
        Secondary verification using the platform's historical data API.
        Default implementation returns True.
        """
        return True

    async def listen_websocket_stream(self) -> AsyncGenerator[MarketTick, None]:
        """
        Optional non-blocking generator that subscribes to the platform's 
        WebSocket feed (e.g. via Redis Pub/Sub relay) and yields MarketTick objects.
        """
        return
        yield