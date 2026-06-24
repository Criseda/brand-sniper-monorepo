from abc import ABC, abstractmethod
from typing import AsyncGenerator
from models import MarketTick

class BaseScraper(ABC):
    """Abstract Base Class establishing the programmatic contract for all market ingestion nodes."""
    
    def __init__(self, platform_name: str):
        self.platform_name = platform_name

    @abstractmethod
    async def poll_market_stream(self) -> AsyncGenerator[MarketTick, None]:
        """
        Continuous non-blocking generator that polls the target platform API 
        and yields verified, normalized MarketTick objects.
        """
        pass