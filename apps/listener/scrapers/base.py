import os
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator

from models import FeedEvent, MarketTick
from redis.asyncio import Redis


def edge_redis_from_env() -> Redis:
    """A client for the edge Redis, configured from EDGE_REDIS_URL (or REDIS_HOST and REDIS_PORT)."""
    edge_redis_url = os.getenv("EDGE_REDIS_URL")
    redis_password = os.getenv("REDIS_PASSWORD")
    if edge_redis_url:
        return Redis.from_url(edge_redis_url, username="default", password=redis_password, decode_responses=True)
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6380))
    return Redis(host=redis_host, port=redis_port, username="default", password=redis_password, decode_responses=True)


def _as_number(value: object) -> float | None:
    """The value as a float when it is a real JSON number (bools excluded), else None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def optional_non_negative_int(value: object) -> int | None:
    """Pattern/finish as an int, or None when missing or out of range (the price is still kept)."""
    number = _as_number(value)
    return int(number) if number is not None and number >= 0 else None


def optional_wear(value: object) -> float | None:
    """Float value in [0, 1], or None when missing or out of range (the price is still kept)."""
    number = _as_number(value)
    return number if number is not None and 0 <= number <= 1 else None


class BaseScraper(ABC):
    """Abstract Base Class establishing the programmatic contract for all market ingestion nodes."""

    # False for a venue whose live feed is complete on its own, so the listener starts no REST poller.
    polls_rest: bool = True

    def __init__(self, venue: str):
        self.venue = venue
        self.sidecar_script_path = None  # Override in subclass if a Node.js sidecar is needed

    @abstractmethod
    async def poll_market_stream(self) -> AsyncGenerator[MarketTick, None]:
        """
        Continuous non-blocking generator that polls the venue's REST API
        and yields verified, normalized MarketTick objects.
        """
        pass  # pragma: no cover - abstract body, subclasses override

    async def close(self) -> None:
        """Releases any venue-specific resources (e.g. HTTP sessions). Override in subclass."""
        return

    async def listen_websocket_stream(self) -> AsyncGenerator[MarketTick | FeedEvent, None]:
        """
        Optional non-blocking generator that subscribes to the venue's
        WebSocket feed (e.g. via Redis Pub/Sub relay) and yields MarketTick objects,
        plus raw FeedEvent records where the venue feed is captured verbatim.
        """
        return
        yield  # pragma: no cover
