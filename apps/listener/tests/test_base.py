import pytest
from scrapers.base import BaseScraper


class ConcreteScraper(BaseScraper):
    async def poll_market_stream(self):
        yield None  # pragma: no cover - never invoked in these tests


@pytest.mark.asyncio
async def test_default_verify_anomaly_returns_false():
    scraper = ConcreteScraper("test_platform")

    assert await scraper.verify_anomaly_with_history("Some Item (Factory New)", 100.0) is False


@pytest.mark.asyncio
async def test_default_websocket_stream_yields_nothing():
    scraper = ConcreteScraper("test_platform")

    messages = [msg async for msg in scraper.listen_websocket_stream()]

    assert messages == []
