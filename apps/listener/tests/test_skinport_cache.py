import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from scrapers.skinport import SkinportScraper


@pytest.mark.asyncio
async def test_skinport_cache_initialization_with_url():
    scraper = SkinportScraper()

    # Mock Redis client and its async methods
    mock_redis = MagicMock()
    mock_redis.aclose = AsyncMock()

    mock_pubsub = MagicMock()
    mock_pubsub.subscribe = AsyncMock()
    mock_pubsub.unsubscribe = AsyncMock()

    # Mock listen as an async generator
    async def empty_async_gen():
        if False:
            yield None

    mock_pubsub.listen = MagicMock(return_value=empty_async_gen())
    mock_redis.pubsub.return_value = mock_pubsub

    env_overrides = {"EDGE_REDIS_URL": "redis://my-secure-redis-host:6379", "REDIS_PASSWORD": "my-secret-test-password"}

    with (
        patch("scrapers.skinport.Redis.from_url", return_value=mock_redis) as mock_from_url,
        patch.dict(os.environ, env_overrides),
    ):
        # We only start the generator and iterate once to trigger connection
        async for _ in scraper.listen_websocket_stream():
            break

        mock_from_url.assert_called_once_with(
            "redis://my-secure-redis-host:6379", username="default", password="my-secret-test-password", decode_responses=True
        )


@pytest.mark.asyncio
async def test_skinport_cache_initialization_fallback():
    scraper = SkinportScraper()

    # Mock Redis client and its async methods
    mock_redis = MagicMock()
    mock_redis.aclose = AsyncMock()

    mock_pubsub = MagicMock()
    mock_pubsub.subscribe = AsyncMock()
    mock_pubsub.unsubscribe = AsyncMock()

    async def empty_async_gen():
        if False:
            yield None

    mock_pubsub.listen = MagicMock(return_value=empty_async_gen())
    mock_redis.pubsub.return_value = mock_pubsub

    env_overrides = {
        "EDGE_REDIS_URL": "",
        "REDIS_HOST": "fallback-redis-host",
        "REDIS_PORT": "1234",
        "REDIS_PASSWORD": "fallback-secret-password",
    }

    with patch("scrapers.skinport.Redis", return_value=mock_redis) as mock_redis_class, patch.dict(os.environ, env_overrides):
        async for _ in scraper.listen_websocket_stream():
            break

        mock_redis_class.assert_called_once_with(
            host="fallback-redis-host",
            port=1234,
            username="default",
            password="fallback-secret-password",
            decode_responses=True,
        )
