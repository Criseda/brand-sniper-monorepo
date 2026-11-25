import os
from unittest.mock import patch

from scrapers.base import edge_redis_from_env


def test_edge_redis_from_url():
    env_overrides = {"EDGE_REDIS_URL": "redis://my-secure-redis-host:6379", "REDIS_PASSWORD": "my-secret-test-password"}

    with patch("scrapers.base.Redis.from_url") as mock_from_url, patch.dict(os.environ, env_overrides):
        edge_redis_from_env()

    mock_from_url.assert_called_once_with(
        "redis://my-secure-redis-host:6379", username="default", password="my-secret-test-password", decode_responses=True
    )


def test_edge_redis_defaults_to_the_local_edge_redis():
    env_overrides = {"EDGE_REDIS_URL": "", "REDIS_PASSWORD": "fallback-secret-password"}

    with patch("scrapers.base.Redis.from_url") as mock_from_url, patch.dict(os.environ, env_overrides):
        edge_redis_from_env()

    mock_from_url.assert_called_once_with(
        "redis://localhost:6380", username="default", password="fallback-secret-password", decode_responses=True
    )
