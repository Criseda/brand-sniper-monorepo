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


def test_edge_redis_from_host_and_port():
    env_overrides = {
        "EDGE_REDIS_URL": "",
        "REDIS_HOST": "fallback-redis-host",
        "REDIS_PORT": "1234",
        "REDIS_PASSWORD": "fallback-secret-password",
    }

    with patch("scrapers.base.Redis") as mock_redis_class, patch.dict(os.environ, env_overrides):
        edge_redis_from_env()

    mock_redis_class.assert_called_once_with(
        host="fallback-redis-host",
        port=1234,
        username="default",
        password="fallback-secret-password",
        decode_responses=True,
    )
