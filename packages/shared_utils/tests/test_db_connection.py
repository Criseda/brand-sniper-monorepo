import pytest
from shared_utils.db_connection import (
    MissingDatabaseURLError,
    MissingDatabaseURLSentinel,
    apply_ssl_for_remote,
)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        # Localhost, 127.0.0.1, [::1] should not require SSL
        pytest.param(
            "postgresql+asyncpg://user:pass@localhost/db", "postgresql+asyncpg://user:pass@localhost/db", id="localhost"
        ),
        pytest.param(
            "postgresql+asyncpg://user:pass@127.0.0.1:5432/db",
            "postgresql+asyncpg://user:pass@127.0.0.1:5432/db",
            id="loopback_ipv4",
        ),
        pytest.param("postgresql+asyncpg://user:pass@[::1]/db", "postgresql+asyncpg://user:pass@[::1]/db", id="loopback_ipv6"),
        # Remote hosts should default to requiring SSL
        pytest.param(
            "postgresql+asyncpg://user:pass@brand-sniper-db.postgres.database.azure.com/db",
            "postgresql+asyncpg://user:pass@brand-sniper-db.postgres.database.azure.com/db?ssl=require",
            id="remote_asyncpg",
        ),
        # If psycopg2 is in the driver name, use sslmode=require
        pytest.param(
            "postgresql+psycopg2://user:pass@brand-sniper-db.postgres.database.azure.com/db",
            "postgresql+psycopg2://user:pass@brand-sniper-db.postgres.database.azure.com/db?sslmode=require",
            id="remote_psycopg2",
        ),
        # If the URL already specifies SSL mode or ssl, do not override or add
        pytest.param(
            "postgresql+asyncpg://user:pass@remote/db?ssl=require",
            "postgresql+asyncpg://user:pass@remote/db?ssl=require",
            id="already_has_ssl",
        ),
        pytest.param(
            "postgresql+asyncpg://user:pass@remote/db?sslmode=verify-full",
            "postgresql+asyncpg://user:pass@remote/db?sslmode=verify-full",
            id="already_has_sslmode",
        ),
        # Verify that hostnames containing 'localhost' as a substring do not bypass SSL requirements
        pytest.param(
            "postgresql+asyncpg://user:pass@evil-localhost.com/db",
            "postgresql+asyncpg://user:pass@evil-localhost.com/db?ssl=require",
            id="substring_bypass_localhost",
        ),
        pytest.param(
            "postgresql+asyncpg://user:pass@localhost.attacker.com/db",
            "postgresql+asyncpg://user:pass@localhost.attacker.com/db?ssl=require",
            id="substring_bypass_prefix",
        ),
        pytest.param(
            "postgresql+asyncpg://user:pass@127.0.0.1.attacker.com/db",
            "postgresql+asyncpg://user:pass@127.0.0.1.attacker.com/db?ssl=require",
            id="substring_bypass_ipv4",
        ),
    ],
)
def test_apply_ssl_for_remote(url, expected):
    assert apply_ssl_for_remote(url) == expected


def test_sentinel_raises_on_attribute_access():
    sentinel = MissingDatabaseURLSentinel("test_sentinel")

    with pytest.raises(MissingDatabaseURLError) as exc_info:
        sentinel.begin()
    assert "DATABASE_URL environment variable is not set" in str(exc_info.value)
    assert "test_sentinel" in str(exc_info.value)


def test_sentinel_raises_on_call():
    sentinel = MissingDatabaseURLSentinel("test_sentinel")

    with pytest.raises(MissingDatabaseURLError) as exc_info:
        sentinel()
    assert "DATABASE_URL environment variable is not set" in str(exc_info.value)


def test_sentinel_bool_evaluation():
    sentinel = MissingDatabaseURLSentinel("test_sentinel")
    assert not sentinel
