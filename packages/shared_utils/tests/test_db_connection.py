import pytest
from shared_utils.db_connection import (
    MissingDatabaseURLError,
    MissingDatabaseURLSentinel,
    apply_ssl_for_remote,
)


def test_apply_ssl_for_remote_localhost():
    # Localhost, 127.0.0.1, [::1] should not require SSL
    assert apply_ssl_for_remote("postgresql+asyncpg://user:pass@localhost/db") == "postgresql+asyncpg://user:pass@localhost/db"
    assert (
        apply_ssl_for_remote("postgresql+asyncpg://user:pass@127.0.0.1:5432/db")
        == "postgresql+asyncpg://user:pass@127.0.0.1:5432/db"
    )
    assert apply_ssl_for_remote("postgresql+asyncpg://user:pass@[::1]/db") == "postgresql+asyncpg://user:pass@[::1]/db"


def test_apply_ssl_for_remote_external():
    # Remote hosts should default to requiring SSL
    assert (
        apply_ssl_for_remote("postgresql+asyncpg://user:pass@brand-sniper-db.postgres.database.azure.com/db")
        == "postgresql+asyncpg://user:pass@brand-sniper-db.postgres.database.azure.com/db?ssl=require"
    )
    # If psycopg2 is in the driver name, use sslmode=require
    assert (
        apply_ssl_for_remote("postgresql+psycopg2://user:pass@brand-sniper-db.postgres.database.azure.com/db")
        == "postgresql+psycopg2://user:pass@brand-sniper-db.postgres.database.azure.com/db?sslmode=require"
    )


def test_apply_ssl_for_remote_already_has_ssl():
    # If the URL already specifies SSL mode or ssl, do not override or add
    assert (
        apply_ssl_for_remote("postgresql+asyncpg://user:pass@remote/db?ssl=require")
        == "postgresql+asyncpg://user:pass@remote/db?ssl=require"
    )
    assert (
        apply_ssl_for_remote("postgresql+asyncpg://user:pass@remote/db?sslmode=verify-full")
        == "postgresql+asyncpg://user:pass@remote/db?sslmode=verify-full"
    )


def test_apply_ssl_for_remote_prevents_substring_bypass():
    # Verify that hostnames containing 'localhost' as a substring do not bypass SSL requirements
    assert (
        apply_ssl_for_remote("postgresql+asyncpg://user:pass@evil-localhost.com/db")
        == "postgresql+asyncpg://user:pass@evil-localhost.com/db?ssl=require"
    )
    assert (
        apply_ssl_for_remote("postgresql+asyncpg://user:pass@localhost.attacker.com/db")
        == "postgresql+asyncpg://user:pass@localhost.attacker.com/db?ssl=require"
    )
    assert (
        apply_ssl_for_remote("postgresql+asyncpg://user:pass@127.0.0.1.attacker.com/db")
        == "postgresql+asyncpg://user:pass@127.0.0.1.attacker.com/db?ssl=require"
    )


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
