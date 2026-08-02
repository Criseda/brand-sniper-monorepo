import pytest
from shared_utils.db_connection import (
    DatabaseConnectionError,
    MissingDatabaseURLError,
    MissingDatabaseURLSentinel,
    apply_ssl_for_remote,
    session_scope,
)
from sqlalchemy.exc import SQLAlchemyError


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


@pytest.mark.asyncio
async def test_session_scope_raises_when_database_url_missing(monkeypatch):
    monkeypatch.setattr("shared_utils.db_connection.async_session_maker", MissingDatabaseURLSentinel("async_session_maker"))

    with pytest.raises(MissingDatabaseURLError):
        async with session_scope():
            pass


@pytest.mark.asyncio
async def test_session_scope_wraps_session_acquisition_failure(monkeypatch):
    def exploding_maker():
        raise SQLAlchemyError("connection refused")

    monkeypatch.setattr("shared_utils.db_connection.async_session_maker", exploding_maker)

    with pytest.raises(DatabaseConnectionError) as exc_info:
        async with session_scope():
            pass
    assert "Could not acquire a database session" in str(exc_info.value)


@pytest.mark.asyncio
async def test_session_scope_wraps_operation_failure_and_rolls_back(monkeypatch):
    rolled_back = False

    class ExplodingSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def commit(self):
            raise SQLAlchemyError("transaction deadlocked")

        async def rollback(self):
            nonlocal rolled_back
            rolled_back = True

    def exploding_maker():
        return ExplodingSession()

    monkeypatch.setattr("shared_utils.db_connection.async_session_maker", exploding_maker)

    with pytest.raises(DatabaseConnectionError) as exc_info:
        async with session_scope():
            pass
    assert "Database operation failed" in str(exc_info.value)
    assert rolled_back


@pytest.mark.asyncio
async def test_session_scope_commits_on_success(monkeypatch):
    committed = False

    class GoodSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def commit(self):
            nonlocal committed
            committed = True

    def good_maker():
        return GoodSession()

    monkeypatch.setattr("shared_utils.db_connection.async_session_maker", good_maker)

    async with session_scope() as session:
        assert isinstance(session, GoodSession)
    assert committed
