import os
import urllib.parse
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession


class MissingDatabaseURLError(ValueError):
    """Raised when trying to use database features but DATABASE_URL is not set."""

    pass


class MissingDatabaseURLSentinel:
    """Sentinel object that raises a clear error when accessed or called.

    This prevents immediate failures during import (e.g., in unit tests)
    while ensuring a clear error is raised if the application actually tries
    to interact with the database.
    """

    def __init__(self, name: str):
        self._name = name

    def __getattr__(self, item: str):
        if item.startswith("__") and item.endswith("__"):
            raise AttributeError(item)
        raise MissingDatabaseURLError(
            f"DATABASE_URL environment variable is not set, so '{self._name}' is not available. "
            "Please configure DATABASE_URL in your environment or root .env file."
        )

    def __call__(self, *args, **kwargs):
        raise MissingDatabaseURLError(
            "DATABASE_URL environment variable is not set. Please configure DATABASE_URL in your environment or root .env file."
        )

    def __bool__(self):
        return False

    def __repr__(self):
        return f"<MissingDatabaseURLSentinel for {self._name}>"


# Build the database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL")


def apply_ssl_for_remote(url: str) -> str:
    """Auto-enable SSL for remote connections — required by Azure PostgreSQL.

    Handles URLs that already contain query params by using '&' as separator.
    Only skips adding SSL if an explicit ssl=/sslmode= key is already present.
    """
    try:
        parsed = urllib.parse.urlparse(url)
        hostname = parsed.hostname
    except Exception:
        hostname = None

    if hostname:
        hostname_lower = hostname.lower()
        if hostname_lower in ("localhost", "127.0.0.1", "[::1]", "::1"):
            return url

    has_ssl = "sslmode=" in url or "ssl=" in url
    if has_ssl:
        return url
    sep = "&" if "?" in url else "?"
    ssl_param = "sslmode=require" if "psycopg2" in url else "ssl=require"
    return f"{url}{sep}{ssl_param}"


if DATABASE_URL:
    DATABASE_URL = apply_ssl_for_remote(DATABASE_URL)

    # Create the asynchronous database engine with enterprise connection pooling
    async_engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        future=True,
        pool_size=20,
        max_overflow=10,
        pool_pre_ping=True,
    )

    # Build a non-blocking session factory
    async_session_maker = sessionmaker(bind=async_engine, class_=AsyncSession, expire_on_commit=False)
else:
    async_engine = MissingDatabaseURLSentinel("async_engine")  # type: ignore[assignment]
    async_session_maker = MissingDatabaseURLSentinel("async_session_maker")  # type: ignore[assignment]


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    if not async_session_maker:
        raise MissingDatabaseURLError(
            "DATABASE_URL environment variable is not set. Database connection cannot be established."
        )
    async with async_session_maker() as session:
        yield session
