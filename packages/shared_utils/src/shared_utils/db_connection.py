import os
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

# Build the database URL from environment, with local fallback
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/brand_sniper")


def apply_ssl_for_remote(url: str) -> str:
    """Auto-enable SSL for remote connections — required by Azure PostgreSQL.

    Handles URLs that already contain query params by using '&' as separator.
    Only skips adding SSL if an explicit ssl=/sslmode= key is already present.
    """
    if "localhost" in url or "127.0.0.1" in url:
        return url
    has_ssl = "sslmode=" in url or "ssl=" in url
    if has_ssl:
        return url
    sep = "&" if "?" in url else "?"
    ssl_param = "sslmode=require" if "psycopg2" in url else "ssl=require"
    return f"{url}{sep}{ssl_param}"


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


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session
