import os
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

# Build the database URL from environment, with local fallback
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/brand_sniper"
)

# Auto-enable SSL for remote connections — required by Azure PostgreSQL
if "localhost" not in DATABASE_URL and "127.0.0.1" not in DATABASE_URL:
    if "?" not in DATABASE_URL and "ssl" not in DATABASE_URL:
        ssl_param = "sslmode=require" if "psycopg2" in DATABASE_URL else "ssl=require"
        DATABASE_URL += f"?{ssl_param}"

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
