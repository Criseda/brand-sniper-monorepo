import os
from collections.abc import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

# Fall back to local development database if environment variable isn't set yet
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/brand_sniper"
)

# Create the asynchronous database engine with enterprise connection pooling
async_engine = create_async_engine(
    DATABASE_URL,
    echo=False,          # Set to True if you want raw SQL queries logged to stdout
    future=True,
    pool_size=20,        # Keeps up to 20 connections open for active tasks
    max_overflow=10,     # Dynamically provisions 10 extra connections under spike loads
    pool_pre_ping=True   # Automatically resets dead or dropped connections
)

# Build a non-blocking session factory
async_session_maker = sessionmaker(
    bind=async_engine, 
    class_=AsyncSession, 
    expire_on_commit=False
)

async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager dependency providing an isolated, safe 
    database transaction session that automatically closes.
    """
    async with async_session_maker() as session:
        yield session