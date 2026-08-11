import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession


@pytest.fixture(autouse=True)
def _backend_api_key(monkeypatch):
    monkeypatch.setenv("BACKEND_API_KEY", "backend-test-key-that-is-at-least-32-characters")


@pytest_asyncio.fixture()
async def db_maker(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite://", echo=False)
    maker = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    from shared_utils import db_connection

    monkeypatch.setattr(db_connection, "async_session_maker", maker)
    yield maker, engine
    await engine.dispose()
