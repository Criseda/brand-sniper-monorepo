import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession


@pytest.fixture()
def db_maker(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite://", echo=False)
    maker = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    from shared_utils import db_connection

    monkeypatch.setattr(db_connection, "async_session_maker", maker)
    return maker, engine
