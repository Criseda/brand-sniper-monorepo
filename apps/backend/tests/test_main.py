import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

_test_engine = create_async_engine("sqlite+aiosqlite://", echo=False)
_test_session_maker = sessionmaker(bind=_test_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture(name="client")
def client_fixture():
    backend_dir = str(Path(__file__).resolve().parent.parent)
    sys.path.insert(0, backend_dir)

    import main as backend_main

    backend_main.engine = _test_engine
    backend_main.AsyncSessionLocal = _test_session_maker

    from main import app

    with TestClient(app) as client:
        yield client


def test_ingest_simulated_trade_success(client):
    payload = {
        "market_hash_name": "Test Item (Factory New)",
        "purchase_price_cents": 1000,
        "estimated_profit_cents": 500,
        "trigger_z_score": -3.5,
    }

    response = client.post("/api/v1/ingest/trade", json=payload)

    assert response.status_code == 201
    assert response.json()["status"] == "SUCCESS"


def test_ingest_trade_missing_field_returns_422(client):
    response = client.post("/api/v1/ingest/trade", json={"market_hash_name": "Test Item"})
    assert response.status_code == 422


def test_ingest_trade_invalid_z_score_type_returns_422(client):
    response = client.post(
        "/api/v1/ingest/trade",
        json={
            "market_hash_name": "Test Item",
            "purchase_price_cents": 1000,
            "estimated_profit_cents": 500,
            "trigger_z_score": "not-a-number",
        },
    )
    assert response.status_code == 422


def test_ingest_bulk_success(client):
    payload = {
        "source": "test_source",
        "ticks": [
            {"market_hash_name": "Item One (Factory New)", "price_cents": 1500, "timestamp": 1700000000},
            {"market_hash_name": "Item Two (Minimal Wear)", "price_cents": 2500, "timestamp": 1700000001},
        ],
    }
    response = client.post("/api/v1/ingest/bulk", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "SUCCESS"
    assert data["records_processed"] == 2


def test_ingest_bulk_empty_ticks(client):
    payload = {"source": "test_source", "ticks": []}
    response = client.post("/api/v1/ingest/bulk", json=payload)
    assert response.status_code == 201
    assert response.json()["status"] == "SKIPPED"


def test_ingest_bulk_missing_source_returns_422(client):
    response = client.post(
        "/api/v1/ingest/bulk", json={"ticks": [{"market_hash_name": "Item", "price_cents": 100, "timestamp": 1700000000}]}
    )
    assert response.status_code == 422
