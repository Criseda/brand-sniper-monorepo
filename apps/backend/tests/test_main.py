import asyncio

import pytest
from fastapi.testclient import TestClient
from shared_utils.db_connection import DatabaseConnectionError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

_test_engine = create_async_engine("sqlite+aiosqlite://", echo=False)
_test_session_maker = async_sessionmaker(bind=_test_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture(scope="module", autouse=True)
def _dispose_test_engine():
    yield
    asyncio.run(_test_engine.dispose())


@pytest.fixture(name="client")
def client_fixture(monkeypatch):
    from shared_utils import db_connection

    import main as backend_main

    backend_main.engine = _test_engine
    monkeypatch.setattr(db_connection, "async_session_maker", _test_session_maker)

    from main import app

    with TestClient(app, raise_server_exceptions=False) as client:
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


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"market_hash_name": "Test Item"}, id="missing_field"),
        pytest.param(
            {
                "market_hash_name": "Test Item",
                "purchase_price_cents": 1000,
                "estimated_profit_cents": 500,
                "trigger_z_score": "not-a-number",
            },
            id="invalid_z_score_type",
        ),
    ],
)
def test_ingest_trade_invalid_payload_returns_422(client, payload):
    response = client.post("/api/v1/ingest/trade", json=payload)
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


def test_market_context_unknown_item_returns_404(client):
    response = client.get("/api/v1/market/context/Not A Real Item")

    assert response.status_code == 404
    assert response.json()["detail"] == "Item not found"
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["status"] == 404
    assert response.json()["instance"].startswith("urn:uuid:")


def test_market_context_database_failure_returns_503(client, monkeypatch):
    import main as backend_main

    async def fail_market_context(_market_hash_name: str):
        raise DatabaseConnectionError("postgres password leaked here")

    monkeypatch.setattr(backend_main, "get_item_market_context", fail_market_context)

    response = client.get("/api/v1/market/context/Item")

    assert response.status_code == 503
    assert response.json()["detail"] == "The database service is temporarily unavailable."
    assert "password" not in response.text


def test_search_trends_database_failure_returns_503(client, monkeypatch):
    import main as backend_main

    async def fail_search(_query: str):
        raise DatabaseConnectionError("database down")

    monkeypatch.setattr(backend_main, "query_macro_trends", fail_search)

    response = client.post("/api/v1/market/search-trends", json={"query": "knife"})

    assert response.status_code == 503
    assert response.json()["status"] == 503


def test_bulk_ingestion_database_failure_returns_503(client, monkeypatch):
    import main as backend_main

    async def fail_item_resolution(*_args):
        raise DatabaseConnectionError("database down")

    monkeypatch.setattr(backend_main, "get_or_create_item_id", fail_item_resolution)

    response = client.post(
        "/api/v1/ingest/bulk",
        json={
            "source": "skinport",
            "ticks": [{"market_hash_name": "Item", "price_cents": 100, "timestamp": 1700000000}],
        },
    )

    assert response.status_code == 503
    assert response.json()["status"] == 503


def test_search_trends_unexpected_failure_returns_safe_500(client, monkeypatch):
    import main as backend_main

    async def fail_search(_query: str):
        raise RuntimeError("internal implementation detail")

    monkeypatch.setattr(backend_main, "query_macro_trends", fail_search)

    response = client.post("/api/v1/market/search-trends", json={"query": "knife"})

    assert response.status_code == 500
    assert response.json()["detail"] == "An unexpected internal error occurred."
    assert "implementation detail" not in response.text


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        pytest.param("/api/v1/market/search-trends", {"query": "   "}, id="blank_query"),
        pytest.param("/api/v1/ingest/bulk", {"source": "  ", "ticks": []}, id="blank_source"),
    ],
)
def test_blank_text_fields_return_problem_422(client, path, payload):
    response = client.post(path, json=payload)

    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["status"] == 422
    assert body["detail"] == "The request payload is invalid."
    assert body["errors"]


def test_openapi_documents_problem_json_responses(client):
    schema = client.get("/openapi.json").json()
    responses = schema["paths"]["/api/v1/ingest/bulk"]["post"]["responses"]

    assert "application/problem+json" in responses["422"]["content"]
    assert "application/problem+json" in responses["503"]["content"]
    assert "application/problem+json" in responses["500"]["content"]


@pytest.mark.parametrize(
    ("origin", "expected"),
    [
        pytest.param("http://localhost:3000", "http://localhost:3000", id="origin_allowed"),
        pytest.param("http://malicious.com", None, id="origin_disallowed"),
    ],
)
def test_cors_origin(client, origin, expected):
    response = client.options(
        "/health",
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": "GET",
        },
    )
    assert response.headers.get("access-control-allow-origin") == expected
    if expected is not None:
        assert response.headers.get("access-control-allow-credentials") == "true"
