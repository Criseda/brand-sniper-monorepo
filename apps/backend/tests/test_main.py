import asyncio
import importlib.util
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from shared_utils import BACKEND_API_KEY_HEADER, BackendApiKeyConfigError
from shared_utils.db_connection import DatabaseConnectionError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

_test_engine = create_async_engine("sqlite+aiosqlite://", echo=False)
_test_session_maker = async_sessionmaker(bind=_test_engine, class_=AsyncSession, expire_on_commit=False)
_test_backend_api_key = "backend-test-key-that-is-at-least-32-characters"


def load_backend_main():
    module_path = Path(__file__).resolve().parents[1] / "main.py"
    spec = importlib.util.spec_from_file_location("backend_main_test", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load backend main module for tests")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


backend_main = load_backend_main()


@pytest.fixture(scope="module", autouse=True)
def _dispose_test_engine():
    yield
    asyncio.run(_test_engine.dispose())


@pytest.fixture(name="client")
def client_fixture(monkeypatch):
    from shared_utils import db_connection

    monkeypatch.setenv("BACKEND_API_KEY", _test_backend_api_key)
    backend_main.engine = _test_engine
    monkeypatch.setattr(db_connection, "async_session_maker", _test_session_maker)

    with TestClient(
        backend_main.app,
        headers={BACKEND_API_KEY_HEADER: _test_backend_api_key},
        raise_server_exceptions=False,
    ) as client:
        yield client


def test_api_routes_require_valid_api_key(client):
    response = client.get(
        "/api/v1/market/context/Test Item",
        headers={BACKEND_API_KEY_HEADER: "wrong-key"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "A valid backend API key is required."


def test_api_routes_reject_missing_api_key(client):
    configured_key = client.headers.pop(BACKEND_API_KEY_HEADER)
    try:
        response = client.get("/api/v1/market/context/Test Item")
    finally:
        client.headers[BACKEND_API_KEY_HEADER] = configured_key

    assert response.status_code == 401


def test_health_remains_public(client):
    configured_key = client.headers.pop(BACKEND_API_KEY_HEADER)
    try:
        response = client.get("/health")
    finally:
        client.headers[BACKEND_API_KEY_HEADER] = configured_key

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_backend_fails_startup_without_api_key(monkeypatch):
    monkeypatch.delenv("BACKEND_API_KEY")

    with pytest.raises(BackendApiKeyConfigError, match="is not set"):
        with TestClient(backend_main.app):
            pass


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


def test_ingest_bulk_replay_is_idempotent(client):
    batch_id = str(uuid4())
    payload = {
        "batch_id": batch_id,
        "source": "test_source",
        "ticks": [{"market_hash_name": "Replay Item", "price_cents": 1500, "timestamp": 1700000000}],
    }

    first = client.post("/api/v1/ingest/bulk", json=payload)
    replay = client.post("/api/v1/ingest/bulk", json=payload)

    assert first.status_code == 201
    assert first.json() == {"status": "SUCCESS", "records_processed": 1}
    assert replay.status_code == 201
    assert replay.json() == {"status": "DUPLICATE", "records_processed": 0}


def test_ingest_bulk_rejects_reused_batch_id_with_different_payload(client):
    batch_id = str(uuid4())
    payload = {
        "batch_id": batch_id,
        "source": "test_source",
        "ticks": [{"market_hash_name": "Conflict Item", "price_cents": 1500, "timestamp": 1700000000}],
    }
    changed_payload = {
        **payload,
        "ticks": [{"market_hash_name": "Conflict Item", "price_cents": 1600, "timestamp": 1700000000}],
    }

    assert client.post("/api/v1/ingest/bulk", json=payload).status_code == 201
    response = client.post("/api/v1/ingest/bulk", json=changed_payload)

    assert response.status_code == 409
    assert "different payload" in response.json()["detail"]


@pytest.mark.asyncio
async def test_concurrent_bulk_requests_keep_new_item_ids_local_until_commit(monkeypatch):
    from schemas import BulkIngestionPayload

    market_hash_name = "Concurrent Uncommitted Item"
    backend_main.item_cache.pop(market_hash_name, None)
    both_waiting_to_commit = asyncio.Event()
    allow_commits = asyncio.Event()
    waiting_count = 0

    class ScalarResult:
        def scalar(self):
            return 424242

    class FakeSession:
        async def exec(self, _statement, params=None):
            return None if params is not None else ScalarResult()

    @asynccontextmanager
    async def delayed_commit_scope():
        nonlocal waiting_count
        yield FakeSession()
        waiting_count += 1
        if waiting_count == 2:
            both_waiting_to_commit.set()
        await allow_commits.wait()

    async def register_batch(_session, _payload):
        return True

    monkeypatch.setattr(backend_main, "session_scope", delayed_commit_scope)
    monkeypatch.setattr(backend_main, "_register_ingestion_batch", register_batch)
    payloads = [
        BulkIngestionPayload.model_validate(
            {
                "batch_id": str(uuid4()),
                "source": "skinport",
                "ticks": [{"market_hash_name": market_hash_name, "price_cents": price, "timestamp": 1700000000}],
            }
        )
        for price in (1000, 1100)
    ]

    requests = [asyncio.create_task(backend_main.process_bulk_ingestion(payload)) for payload in payloads]
    try:
        await asyncio.wait_for(both_waiting_to_commit.wait(), timeout=1)
        assert market_hash_name not in backend_main.item_cache
        allow_commits.set()
        responses = await asyncio.gather(*requests)
        assert responses == [
            {"status": "SUCCESS", "records_processed": 1},
            {"status": "SUCCESS", "records_processed": 1},
        ]
        assert backend_main.item_cache[market_hash_name] == 424242
    finally:
        allow_commits.set()
        await asyncio.gather(*requests, return_exceptions=True)
        backend_main.item_cache.pop(market_hash_name, None)


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
    async def fail_market_context(_market_hash_name: str):
        raise DatabaseConnectionError("postgres password leaked here")

    monkeypatch.setattr(backend_main, "get_item_market_context", fail_market_context)

    response = client.get("/api/v1/market/context/Item")

    assert response.status_code == 503
    assert response.json()["detail"] == "The database service is temporarily unavailable."
    assert "password" not in response.text


def test_search_trends_database_failure_returns_503(client, monkeypatch):
    async def fail_search(_query: str):
        raise DatabaseConnectionError("database down")

    monkeypatch.setattr(backend_main, "query_macro_trends", fail_search)

    response = client.post("/api/v1/market/search-trends", json={"query": "knife"})

    assert response.status_code == 503
    assert response.json()["status"] == 503


def test_bulk_ingestion_database_failure_returns_503(client, monkeypatch):
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


def test_item_cache_is_not_updated_when_commit_fails(client, monkeypatch):
    market_hash_name = "Uncommitted Cache Item"
    backend_main.item_cache.pop(market_hash_name, None)

    class FakeSession:
        def add(self, _instance):
            return None

    @asynccontextmanager
    async def fail_during_commit():
        yield FakeSession()
        raise DatabaseConnectionError("commit failed")

    async def resolve_item(_session, name, pending_items):
        pending_items[name] = 999999
        return 999999

    monkeypatch.setattr(backend_main, "session_scope", fail_during_commit)
    monkeypatch.setattr(backend_main, "get_or_create_item_id", resolve_item)

    response = client.post(
        "/api/v1/ingest/trade",
        json={
            "market_hash_name": market_hash_name,
            "purchase_price_cents": 1000,
            "estimated_profit_cents": 500,
            "trigger_z_score": -3.5,
        },
    )

    assert response.status_code == 503
    assert market_hash_name not in backend_main.item_cache


def test_search_trends_unexpected_failure_returns_safe_500(client, monkeypatch):
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
    operation = schema["paths"]["/api/v1/ingest/bulk"]["post"]
    responses = operation["responses"]

    assert operation["security"] == [{"BackendApiKey": []}]
    assert schema["components"]["securitySchemes"]["BackendApiKey"]["name"] == BACKEND_API_KEY_HEADER
    for path, path_item in schema["paths"].items():
        if path.startswith("/api/v1/"):
            for method, api_operation in path_item.items():
                if method in {"delete", "get", "patch", "post", "put"}:
                    assert api_operation["security"] == [{"BackendApiKey": []}]
    assert "application/problem+json" in responses["409"]["content"]
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
