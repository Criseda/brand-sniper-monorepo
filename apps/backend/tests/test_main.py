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


def test_ingest_simulated_trade_records_the_bought_listing(client):
    from shared_utils.models import SimulatedTrade
    from sqlmodel import select

    payload = {
        "market_hash_name": "Listing Trade Item (Field-Tested)",
        "purchase_price_cents": 1000,
        "estimated_profit_cents": 500,
        "trigger_z_score": -3.5,
        "listing_id": "58903454",
        "float_value": 0.36,
    }

    response = client.post("/api/v1/ingest/trade", json=payload)

    assert response.status_code == 201
    trades = asyncio.run(_fetch_all(select(SimulatedTrade).where(SimulatedTrade.listing_id == "58903454")))
    assert len(trades) == 1
    assert trades[0].float_value == pytest.approx(0.36)


def test_ingest_simulated_trade_stores_estimate_basis_and_missing_estimate(client):
    from shared_utils.models import SimulatedTrade
    from sqlmodel import select

    profit_before = backend_main.paper_trading_estimated_profit_total._value.get()
    payload = {
        "market_hash_name": "No Baseline Item (Field-Tested)",
        "purchase_price_cents": 1000,
        "estimated_profit_cents": None,
        "profit_estimate_basis": "net_of_seller_fee",
        "trigger_z_score": -3.5,
        "listing_id": "70000001",
    }

    response = client.post("/api/v1/ingest/trade", json=payload)

    assert response.status_code == 201
    trades = asyncio.run(_fetch_all(select(SimulatedTrade).where(SimulatedTrade.listing_id == "70000001")))
    assert trades[0].estimated_profit_cents is None
    assert trades[0].profit_estimate_basis == "net_of_seller_fee"
    # A trade without an estimate must not move the estimated-profit metric.
    assert backend_main.paper_trading_estimated_profit_total._value.get() == profit_before


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"market_hash_name": "Test Item"}, id="missing_field"),
        pytest.param(
            {"market_hash_name": "Test Item", "purchase_price_cents": 1000, "trigger_z_score": -3.5},
            id="estimate_must_be_sent_even_when_null",
        ),
        pytest.param(
            {
                "market_hash_name": "Test Item",
                "purchase_price_cents": 1000,
                "estimated_profit_cents": 500,
                "profit_estimate_basis": "x" * 33,
                "trigger_z_score": -3.5,
            },
            id="basis_too_long",
        ),
        pytest.param(
            {
                "market_hash_name": "Test Item",
                "purchase_price_cents": 1000,
                "estimated_profit_cents": 500,
                "trigger_z_score": -3.5,
                "float_value": 1.5,
            },
            id="float_out_of_range",
        ),
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
    assert first.json() == {"status": "SUCCESS", "records_processed": 1, "feed_events_processed": 0}
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


def _listing_payload(batch_id: str) -> dict:
    return {
        "batch_id": batch_id,
        "source": "skinport",
        "ticks": [
            {
                "market_hash_name": "Listing Level Item (Field-Tested)",
                "price_cents": 397,
                "timestamp": 1790000000,
                "event_type": "sold",
                "listing_id": "58903454",
                "float_value": 0.36,
                "pattern": 415,
                "paint_index": 1035,
                "stickers": [{"name": "9z Team (Glitter) | Antwerp 2022", "slot": 2}],
                "listing_url": "https://skinport.com/item/listing-level-item-field-tested",
            },
            {"market_hash_name": "Listing Level Item (Field-Tested)", "price_cents": 450, "timestamp": 1790000001},
        ],
        "feed_events": [
            {"event_type": "sold", "received_at_ms": 1790000000123, "payload": {"eventType": "sold", "sales": [{"id": 0}]}},
        ],
    }


async def _fetch_all(statement):
    async with _test_session_maker() as session:
        return list((await session.exec(statement)).all())


def test_ingest_bulk_persists_listing_fields_and_raw_feed_events(client):
    from shared_utils.models import FeedEvent, LiveMarketTick
    from sqlmodel import select

    batch_id = str(uuid4())
    response = client.post("/api/v1/ingest/bulk", json=_listing_payload(batch_id))

    assert response.status_code == 201
    assert response.json() == {"status": "SUCCESS", "records_processed": 2, "feed_events_processed": 1}

    ticks = asyncio.run(_fetch_all(select(LiveMarketTick).where(LiveMarketTick.price_cents.in_([397, 450]))))
    sold = next(tick for tick in ticks if tick.price_cents == 397)
    snapshot = next(tick for tick in ticks if tick.price_cents == 450)
    assert (sold.event_type, sold.listing_id, sold.pattern, sold.paint_index) == ("sold", "58903454", 415, 1035)
    assert sold.float_value == pytest.approx(0.36)
    assert sold.stickers == [{"name": "9z Team (Glitter) | Antwerp 2022", "slot": 2}]
    assert sold.listing_url == "https://skinport.com/item/listing-level-item-field-tested"
    assert (snapshot.event_type, snapshot.listing_id, snapshot.stickers) == (None, None, None)

    events = asyncio.run(_fetch_all(select(FeedEvent).where(FeedEvent.source == "skinport")))
    assert len(events) == 1
    assert events[0].event_type == "sold"
    assert events[0].payload == {"eventType": "sold", "sales": [{"id": 0}]}
    assert events[0].received_at.isoformat() == "2026-09-21T14:13:20.123000"

    replay = client.post("/api/v1/ingest/bulk", json=_listing_payload(batch_id))
    assert replay.json() == {"status": "DUPLICATE", "records_processed": 0}
    assert len(asyncio.run(_fetch_all(select(FeedEvent).where(FeedEvent.source == "skinport")))) == 1


def test_ingest_bulk_accepts_feed_event_only_batch(client):
    payload = {
        "batch_id": str(uuid4()),
        "source": "feed-only",
        "ticks": [],
        "feed_events": [{"event_type": "unknown", "received_at_ms": 1790000000000, "payload": {}}],
    }

    response = client.post("/api/v1/ingest/bulk", json=payload)

    assert response.status_code == 201
    assert response.json() == {"status": "SUCCESS", "records_processed": 0, "feed_events_processed": 1}


@pytest.mark.parametrize(
    "tick_override",
    [
        pytest.param({"float_value": 1.5}, id="float_above_one"),
        pytest.param({"pattern": -1}, id="negative_pattern"),
        pytest.param({"listing_id": "x" * 65}, id="listing_id_too_long"),
    ],
)
def test_ingest_bulk_rejects_invalid_listing_fields(client, tick_override):
    tick = {"market_hash_name": "Item", "price_cents": 100, "timestamp": 1700000000, **tick_override}

    response = client.post("/api/v1/ingest/bulk", json={"source": "skinport", "ticks": [tick]})

    assert response.status_code == 422


def test_bulk_digest_is_unchanged_for_pre_listing_batches():
    """A batch recorded before #232 must still hash identically, so its replay is a DUPLICATE, not a 409."""
    import hashlib
    import json

    from schemas import BulkIngestionPayload

    ticks = [{"market_hash_name": "Legacy Item", "price_cents": 1500, "timestamp": 1700000000}]
    legacy_digest = hashlib.sha256(
        json.dumps({"source": "skinport", "ticks": ticks}, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    payload = BulkIngestionPayload.model_validate({"source": "skinport", "ticks": ticks})

    assert backend_main._bulk_payload_digest(payload) == legacy_digest


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
            {"status": "SUCCESS", "records_processed": 1, "feed_events_processed": 0},
            {"status": "SUCCESS", "records_processed": 1, "feed_events_processed": 0},
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


def _store_build(venue: str, built_at_iso: str, rows: list[dict]) -> int:
    from datetime import datetime

    from shared_utils.models import BaselineBuild, VenueBaseline

    async def store() -> int:
        async with _test_session_maker() as session:
            build = BaselineBuild(
                venue=venue, method="sales-history-v1", built_at=datetime.fromisoformat(built_at_iso), item_count=len(rows)
            )
            session.add(build)
            await session.flush()
            assert build.id is not None
            for row in rows:
                session.add(VenueBaseline(build_id=build.id, **row))
            await session.commit()
            return build.id

    return asyncio.run(store())


def _baseline_row(name: str, latest: int) -> dict:
    return {
        "market_hash_name": name,
        "latest_price_cents": latest,
        "rolling_30d_avg_cents": latest,
        "rolling_90d_avg_cents": latest,
        "volatility_cents": 10,
        "support_floor_cents": latest - 20,
        "avg_volume_30d": 1.5,
        "drift_percent": 0.0,
        "volatility_method": "sales_spread",
        "median_24h_cents": None,
        "volume_24h": 0,
        "median_7d_cents": latest,
        "volume_7d": 5,
        "min_30d_cents": latest - 30,
        "volume_30d": 45,
        "volume_90d": 120,
    }


def test_latest_baselines_serves_the_newest_build_with_sticker_prices(client):
    venue = f"venue-{uuid4().hex[:8]}"
    _store_build(venue, "2026-09-25T12:00:00", [_baseline_row("AK-47 | Redline (Field-Tested)", 2700)])
    newest = _store_build(
        venue,
        "2026-09-26T12:00:00",
        [_baseline_row("AK-47 | Redline (Field-Tested)", 2739), _baseline_row("Sticker | Crown (Foil)", 90000)],
    )

    response = client.get(f"/api/v1/baselines/{venue}/latest")

    assert response.status_code == 200
    body = response.json()
    assert body["build_id"] == newest
    assert (body["venue"], body["built_at"], body["item_count"]) == (venue, "2026-09-26T12:00:00", 2)
    assert body["baselines"]["AK-47 | Redline (Field-Tested)"]["latest_price_cents"] == 2739
    assert body["baselines"]["AK-47 | Redline (Field-Tested)"]["coefficient_of_variation"] == round(10 / 2739, 4)
    # Sticker prices are keyed the way a listing names an applied sticker.
    assert body["sticker_prices"] == {"Crown (Foil)": 90000}


def test_latest_baselines_returns_no_content_when_the_caller_is_current(client):
    venue = f"venue-{uuid4().hex[:8]}"
    build_id = _store_build(venue, "2026-09-26T12:00:00", [_baseline_row("AK-47 | Redline (Field-Tested)", 2739)])

    assert client.get(f"/api/v1/baselines/{venue}/latest", params={"after_build_id": build_id}).status_code == 204
    assert client.get(f"/api/v1/baselines/{venue}/latest", params={"after_build_id": build_id - 1}).status_code == 200


def test_latest_baselines_is_not_found_for_a_venue_without_builds(client):
    response = client.get("/api/v1/baselines/nowhere/latest")

    assert response.status_code == 404
    assert "nowhere" in response.json()["detail"]


def test_latest_baselines_requires_the_api_key(client):
    response = client.get("/api/v1/baselines/skinport/latest", headers={BACKEND_API_KEY_HEADER: "wrong-key"})

    assert response.status_code == 401
