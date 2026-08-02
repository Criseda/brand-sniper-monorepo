import asyncio
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
from executor import PaperExecutor, _get_session, close_http_session


@pytest.mark.asyncio
async def test_paper_executor_sends_payload():
    executor = PaperExecutor("http://mock-backend:8080")
    assert executor.trade_ingest_url == "http://mock-backend:8080/api/v1/ingest/trade"

    market_hash_name = "AK-47 | Redline (Field-Tested)"
    purchase_price = 1000
    est_profit = 500
    z_score = -2.5

    # Mock the internal private method to avoid making actual HTTP requests
    with patch.object(executor, "_send_to_backend", new_callable=AsyncMock) as mock_send:
        # Call execute (which creates a task)
        await executor.execute(market_hash_name, purchase_price, est_profit, z_score)

        # Let the event loop run briefly so the created task can execute
        await asyncio.sleep(0.01)

        # Assert payload was constructed correctly and sent
        mock_send.assert_called_once()
        called_payload = mock_send.call_args[0][0]

        assert called_payload["market_hash_name"] == market_hash_name
        assert called_payload["purchase_price_cents"] == purchase_price
        assert called_payload["estimated_profit_cents"] == est_profit
        assert called_payload["trigger_z_score"] == -2.5


class _Response:
    status = 500

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


class _Client:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.posted = []

    def post(self, url, json=None):
        self.posted.append((url, json))
        if self.error is not None:
            raise self.error
        return self.response


@pytest.mark.asyncio
async def test_send_to_backend_logs_rejection_on_non_2xx(mocker):
    client = _Client(response=_Response())
    mocker.patch("executor._get_session", return_value=client)
    executor = PaperExecutor("http://mock-backend:8080")

    await executor._send_to_backend({"market_hash_name": "Item"})

    assert client.posted == [("http://mock-backend:8080/api/v1/ingest/trade", {"market_hash_name": "Item"})]


@pytest.mark.asyncio
async def test_send_to_backend_survives_connection_error(mocker):
    client = _Client(error=aiohttp.ClientError("connection refused"))
    mocker.patch("executor._get_session", return_value=client)
    executor = PaperExecutor("http://mock-backend:8080")

    await executor._send_to_backend({"market_hash_name": "Item"})


@pytest.mark.asyncio
async def test_get_session_reuses_singleton():
    first = await _get_session()
    second = await _get_session()
    try:
        assert first is second
    finally:
        await close_http_session()


@pytest.mark.asyncio
async def test_close_http_session_closes_and_resets():
    session = await _get_session()

    await close_http_session()

    assert session.closed
