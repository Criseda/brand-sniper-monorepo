import pytest
import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.append(str(Path(__file__).resolve().parent.parent))
from executor import PaperExecutor

@pytest.mark.asyncio
async def test_paper_executor_sends_payload():
    executor = PaperExecutor("http://mock-backend:8080")
    assert executor.trade_ingest_url == "http://mock-backend:8080/api/v1/ingest/trade"

    market_hash_name = "AK-47 | Redline (Field-Tested)"
    purchase_price = 1000
    est_profit = 500
    z_score = -2.5

    # Mock the internal private method to avoid making actual HTTP requests
    with patch.object(executor, '_send_to_backend', new_callable=AsyncMock) as mock_send:
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
