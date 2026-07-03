import pytest
import sys
from pathlib import Path
from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parent.parent))
from main import app
from datetime import datetime, timezone
import json

client = TestClient(app)

def test_ingest_simulated_trade_success():
    payload = {
        "market_hash_name": "Test Item (Factory New)",
        "purchase_price_cents": 1000,
        "estimated_profit_cents": 500,
        "trigger_z_score": -3.5
    }

    # Test the ingestion endpoint
    response = client.post("/api/v1/ingest/trade", json=payload)
    
    # Assert successful processing
    assert response.status_code == 201
    assert response.json()["status"] == "SUCCESS"
