import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Mock environment variable for Gemini API Key to bypass init crash
os.environ["GEMINI_API_KEY"] = "MOCK_API_KEY"

from evaluate_performance import evaluate_trade
from shared_utils.models import SimulatedTrade


@patch("evaluate_performance.gemini_client")
@patch("evaluate_performance.mlflow")
@patch("evaluate_performance.get_experiment_id", return_value="1")
def test_evaluate_trade(mock_get_experiment_id, mock_mlflow, mock_gemini_client):
    # Setup mock trade
    mock_trade = SimulatedTrade(item_id=1, purchase_price_cents=1000, estimated_profit_cents=500, trigger_z_score=-3.0)

    # Mock the Gemini API Response
    mock_chat = MagicMock()
    mock_response = MagicMock()
    mock_response.text = json.dumps(
        {"confidence_score": 25, "reasoning": "Live market floor is 900 cents, bots baseline of 1500 is stale."}
    )
    mock_chat.send_message.return_value = mock_response
    mock_gemini_client.chats.create.return_value = mock_chat

    # Run the function
    evaluate_trade(mock_trade, "AK-47 | Redline (Field-Tested)")

    # Verify Gemini was called
    mock_gemini_client.chats.create.assert_called_once()
    mock_chat.send_message.assert_called_once()

    # Verify MLflow tracking was called
    mock_mlflow.start_run.assert_called_once()
    mock_mlflow.log_metric.assert_called_with("cfo_confidence_score", 25)
    mock_mlflow.set_tag.assert_called_with("eval_status", "REJECTED")
    mock_mlflow.log_artifact.assert_called_once()
    args, _ = mock_mlflow.log_artifact.call_args
    assert args[0].endswith("cfo_reasoning.txt")
