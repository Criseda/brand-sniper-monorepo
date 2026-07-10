import json
import os
from unittest.mock import MagicMock, patch

import pytest

os.environ["GROQ_API_KEY"] = "MOCK_API_KEY"

from evaluate_performance import evaluate_trade
from shared_utils.models import SimulatedTrade


@pytest.mark.asyncio
@patch("evaluate_performance.MlflowClient")
@patch("evaluate_performance.openai_client")
@patch("evaluate_performance.get_experiment_id", return_value="1")
async def test_evaluate_trade(mock_get_experiment_id, mock_openai_client, mock_mlflow_client_cls):
    mock_client = MagicMock()
    mock_run = MagicMock()
    mock_run.info.run_id = "test_run_id"
    mock_client.create_run.return_value = mock_run
    mock_mlflow_client_cls.return_value = mock_client

    mock_trade = SimulatedTrade(item_id=1, purchase_price_cents=1000, estimated_profit_cents=500, trigger_z_score=-3.0)

    # Phase 1: no tool calls (stop)
    mock_phase1 = MagicMock()
    mock_choice1 = MagicMock()
    mock_choice1.finish_reason = "stop"
    mock_choice1.message.content = "I checked the market and here is my analysis."
    mock_choice1.message.tool_calls = None
    mock_phase1.choices = [mock_choice1]

    # Phase 2: raw JSON response (no instructor)
    mock_phase2 = MagicMock()
    mock_choice2 = MagicMock()
    mock_choice2.message.content = json.dumps(
        {"confidence_score": 25, "reasoning": "Live market floor is 900 cents, bots baseline of 1500 is stale."}
    )
    mock_phase2.choices = [mock_choice2]

    mock_openai_client.chat.completions.create.side_effect = [mock_phase1, mock_phase2]

    await evaluate_trade(mock_trade, "AK-47 | Redline (Field-Tested)", None)

    assert mock_openai_client.chat.completions.create.call_count == 2
    assert mock_openai_client.chat.completions.create.call_args_list[0][1]["model"] == "qwen/qwen3-32b"

    mock_client.log_metric.assert_called_with("test_run_id", "cfo_confidence_score", 25)
    mock_client.set_tag.assert_called_with("test_run_id", "eval_status", "REJECTED")
    mock_client.log_artifact.assert_called_once()
    args, _ = mock_client.log_artifact.call_args
    assert args[0] == "test_run_id"
    assert args[1].endswith("cfo_reasoning.txt")
    mock_client.set_terminated.assert_called_with("test_run_id", status="FINISHED")


@pytest.mark.asyncio
@patch("evaluate_performance.MlflowClient")
@patch("evaluate_performance.openai_client")
@patch("evaluate_performance.get_experiment_id", return_value="1")
async def test_evaluate_trade_with_tool_calls(mock_get_experiment_id, mock_openai_client, mock_mlflow_client_cls):
    mock_client = MagicMock()
    mock_run = MagicMock()
    mock_run.info.run_id = "test_run_id"
    mock_client.create_run.return_value = mock_run
    mock_mlflow_client_cls.return_value = mock_client

    mock_trade = SimulatedTrade(item_id=1, purchase_price_cents=1000, estimated_profit_cents=500, trigger_z_score=-3.0)

    # First response: tool call
    mock_tool_choice = MagicMock()
    mock_tool_choice.finish_reason = "tool_calls"
    mock_tool = MagicMock()
    mock_tool.id = "call_1"
    mock_tool.function.name = "fetch_live_market_floor"
    mock_tool.function.arguments = '{"market_hash_name": "AK-47 | Redline (Field-Tested)"}'
    mock_tool_choice.message.content = ""
    mock_tool_choice.message.tool_calls = [mock_tool]
    mock_tool_response = MagicMock()
    mock_tool_response.choices = [mock_tool_choice]

    # Second response: message (tools done, stop)
    mock_msg_choice = MagicMock()
    mock_msg_choice.finish_reason = "stop"
    mock_msg_choice.message.content = "Market floor is lower than baseline."
    mock_msg_choice.message.tool_calls = None
    mock_msg_response = MagicMock()
    mock_msg_response.choices = [mock_msg_choice]

    # Third response: raw JSON (no instructor)
    mock_final = MagicMock()
    mock_final_choice = MagicMock()
    mock_final_choice.message.content = json.dumps({"confidence_score": 50, "reasoning": "Evaluated after tool calls."})
    mock_final.choices = [mock_final_choice]

    mock_openai_client.chat.completions.create.side_effect = [
        mock_tool_response,
        mock_msg_response,
        mock_final,
    ]

    await evaluate_trade(mock_trade, "AK-47 | Redline (Field-Tested)", None)

    assert mock_openai_client.chat.completions.create.call_count == 3
    mock_client.log_metric.assert_called_with("test_run_id", "cfo_confidence_score", 50)


def test_verify_float_value():
    from tools import verify_float_value

    result = json.loads(verify_float_value("AK-47 | Redline (Field-Tested)", 0.001))
    assert result["float_quality"] == "Exceptional"
    assert result["premium_multiplier"] == 1.5
    assert result["wear_tier"] == "Factory New"

    result = json.loads(verify_float_value("AWP | Asiimov (Field-Tested)", 0.96))
    assert result["float_quality"] == "Exceptional"
    assert result["premium_multiplier"] == 1.3
    assert result["wear_tier"] == "Battle-Scarred"

    result = json.loads(verify_float_value("M4A4 | Howl (Factory New)", 0.30))
    assert result["float_quality"] == "Standard"
    assert result["premium_multiplier"] == 1.0
    assert result["wear_tier"] == "Field-Tested"

    result = json.loads(verify_float_value("AK-47 | Redline (Factory New)", 0.05))
    assert result["float_quality"] == "Good"
    assert result["premium_multiplier"] == 1.1

    result = json.loads(verify_float_value("AK-47 | Redline (Minimal Wear)", 0.075))
    assert result["float_quality"] == "Good"
    assert result["premium_multiplier"] == 1.15
    assert result["wear_tier"] == "Minimal Wear"


def test_extract_retry_after():
    from evaluate_performance import _extract_retry_after

    assert _extract_retry_after("Please try again in 5.0s") == 6.0
    assert _extract_retry_after("Please try again in 10s") == 11.0
    assert _extract_retry_after("Retry after 3.5s") == 4.5
    assert _extract_retry_after("retry after 2s") == 3.0
    assert _extract_retry_after("Some other error") == 3.0
