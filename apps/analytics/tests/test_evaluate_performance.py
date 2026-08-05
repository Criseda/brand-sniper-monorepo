import json
from unittest.mock import AsyncMock, MagicMock, patch

import evaluate_performance
import pytest
from evaluate_performance import (
    _extract_retry_after,
    _is_tpd_error,
    _json_phase,
    _msg_dict,
    _switch_to_next_model,
    _tool_loop,
    evaluate_trade,
    get_experiment_id,
)
from mlflow.exceptions import MlflowException
from shared_utils.models import SimulatedTrade


def _json_response(score, reasoning):
    resp = MagicMock()
    resp.choices[0].message.content = json.dumps({"confidence_score": score, "reasoning": reasoning})
    resp.choices[0].message.tool_calls = None
    return resp


def _tool_call_response(name, arguments, finish_reason="tool_calls"):
    resp = MagicMock()
    resp.choices[0].finish_reason = finish_reason
    resp.choices[0].message.content = ""
    tc = MagicMock()
    tc.id = "call_1"
    tc.function.name = name
    tc.function.arguments = arguments
    resp.choices[0].message.tool_calls = [tc]
    return resp


def _trade():
    return SimulatedTrade(item_id=1, purchase_price_cents=1000, estimated_profit_cents=500, trigger_z_score=-3.0)


# ---------------------------------------------------------------------------
# get_experiment_id
# ---------------------------------------------------------------------------


@patch("evaluate_performance.MlflowClient")
def test_get_experiment_id_caches_existing(mock_mlflow_client_cls):
    mock_client = MagicMock()
    exp = MagicMock()
    exp.experiment_id = "7"
    mock_client.get_experiment_by_name.return_value = exp
    mock_mlflow_client_cls.return_value = mock_client
    evaluate_performance._experiment_id = None

    assert get_experiment_id() == "7"
    assert get_experiment_id() == "7"
    assert mock_client.get_experiment_by_name.call_count == 1


@patch("evaluate_performance.MlflowClient")
def test_get_experiment_id_creates_when_missing(mock_mlflow_client_cls):
    mock_client = MagicMock()
    mock_client.get_experiment_by_name.return_value = None
    mock_client.create_experiment.return_value = "9"
    mock_mlflow_client_cls.return_value = mock_client
    evaluate_performance._experiment_id = None

    assert get_experiment_id() == "9"
    mock_client.create_experiment.assert_called_once_with("cfo-evaluation")


@patch("evaluate_performance.MlflowClient")
def test_get_experiment_id_falls_back_on_error(mock_mlflow_client_cls):
    mock_client = MagicMock()
    mock_client.get_experiment_by_name.side_effect = MlflowException("mlflow down")
    mock_mlflow_client_cls.return_value = mock_client
    evaluate_performance._experiment_id = None

    assert get_experiment_id() == "1"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def test_is_tpd_error():
    assert _is_tpd_error("TPD limit reached") is True
    assert _is_tpd_error("exceeded your tokens per day limit") is True
    assert _is_tpd_error("rate limited") is False


@pytest.mark.asyncio
async def test_sleep_yields_without_delay():
    await evaluate_performance._sleep(0)


def test_switch_to_next_model_cycles_until_exhausted():
    evaluate_performance._model_index_var.set(0)

    assert _switch_to_next_model() == "llama-3.3-70b-versatile"
    assert _switch_to_next_model() == "llama-3.1-8b-instant"
    assert _switch_to_next_model() == "openai/gpt-oss-20b"
    assert _switch_to_next_model() is None


def test_msg_dict_with_tool_calls():
    msg = MagicMock()
    msg.role = "assistant"
    msg.content = "thinking"
    tc = MagicMock()
    tc.model_dump.return_value = {"id": "call_1"}
    msg.tool_calls = [tc]

    entry = _msg_dict(msg)

    assert entry["role"] == "assistant"
    assert entry["content"] == "thinking"
    assert entry["tool_calls"] == [{"id": "call_1"}]


@pytest.mark.asyncio
async def test_json_phase_empty_content_raises(monkeypatch):
    resp = MagicMock()
    resp.choices[0].message.content = None
    monkeypatch.setattr(evaluate_performance, "_call", AsyncMock(return_value=resp))

    with pytest.raises(ValueError, match="empty content"):
        await _json_phase([])


@pytest.mark.asyncio
async def test_json_phase_strips_code_fence(monkeypatch):
    resp = MagicMock()
    resp.choices[0].message.content = '```json\n{"confidence_score": 80, "reasoning": "solid"}\n```'
    monkeypatch.setattr(evaluate_performance, "_call", AsyncMock(return_value=resp))

    score, reasoning = await _json_phase([])

    assert score == 80
    assert reasoning == "solid"


@pytest.mark.asyncio
async def test_tool_loop_exhausts_rounds_then_asks_no_tools(monkeypatch):
    tool_resp = _tool_call_response("fetch_live_market_floor", '{"market_hash_name": "AK-47 | Redline (Field-Tested)"}')
    stop_resp = _json_response(60, "done")
    monkeypatch.setattr(evaluate_performance, "_MAX_TOOL_ROUNDS", 2)
    monkeypatch.setitem(
        evaluate_performance.AVAILABLE_FUNCTIONS,
        "fetch_live_market_floor",
        lambda **kwargs: json.dumps({"live_floor_cents": 900}),
    )
    mock_call = AsyncMock(side_effect=[tool_resp, tool_resp, stop_resp])
    monkeypatch.setattr(evaluate_performance, "_call", mock_call)

    await _tool_loop([])

    assert mock_call.await_count == 3
    assert mock_call.await_args.kwargs["tool_choice"] == "none"


# ---------------------------------------------------------------------------
# fetch_daily_trades
# ---------------------------------------------------------------------------


class _FakeSession:
    def __init__(self, rows):
        self.rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, stmt):
        result = MagicMock()
        result.all.return_value = self.rows
        return result


@pytest.mark.asyncio
async def test_fetch_daily_trades(monkeypatch):
    rows = [("trade_row", "AK-47 | Redline (Field-Tested)", 0.5)]
    monkeypatch.setattr(evaluate_performance, "AsyncSession", lambda engine: _FakeSession(rows))

    result = await evaluate_performance.fetch_daily_trades()

    assert result == rows


# ---------------------------------------------------------------------------
# evaluate_trade retry / TPD / MLflow failure paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("evaluate_performance.MlflowClient")
@patch("evaluate_performance.openai_client")
@patch("evaluate_performance.get_experiment_id", return_value="1")
async def test_evaluate_trade_retries_then_succeeds(
    mock_get_experiment_id, mock_openai_client, mock_mlflow_client_cls, monkeypatch
):
    evaluate_performance._model_index_var.set(0)
    mock_client = MagicMock()
    mock_run = MagicMock()
    mock_run.info.run_id = "run_retry"
    mock_client.create_run.return_value = mock_run
    mock_mlflow_client_cls.return_value = mock_client

    mock_sleep = AsyncMock()
    monkeypatch.setattr(evaluate_performance, "_sleep", mock_sleep)

    mock_openai_client.chat.completions.create.side_effect = [
        Exception("transient failure"),
        Exception("transient failure 2"),
        _json_response(90, "good deal"),
        _json_response(90, "good deal"),
    ]

    await evaluate_trade(_trade(), "AK-47 | Redline (Field-Tested)", None)

    assert mock_openai_client.chat.completions.create.call_count == 4
    assert mock_sleep.await_count == 2
    mock_client.log_metric.assert_called_with("run_retry", "cfo_confidence_score", 90)
    mock_client.set_tag.assert_called_with("run_retry", "eval_status", "APPROVED")
    mock_client.set_terminated.assert_called_with("run_retry", status="FINISHED")


@pytest.mark.asyncio
@patch("evaluate_performance.MlflowClient")
@patch("evaluate_performance.openai_client")
@patch("evaluate_performance.get_experiment_id", return_value="1")
async def test_evaluate_trade_fails_after_three_attempts(
    mock_get_experiment_id, mock_openai_client, mock_mlflow_client_cls, monkeypatch
):
    evaluate_performance._model_index_var.set(0)
    mock_client = MagicMock()
    mock_run = MagicMock()
    mock_run.info.run_id = "run_fail"
    mock_client.create_run.return_value = mock_run
    mock_mlflow_client_cls.return_value = mock_client

    mock_sleep = AsyncMock()
    monkeypatch.setattr(evaluate_performance, "_sleep", mock_sleep)

    mock_openai_client.chat.completions.create.side_effect = Exception("always broken")

    await evaluate_trade(_trade(), "AK-47 | Redline (Field-Tested)", None)

    assert mock_openai_client.chat.completions.create.call_count == 3
    mock_client.set_tag.assert_called_with("run_fail", "eval_status", "ERROR")
    mock_client.set_terminated.assert_called_with("run_fail", status="FAILED")


@pytest.mark.asyncio
@patch("evaluate_performance.MlflowClient")
@patch("evaluate_performance.openai_client")
@patch("evaluate_performance.get_experiment_id", return_value="1")
async def test_evaluate_trade_switches_model_on_tpd(mock_get_experiment_id, mock_openai_client, mock_mlflow_client_cls):
    evaluate_performance._model_index_var.set(0)
    mock_client = MagicMock()
    mock_run = MagicMock()
    mock_run.info.run_id = "run_tpd"
    mock_client.create_run.return_value = mock_run
    mock_mlflow_client_cls.return_value = mock_client

    mock_openai_client.chat.completions.create.side_effect = [
        Exception("TPD limit reached"),
        _json_response(40, "switched models"),
        _json_response(40, "switched models"),
    ]

    await evaluate_trade(_trade(), "AK-47 | Redline (Field-Tested)", None)

    assert mock_openai_client.chat.completions.create.call_count == 3
    assert mock_openai_client.chat.completions.create.call_args_list[1][1]["model"] == "llama-3.3-70b-versatile"
    mock_client.set_tag.assert_called_with("run_tpd", "eval_status", "REJECTED")


@pytest.mark.asyncio
@patch("evaluate_performance.MlflowClient")
@patch("evaluate_performance.openai_client")
@patch("evaluate_performance.get_experiment_id", return_value="1")
async def test_evaluate_trade_tpd_exhausted_all_models(mock_get_experiment_id, mock_openai_client, mock_mlflow_client_cls):
    evaluate_performance._model_index_var.set(0)
    mock_client = MagicMock()
    mock_run = MagicMock()
    mock_run.info.run_id = "run_tpd_all"
    mock_client.create_run.return_value = mock_run
    mock_mlflow_client_cls.return_value = mock_client

    mock_openai_client.chat.completions.create.side_effect = Exception("TPD limit reached")

    await evaluate_trade(_trade(), "AK-47 | Redline (Field-Tested)", None)

    assert mock_openai_client.chat.completions.create.call_count == 4
    mock_client.set_tag.assert_called_with("run_tpd_all", "eval_status", "ERROR")
    mock_client.set_terminated.assert_called_with("run_tpd_all", status="FAILED")


@pytest.mark.asyncio
@patch("evaluate_performance.MlflowClient")
@patch("evaluate_performance.openai_client")
@patch("evaluate_performance.get_experiment_id", return_value="1")
async def test_evaluate_trade_mlflow_failure_logs_and_marks_failed(
    mock_get_experiment_id, mock_openai_client, mock_mlflow_client_cls, caplog
):
    evaluate_performance._model_index_var.set(0)
    mock_client = MagicMock()
    mock_run = MagicMock()
    mock_run.info.run_id = "run_mlfail"
    mock_client.create_run.return_value = mock_run
    mock_client.log_param.side_effect = MlflowException("mlflow storage full")
    mock_mlflow_client_cls.return_value = mock_client

    mock_openai_client.chat.completions.create.side_effect = [
        _json_response(70, "ok"),
        _json_response(70, "ok"),
    ]

    await evaluate_trade(_trade(), "AK-47 | Redline (Field-Tested)", None)

    assert "MLflow logging failed" in caplog.text
    mock_client.set_terminated.assert_called_with("run_mlfail", status="FAILED")


@pytest.mark.asyncio
@patch("evaluate_performance.MlflowClient")
@patch("evaluate_performance.openai_client")
@patch("evaluate_performance.get_experiment_id", return_value="1")
async def test_evaluate_trade_mlflow_failure_swallows_terminate_error(
    mock_get_experiment_id, mock_openai_client, mock_mlflow_client_cls, caplog
):
    evaluate_performance._model_index_var.set(0)
    mock_client = MagicMock()
    mock_run = MagicMock()
    mock_run.info.run_id = "run_mlfail2"
    mock_client.create_run.return_value = mock_run
    mock_client.log_param.side_effect = MlflowException("mlflow storage full")
    mock_client.set_terminated.side_effect = MlflowException("mlflow gone")
    mock_mlflow_client_cls.return_value = mock_client

    mock_openai_client.chat.completions.create.side_effect = [
        _json_response(70, "ok"),
        _json_response(70, "ok"),
    ]

    await evaluate_trade(_trade(), "AK-47 | Redline (Field-Tested)", None)

    assert "MLflow logging failed" in caplog.text
    assert mock_client.set_terminated.call_count == 1


# ---------------------------------------------------------------------------
# pipeline
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_cfo_evaluation_pipeline(monkeypatch):
    evaluate_performance._model_index_var.set(0)
    trade = _trade()
    mock_fetch = AsyncMock(return_value=[(trade, "AK-47 | Redline (Field-Tested)", None)])
    mock_evaluate = AsyncMock()
    monkeypatch.setattr(evaluate_performance, "fetch_daily_trades", mock_fetch)
    monkeypatch.setattr(evaluate_performance, "evaluate_trade", mock_evaluate)

    await evaluate_performance.run_cfo_evaluation_pipeline()

    mock_fetch.assert_awaited_once()
    mock_evaluate.assert_awaited_once_with(trade, "AK-47 | Redline (Field-Tested)", None)


def test_extract_retry_after_minutes_format():
    assert _extract_retry_after("Please try again in 2m13.5744s") == pytest.approx(2 * 60 + 13.5744 + 1, abs=0.01)
