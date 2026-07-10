import asyncio
import contextvars
import json
import os
import re
import sys
import tempfile
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")
load_dotenv(dotenv_path=PROJECT_ROOT / "apps" / "analytics" / ".env", override=True)

import mlflow
from mlflow.client import MlflowClient
from openai import OpenAI
from prefect import flow, task
from pydantic import BaseModel, Field
from shared_utils import get_logger
from shared_utils.db_connection import async_engine
from shared_utils.models import LiveMarketTick, MarketItem, SimulatedTrade
from sqlalchemy import desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
from tools import AVAILABLE_FUNCTIONS, TOOL_SCHEMAS

logger = get_logger("analytics.evaluate")

_MODELS = [
    "qwen/qwen3-32b",
    "llama-3.3-70b-versatile",
    "llama-3.1-8b-instant",
    "openai/gpt-oss-20b",
]
_model_index_var: contextvars.ContextVar[int] = contextvars.ContextVar("_model_index", default=0)
_MAX_TOOL_ROUNDS = 5

_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(_tracking_uri)

_experiment_id = None


class CFOEvaluation(BaseModel):
    confidence_score: int = Field(ge=0, le=100, description="Confidence score 0-100")
    reasoning: str = Field(description="Reasoning for the evaluation")


def get_experiment_id():
    global _experiment_id
    if _experiment_id is not None:
        return _experiment_id

    client_mlflow = MlflowClient(tracking_uri=_tracking_uri)
    try:
        exp = client_mlflow.get_experiment_by_name("cfo-evaluation")
        _experiment_id = exp.experiment_id if exp else client_mlflow.create_experiment("cfo-evaluation")
    except Exception:
        _experiment_id = "1"
    return _experiment_id


openai_client = OpenAI(
    api_key=os.getenv("GROQ_API_KEY"),
    base_url="https://api.groq.com/openai/v1",
)

_SYSTEM_INSTRUCTION = """
You are the Adversarial CFO of an algorithmic trading firm.
Your automated trading bot just executed a paper trade.
The bot claims this is a highly profitable "snipe" based on its historical baselines.
Your job is to PROVE THE BOT WRONG.
Use the available tools to fetch the LIVE market floor and search for recent
macro trends (like market crashes, new cases, etc).
If the live floor is lower than the bot's baseline, or if there is a falling knife
macro trend, the bot made a bad trade.
"""


def _extract_retry_after(error_str: str) -> float:
    """Extract retry-after seconds from a Groq rate-limit error message.
    Handles formats like '2m13.5744s' and '18.99s'."""
    match = re.search(r"(?:Please try again in|Retry after|retry after) (?:(\d+)m)?([0-9.]+)s?", error_str, re.IGNORECASE)
    if match:
        minutes = float(match.group(1) or 0)
        seconds = float(match.group(2))
        return minutes * 60 + seconds + 1
    return 3.0


def _is_tpd_error(error_str: str) -> bool:
    return "tpd" in error_str.lower() or "tokens per day" in error_str.lower()


def _get_current_model() -> str:
    return _MODELS[_model_index_var.get()]


def _switch_to_next_model() -> str | None:
    idx = _model_index_var.get()
    if idx < len(_MODELS) - 1:
        _model_index_var.set(idx + 1)
        new_idx = _model_index_var.get()
        logger.warning(
            "TPD limit hit — switching to model %s (#%d/%d)",
            _MODELS[new_idx],
            new_idx + 1,
            len(_MODELS),
        )
        return _MODELS[_model_index_var.get()]
    logger.error("All %d models exhausted on TPD limit", len(_MODELS))
    return None


def _msg_dict(msg) -> dict:
    entry = {"role": msg.role}
    if msg.content is not None:
        entry["content"] = msg.content
    if msg.tool_calls:
        entry["tool_calls"] = [tc.model_dump() for tc in msg.tool_calls]
    return entry


async def _call(messages, temperature=0.5, **kwargs):
    return await asyncio.to_thread(
        openai_client.chat.completions.create,
        model=_get_current_model(),
        messages=messages,
        temperature=temperature,
        max_tokens=2048,
        **kwargs,
    )


async def _tool_loop(messages):
    for _ in range(_MAX_TOOL_ROUNDS):
        kwargs = {"tools": TOOL_SCHEMAS, "tool_choice": "auto"}
        response = await _call(messages, **kwargs)

        msg = response.choices[0].message
        messages.append(_msg_dict(msg))

        if response.choices[0].finish_reason != "tool_calls":
            return response

        for tc in msg.tool_calls:
            fn_args = json.loads(tc.function.arguments)
            result = AVAILABLE_FUNCTIONS[tc.function.name](**fn_args)
            messages.append({"role": "tool", "tool_call_id": tc.id, "name": tc.function.name, "content": result})

    kwargs = {"tools": TOOL_SCHEMAS, "tool_choice": "none"}
    return await _call(messages, **kwargs)


async def _json_phase(messages):
    messages.append(
        {
            "role": "user",
            "content": (
                "Now produce your final CFO evaluation as structured JSON with keys: "
                "confidence_score (integer 0-100) and reasoning (string). "
                "Do not include any other text outside the JSON."
            ),
        }
    )

    response = await _call(messages, temperature=0.3, tools=TOOL_SCHEMAS, tool_choice="none")
    raw = (response.choices[0].message.content or "").strip()
    if not raw:
        raise ValueError("Groq returned empty content in JSON phase")
    if raw.startswith("```") and "\n" in raw:
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    data = json.loads(raw)
    result = CFOEvaluation(**data)
    return result.confidence_score, result.reasoning


@task
async def fetch_daily_trades():
    async with AsyncSession(async_engine) as session:
        latest_tick_subq = (
            select(LiveMarketTick.float_value)
            .where(LiveMarketTick.item_id == MarketItem.id)
            .order_by(desc(LiveMarketTick.inserted_at))
            .limit(1)
            .correlate(MarketItem)
            .scalar_subquery()
        )
        stmt = select(SimulatedTrade, MarketItem.market_hash_name, latest_tick_subq).join(
            MarketItem, SimulatedTrade.item_id == MarketItem.id
        )
        result = await session.execute(stmt)
        return result.all()


@task
async def evaluate_trade(trade: SimulatedTrade, item_name: str, float_value: float | None):
    logger.info("Auditing trade for %s...", item_name)

    float_line = f"Float Value: {float_value:.4f}" if float_value is not None else "Float Value: Not available"
    prompt = f"""
    The bot bought: {item_name}
    Purchase Price: {trade.purchase_price_cents} cents
    Bot's Estimated Profit: {trade.estimated_profit_cents} cents
    Trigger Z-Score: {trade.trigger_z_score}
    {float_line}

    Audit this trade immediately using your tools. Call fetch_live_market_floor once,
    then call verify_float_value if a float value is available, and search_macro_trends at most twice.
    """

    score = 0
    reasoning = "Evaluation failed: no response from CFO"
    eval_status = "ERROR"

    attempt = 0
    while attempt < 3:
        try:
            messages = [
                {"role": "system", "content": _SYSTEM_INSTRUCTION},
                {"role": "user", "content": prompt},
            ]

            await _tool_loop(messages)
            score, reasoning = await _json_phase(messages)
            eval_status = "APPROVED" if score >= 70 else "REJECTED"
            break

        except Exception as e:
            error_str = str(e)
            # TPD (tokens-per-day) is per-model — switch to the next model
            # without counting this as a failed attempt.
            if _is_tpd_error(error_str):
                if _switch_to_next_model() is not None:
                    continue
                # All models exhausted on TPD — treat as final failure.
                logger.error("All models TPD-exhausted for %s: %s", item_name, e)
                score = 0
                reasoning = f"All models TPD-exhausted: {e}"
                eval_status = "ERROR"
                break

            attempt += 1
            if attempt < 3:
                delay = _extract_retry_after(error_str)
                logger.warning(
                    "Attempt %d/3 for %s with %s failed (retry in %.1fs): %s",
                    attempt,
                    item_name,
                    _get_current_model(),
                    delay,
                    e,
                )
                await asyncio.sleep(delay)
            else:
                logger.error("Groq CFO failed for %s after 3 attempts: %s", item_name, e)
                score = 0
                reasoning = f"Evaluation failed after 3 attempts: {e}"
                eval_status = "ERROR"

    safe_reasoning = reasoning.encode("ascii", "ignore").decode("ascii")
    logger.info("CFO eval status=%s score=%d for %s", eval_status, score, item_name)

    mlflow_client = MlflowClient(tracking_uri=_tracking_uri)
    run = mlflow_client.create_run(experiment_id=get_experiment_id(), run_name=f"audit_{item_name}")
    run_id = run.info.run_id
    try:
        mlflow_client.log_param(run_id, "market_hash_name", item_name)
        mlflow_client.log_param(run_id, "purchase_price_cents", trade.purchase_price_cents)
        mlflow_client.log_param(run_id, "bot_estimated_profit", trade.estimated_profit_cents)
        mlflow_client.log_metric(run_id, "cfo_confidence_score", score)
        mlflow_client.set_tag(run_id, "eval_status", eval_status)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = os.path.join(temp_dir, "cfo_reasoning.txt")
            with open(temp_path, "w", encoding="utf-8") as f:
                f.write(safe_reasoning)
            mlflow_client.log_artifact(run_id, temp_path)

        final_status = "FAILED" if eval_status == "ERROR" else "FINISHED"
        mlflow_client.set_terminated(run_id, status=final_status)
    except Exception as e:
        logger.error("MLflow logging failed for %s: %s", item_name, e)
        try:
            mlflow_client.set_terminated(run_id, status="FAILED")
        except Exception:
            pass


@flow(name="Daily CFO Evaluation")
async def run_cfo_evaluation_pipeline():
    _model_index_var.set(0)
    trades = await fetch_daily_trades()
    logger.info("Found %d trades to evaluate.", len(trades))

    for trade, item_name, float_value in trades:
        await evaluate_trade(trade, item_name, float_value)


if __name__ == "__main__":
    asyncio.run(run_cfo_evaluation_pipeline())
