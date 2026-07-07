import asyncio
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

import instructor
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

MODEL = "qwen/qwen3-32b"
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


client = instructor.from_openai(
    OpenAI(
        api_key=os.getenv("GROQ_API_KEY"),
        base_url="https://api.groq.com/openai/v1",
    )
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
    match = re.search(r"(?:Please try again in|Retry after|retry after) ([0-9.]+)s?", error_str, re.IGNORECASE)
    return float(match.group(1)) + 1 if match else 3.0


async def _call(messages, temperature=0.5, **kwargs):
    return await asyncio.to_thread(
        client.chat.completions.create,
        model=MODEL,
        messages=messages,
        temperature=temperature,
        max_tokens=2048,
        **kwargs,
    )


async def _tool_loop(messages):
    for _ in range(_MAX_TOOL_ROUNDS):
        kwargs = {"tools": TOOL_SCHEMAS, "tool_choice": "auto"}
        response = await _call(messages, **kwargs)

        if response.choices[0].finish_reason != "tool_calls":
            return response

        msg = response.choices[0].message
        messages.append(msg)

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

    result: CFOEvaluation = await _call(messages, temperature=0.3, response_model=CFOEvaluation)
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

    for attempt in range(3):
        try:
            messages = [
                {"role": "system", "content": _SYSTEM_INSTRUCTION},
                {"role": "user", "content": prompt},
            ]

            await _tool_loop(messages)
            score, reasoning = await _json_phase(messages)

            safe_reasoning = reasoning.encode("ascii", "ignore").decode("ascii")
            logger.info("CFO Reasoning: %s", safe_reasoning)

            with mlflow.start_run(experiment_id=get_experiment_id(), run_name=f"audit_{item_name}"):
                mlflow.log_param("market_hash_name", item_name)
                mlflow.log_param("purchase_price_cents", trade.purchase_price_cents)
                mlflow.log_param("bot_estimated_profit", trade.estimated_profit_cents)
                mlflow.log_metric("cfo_confidence_score", score)
                mlflow.set_tag("eval_status", "APPROVED" if score >= 70 else "REJECTED")

                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_path = os.path.join(temp_dir, "cfo_reasoning.txt")
                    with open(temp_path, "w", encoding="utf-8") as f:
                        f.write(safe_reasoning)
                    mlflow.log_artifact(temp_path)

            return

        except Exception as e:
            if attempt < 2:
                delay = _extract_retry_after(str(e))
                logger.warning("Attempt %d/3 failed for %s (retry in %.1fs): %s", attempt + 1, item_name, delay, e)
                await asyncio.sleep(delay)
            else:
                logger.error("Groq CFO failed to evaluate trade after 3 attempts: %s", e)


@flow(name="Daily CFO Evaluation")
async def run_cfo_evaluation_pipeline():
    trades = await fetch_daily_trades()
    logger.info("Found %d trades to evaluate.", len(trades))

    for trade, item_name, float_value in trades:
        await evaluate_trade(trade, item_name, float_value)


if __name__ == "__main__":
    asyncio.run(run_cfo_evaluation_pipeline())
