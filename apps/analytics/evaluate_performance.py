import asyncio
import contextvars
import json
import math
import os
import re
import tempfile
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

from shared_utils import setup_script_environment

setup_script_environment(__file__)

from llm_config import LLMSettings, load_llm_settings
from mlflow.client import MlflowClient
from mlflow.exceptions import MlflowException
from openai import OpenAI
from prefect import flow, task
from pydantic import BaseModel, Field
from shared_utils import get_backend_api_key, get_logger, validate_required_env
from shared_utils.db_connection import async_engine
from shared_utils.models import MarketItem, SimulatedTrade
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
from tools import AVAILABLE_FUNCTIONS, TOOL_SCHEMAS, close_http_session

logger = get_logger("analytics.evaluate")


async def _sleep(seconds: float) -> None:
    """Testable seam over asyncio.sleep for retry backoff waits."""
    await asyncio.sleep(seconds)


_model_index_var: contextvars.ContextVar[int] = contextvars.ContextVar("_model_index", default=0)
_MAX_TOOL_ROUNDS = 5

llm_settings: LLMSettings | None = None


def _get_llm_settings() -> LLMSettings:
    """Returns the cached provider-neutral LLM settings, loading them on first use."""
    global llm_settings
    if llm_settings is None:
        llm_settings = load_llm_settings()
    return llm_settings


def _reset_llm_state() -> None:
    """Resets cached LLM settings, client, and model index (test seam)."""
    global llm_settings, openai_client
    llm_settings = None
    openai_client = None
    _model_index_var.set(0)


def _get_all_models() -> list[str]:
    return _get_llm_settings().all_models


_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

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
    except MlflowException:
        _experiment_id = "1"
    return _experiment_id


def _log_cfo_evaluation(
    trade: SimulatedTrade,
    item_name: str,
    safe_reasoning: str,
    score: int,
    eval_status: str,
) -> None:
    """Log one CFO evaluation through the synchronous MLflow client."""
    mlflow_client: MlflowClient | None = None
    run_id: str | None = None

    try:
        mlflow_client = MlflowClient(tracking_uri=_tracking_uri)
        run = mlflow_client.create_run(
            experiment_id=get_experiment_id(),
            run_name=f"audit_{item_name}",
        )
        run_id = run.info.run_id

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
    except (MlflowException, OSError) as e:
        logger.error("[CFO] MLflow logging failed for %s: %s", item_name, e)
        if mlflow_client is not None and run_id is not None:
            try:
                mlflow_client.set_terminated(run_id, status="FAILED")
            except (MlflowException, OSError):
                pass


openai_client: OpenAI | None = None


def _get_openai_client() -> OpenAI:
    global openai_client
    if openai_client is None:
        settings = _get_llm_settings()
        openai_client = OpenAI(
            # The SDK requires a non-empty key even for endpoints that ignore
            # auth; the placeholder is only sent when no-auth mode is configured.
            api_key=settings.api_key or "local-no-auth",
            base_url=settings.base_url,
            timeout=settings.request_timeout_seconds,
        )
    return openai_client


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


def _is_quota_error(error: BaseException) -> bool:
    """Detects day-scale quota exhaustion (per-model or per-key) from any compatible endpoint.

    Minute-scale rate limits are transient and retried on the same model; only
    quota exhaustion rotates to the next configured fallback model.
    """
    message = str(error).lower()
    if any(marker in message for marker in ("tokens per day", "requests per day", "tpd", "quota", "daily limit", "rpd")):
        return True
    status_code = getattr(error, "status_code", None)
    if status_code == 429 and any(marker in message for marker in ("day", "quota")):
        return True
    return False


def _parse_retry_after_value(value: object) -> float | None:
    """Parses a Retry-After value as seconds, supporting numeric and HTTP-date forms."""
    text = str(value).strip()
    stripped = text[:-1].strip() if text.endswith("s") else text
    for candidate in (stripped, text):
        try:
            parsed = float(candidate)
        except ValueError:
            continue
        return parsed if math.isfinite(parsed) else None
    try:
        retry_at = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=UTC)
    return max((retry_at - datetime.now(UTC)).total_seconds(), 0.0)


def _retry_delay_for(error: BaseException) -> float:
    """Prefers the protocol-level Retry-After header, falling back to message parsing."""
    headers = getattr(getattr(error, "response", None), "headers", None)
    if headers:
        try:
            lowered = {str(k).lower(): v for k, v in dict(headers).items()}
        except Exception:
            lowered = {}
        for key in ("retry-after-ms", "retry_after_ms"):
            if key in lowered:
                parsed = _parse_retry_after_value(lowered[key])
                if parsed is None:
                    break
                return max(parsed / 1000, 0.0) + 1
        for key in ("retry-after", "retry_after"):
            if key in lowered:
                parsed = _parse_retry_after_value(lowered[key])
                if parsed is None:
                    break
                return max(parsed, 0.0) + 1
    return _extract_retry_after(str(error))


def _get_current_model() -> str:
    models = _get_all_models()
    return models[min(_model_index_var.get(), len(models) - 1)]


def _switch_to_next_model() -> str | None:
    models = _get_all_models()
    idx = min(_model_index_var.get(), len(models) - 1)
    if idx < len(models) - 1:
        _model_index_var.set(idx + 1)
        new_idx = _model_index_var.get()
        logger.warning(
            "[CFO] LLM quota exhausted — switching to model %s (#%d/%d)",
            models[new_idx],
            new_idx + 1,
            len(models),
        )
        return models[new_idx]
    logger.error("[CFO] All %d configured models exhausted on quota", len(models))
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
        _get_openai_client().chat.completions.create,
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
            result = await AVAILABLE_FUNCTIONS[tc.function.name](**fn_args)
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
        raise ValueError("LLM provider returned empty content in JSON phase")
    if raw.startswith("```") and "\n" in raw:
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    data = json.loads(raw)
    result = CFOEvaluation(**data)
    return result.confidence_score, result.reasoning


@task
async def fetch_daily_trades():
    # The float comes from the trade itself (the listing that was bought). The latest tick for the
    # item would describe some other listing of the same skin and mislead the audit.
    async with AsyncSession(async_engine) as session:
        stmt = select(SimulatedTrade, MarketItem.market_hash_name, SimulatedTrade.float_value).join(
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

        # Broad on purpose: retry loop relies on duck-typed API error payloads
        # (quota detection via status/message/headers) and retries transient
        # provider failures.
        except Exception as e:
            # Day-scale quota exhaustion is per-model — switch to the next
            # configured model without counting this as a failed attempt.
            if _is_quota_error(e):
                if _switch_to_next_model() is not None:
                    continue
                # All models exhausted on quota — treat as final failure.
                logger.error("[CFO] All configured models quota-exhausted for %s: %s", item_name, e)
                score = 0
                reasoning = f"All configured models quota-exhausted: {e}"
                eval_status = "ERROR"
                break

            attempt += 1
            if attempt < 3:
                delay = _retry_delay_for(e)
                logger.warning(
                    "[CFO] Attempt %d/3 for %s with %s failed (retry in %.1fs): %s",
                    attempt,
                    item_name,
                    _get_current_model(),
                    delay,
                    e,
                )
                await _sleep(delay)
            else:
                logger.error("[CFO] LLM evaluation failed for %s after 3 attempts: %s", item_name, e)
                score = 0
                reasoning = f"Evaluation failed after 3 attempts: {e}"
                eval_status = "ERROR"

    safe_reasoning = reasoning.encode("ascii", "ignore").decode("ascii")
    logger.info("CFO eval status=%s score=%d for %s", eval_status, score, item_name)

    await asyncio.to_thread(
        _log_cfo_evaluation,
        trade,
        item_name,
        safe_reasoning,
        score,
        eval_status,
    )


@flow(name="Daily CFO Evaluation")
async def run_cfo_evaluation_pipeline():
    _model_index_var.set(0)
    _get_llm_settings()
    trades = await fetch_daily_trades()
    logger.info("Found %d trades to evaluate.", len(trades))

    try:
        for trade, item_name, float_value in trades:
            await evaluate_trade(trade, item_name, float_value)
    finally:
        await close_http_session()


if __name__ == "__main__":  # pragma: no cover - entrypoint glue, covered via unit tests
    validate_required_env(["BACKEND_API_KEY", "MLFLOW_TRACKING_URI"])
    load_llm_settings()
    get_backend_api_key()
    asyncio.run(run_cfo_evaluation_pipeline())
