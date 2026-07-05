import json
import os
import sys
from pathlib import Path

# Force standard streams to use UTF-8 to support Unicode characters on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Add project root and shared_utils to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / "packages" / "shared_utils" / "src"))

from dotenv import load_dotenv

load_dotenv()

import mlflow
from google import genai
from google.genai import types
from mlflow.client import MlflowClient
from prefect import flow, task
from shared_utils.models import MarketItem, SimulatedTrade
from sqlmodel import Session, create_engine, select
from tools import fetch_live_market_floor, search_macro_trends

# Setup Database
engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/brand_sniper")

# Setup MLflow
_experiment_id = None


def get_experiment_id():
    global _experiment_id
    if _experiment_id is not None:
        return _experiment_id

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(tracking_uri)
    client_mlflow = MlflowClient(tracking_uri=tracking_uri)
    try:
        exp = client_mlflow.get_experiment_by_name("cfo-evaluation")
        _experiment_id = exp.experiment_id if exp else client_mlflow.create_experiment("cfo-evaluation")
    except Exception:
        _experiment_id = "1"
    return _experiment_id


# Initialize Gemini Client
# Assumes GEMINI_API_KEY is set in the environment
gemini_client = genai.Client()

SYSTEM_INSTRUCTION = """
You are the Adversarial CFO of an algorithmic trading firm.
Your automated trading bot just executed a paper trade.
The bot claims this is a highly profitable "snipe" based on its historical baselines.
Your job is to PROVE THE BOT WRONG.
You MUST use your FastMCP tools to fetch the LIVE market floor and search for recent
macro trends (like market crashes, new cases, etc).
If the live floor is lower than the bot's baseline, or if there is a falling knife
macro trend, the bot made a bad trade.
Return your evaluation in strict JSON format:
{
    "confidence_score": 0-100 (100 = perfect snipe, 0 = terrible mistake),
    "reasoning": "Detailed explanation of why this trade is good or bad based on LIVE tool data."
}
"""


@task
def fetch_daily_trades():
    with Session(engine) as session:
        # Fetching all trades for demonstration (in production, filter by today's date)
        # We limit to 1 here to strictly respect Gemini Free Tier Quotas (5 requests / min)
        stmt = (
            select(SimulatedTrade, MarketItem.market_hash_name)
            .join(MarketItem, SimulatedTrade.item_id == MarketItem.id)
            .limit(1)
        )
        results = session.execute(stmt).all()
        return results


@task
def evaluate_trade(trade: SimulatedTrade, item_name: str):
    print(f"[CFO] Auditing trade for {item_name}...")

    prompt = f"""
    The bot bought: {item_name}
    Purchase Price: {trade.purchase_price_cents} cents
    Bot's Estimated Profit: {trade.estimated_profit_cents} cents
    Trigger Z-Score: {trade.trigger_z_score}

    Audit this trade immediately using your tools.
    """

    try:
        # We pass the FastMCP tools directly to Gemini to avoid ad-hoc JSON execution layers
        # Note: We cannot use response_mime_type="application/json" with tools=[] in the Gemini API.
        chat = gemini_client.chats.create(
            model="gemini-2.5-flash",
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_INSTRUCTION, tools=[fetch_live_market_floor, search_macro_trends]
            ),
        )

        response = chat.send_message(prompt)

        # Clean markdown code blocks from response
        raw_text = response.text.strip()
        if raw_text.startswith("```json"):
            raw_text = raw_text[7:]
        if raw_text.startswith("```"):
            raw_text = raw_text[3:]
        if raw_text.endswith("```"):
            raw_text = raw_text[:-3]

        result_json = json.loads(raw_text.strip())
        score = result_json.get("confidence_score", 0)
        reasoning = result_json.get("reasoning", "No reasoning provided.")

        safe_reasoning = reasoning.encode("ascii", "ignore").decode("ascii")
        print(f"   -> CFO Reasoning: {safe_reasoning}")

        # Log the evaluation to MLflow securely
        with mlflow.start_run(experiment_id=get_experiment_id(), run_name=f"audit_{item_name}"):
            mlflow.log_param("market_hash_name", item_name)
            mlflow.log_param("purchase_price_cents", trade.purchase_price_cents)
            mlflow.log_param("bot_estimated_profit", trade.estimated_profit_cents)
            mlflow.log_metric("cfo_confidence_score", score)
            mlflow.set_tag("eval_status", "APPROVED" if score >= 70 else "REJECTED")

            # Log the CFO's full reasoning as an artifact without polluting the repo
            import tempfile

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = os.path.join(temp_dir, "cfo_reasoning.txt")
                with open(temp_path, "w", encoding="utf-8") as f:
                    f.write(safe_reasoning)
                mlflow.log_artifact(temp_path)

    except Exception as e:
        print(f"[ERROR] Gemini CFO failed to evaluate trade: {e}")


@flow(name="Daily CFO Evaluation")
def run_cfo_evaluation_pipeline():
    trades = fetch_daily_trades()
    print(f"[PREFECT] Found {len(trades)} trades to evaluate.")

    for trade, item_name in trades:
        evaluate_trade(trade, item_name)


if __name__ == "__main__":
    run_cfo_evaluation_pipeline()
