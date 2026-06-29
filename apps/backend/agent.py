import os
import sys
import asyncio
import time
import tempfile
import mlflow
from pathlib import Path
from google import genai
from google.genai import types
from langfuse import observe, propagate_attributes
from mlflow.client import MlflowClient

# Add project root and apps/backend to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / "apps" / "backend"))

from tools import get_market_context, verify_float_value, confirm_alert_approval
from schemas import AnomalyAlertPayload
from telemetry import run_telemetry
from queries import get_sticker_price_cents

# Configure local MLflow server target and experiment client
tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(tracking_uri)
client_mlflow = MlflowClient(tracking_uri=tracking_uri)

# Retrieve or create experiment ID
try:
    exp = client_mlflow.get_experiment_by_name("sniper-verifier")
    if exp:
        experiment_id = exp.experiment_id
    else:
        experiment_id = client_mlflow.create_experiment("sniper-verifier")
except Exception:
    experiment_id = "1"

@observe()
async def run_verification_loop(payload: AnomalyAlertPayload, float_val: float = None):
    print(f"\n[AGENT] Starting AI Verification Loop for: {payload.market_hash_name}")
    
    # Initialize Google GenAI client
    client = genai.Client()
    tools_list = [get_market_context, verify_float_value, confirm_alert_approval]
    
    # Resolve wear from payload if available, falling back to float_val argument
    resolved_wear = payload.float_value if payload.float_value is not None else float_val
    
    # Resolve and format sticker context if available
    stickers_info = ""
    total_sticker_value_cents = 0
    if hasattr(payload, "stickers") and payload.stickers:
        stickers_context = []
        for s in payload.stickers:
            name = s.get("name")
            wear = s.get("wear")
            if name:
                price = await get_sticker_price_cents(name)
                if price is not None:
                    stickers_context.append(f"- {name} (Wear: {wear if wear is not None else 0.0:.4f}, Standalone Market Price: ${price/100:.2f})")
                    total_sticker_value_cents += price
                else:
                    stickers_context.append(f"- {name} (Wear: {wear if wear is not None else 0.0:.4f}, Standalone Market Price: Unknown)")
        if stickers_context:
            stickers_info = "\nApplied Stickers Details:\n" + "\n".join(stickers_context) + f"\nTotal Standalone Sticker Value: ${total_sticker_value_cents/100:.2f}\n"

    # Build the GenAI Analyst prompt
    prompt = f"""
    You are a Lead Sniping Analyst for a digital asset arbitrage system.
    We have intercepted a potential pricing anomaly:
    - Asset Name: {payload.market_hash_name}
    - Alert Price: ${payload.price_usd:.2f} ({payload.price_cents} cents)
    - Edge Z-Score: {payload.z_score}{stickers_info}
    
    Verify this opportunity using the available tools:
    1. Call 'get_market_context' to retrieve database baselines, target snipe thresholds, and the marketplace pages.
    2. Check if the alert price is less than or equal to the calculated 'snipe_threshold_cents'.
    3. Evaluate the float wear if available (Alert float value: {resolved_wear if resolved_wear is not None else 'None'}) using 'verify_float_value'.
    4. Evaluate sticker sniping premium if stickers are present:
       - In CS2, applied stickers do not transfer 100% value. However, highly valuable stickers (total value > $100) add a 'sticker premium' (typically 2% to 10% of their standalone price) to the item.
       - Calculate the Implied Sticker Percentage (SP%):
         SP% = ((Alert Price Cents - Base Skin Value Cents) / Total Sticker Value Cents) * 100
       - If the item is listed at or below its normal base skin value (implied SP% is 0% or negative), it is an automatic verification approval.
       - If the item's price is slightly above the base skin threshold, but SP% is extremely low (e.g. less than 3%), this is still a high-priority snipe and should be approved.
    5. If and ONLY IF the alert price is verified to be a genuine deep discount or represents an insane sticker sniping bargain, call 'confirm_alert_approval' to approve the alert and register the direct purchase link ('item_page').
    6. Formulate a final summary response detailing your reasoning, baseline values, target threshold, wear premium checks, applied sticker calculations (including SP% if stickers are present), the verification status (APPROVED or REJECTED), and include the direct purchase link ('item_page') prominently so the user can click it to buy it manually.
    """
    
    # Initialize the local ContextVar tracking state for this run
    token = run_telemetry.set({
        "alert_approved": False,
        "float_value": resolved_wear,
        "market_hash_name": payload.market_hash_name,
        "alert_price_cents": payload.price_cents,
        "alert_price_usd": payload.price_usd,
        "edge_z_score": payload.z_score,
        "item_page": None,
    })
    
    start_time = time.time()
    
    # Create a unique stateless run
    run = client_mlflow.create_run(
        experiment_id=experiment_id,
        run_name=f"verify_{payload.market_hash_name}"
    )
    run_id = run.info.run_id
    
    try:
        # Log initial alert parameters
        client_mlflow.log_param(run_id, "market_hash_name", payload.market_hash_name)
        client_mlflow.log_param(run_id, "alert_price_cents", str(payload.price_cents))
        client_mlflow.log_param(run_id, "alert_price_usd", str(payload.price_usd))
        client_mlflow.log_param(run_id, "edge_z_score", str(payload.z_score))
        client_mlflow.log_param(run_id, "alert_float_value", str(resolved_wear) if resolved_wear is not None else "None")
        if total_sticker_value_cents > 0:
            client_mlflow.log_param(run_id, "total_sticker_value_cents", str(total_sticker_value_cents))
        
        # Wrap the tool calling block inside the propagate_attributes context manager to attach metadata
        with propagate_attributes(
            metadata={
                "market_hash_name": payload.market_hash_name,
                "price_cents": payload.price_cents,
                "z_score": payload.z_score,
                "triggered_at": payload.triggered_at
            },
            tags=["anomaly-check"]
        ):
            print("   [AGENT] Executing Gemini 3.1 generative reasoning and tool calling...")
            response = await client.aio.models.generate_content(
                model='gemini-3.1-flash-lite',
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=tools_list,
                    temperature=0.0
                )
            )
        
        latency = time.time() - start_time
        print(f"   [AGENT] Inference completed in {latency:.3f}s.")
        
        print("\n=== VERIFICATION DECISION SUMMARY ===")
        print(response.text)
        print("======================================\n")
        
        # Extract accumulated telemetry data from ContextVar
        t_data = run_telemetry.get()
        
        # Log resolved parameters
        resolved_params = {
            "historical_steam_avg_cents": t_data.get("historical_steam_avg_cents"),
            "historical_skinport_avg_cents": t_data.get("historical_skinport_avg_cents"),
            "cash_equivalent_avg_cents": t_data.get("cash_equivalent_avg_cents"),
            "snipe_threshold_cents": t_data.get("snipe_threshold_cents"),
            "is_liquid": t_data.get("is_liquid"),
            "regime_shift_detected": t_data.get("regime_shift_detected"),
            "downtrend_detected": t_data.get("downtrend_detected"),
        }
        for k, v in resolved_params.items():
            client_mlflow.log_param(run_id, k, str(v) if v is not None else "None")
            
        # Log metrics
        client_mlflow.log_metric(run_id, "gemini_latency_seconds", latency)
        
        if t_data.get("downtrend_severity") is not None:
            client_mlflow.log_metric(run_id, "downtrend_severity", float(t_data.get("downtrend_severity")))
            
        alert_approved = 1 if t_data.get("alert_approved") else 0
        client_mlflow.log_metric(run_id, "alert_approved", alert_approved)
        
        if t_data.get("item_page"):
            client_mlflow.log_param(run_id, "item_page", t_data.get("item_page"))
            
        # Log tracking tags
        client_mlflow.set_tag(run_id, "status", "APPROVED" if alert_approved else "REJECTED")
        client_mlflow.set_tag(run_id, "market_hash_name", payload.market_hash_name)
        
        # Save prompt and final reasoning as text artifacts
        with tempfile.TemporaryDirectory() as tmpdir:
            prompt_path = Path(tmpdir) / "prompt.txt"
            prompt_path.write_text(prompt, encoding="utf-8")
            client_mlflow.log_artifact(run_id, str(prompt_path))
            
            reasoning_path = Path(tmpdir) / "reasoning.txt"
            reasoning_path.write_text(response.text, encoding="utf-8")
            client_mlflow.log_artifact(run_id, str(reasoning_path))
            
        client_mlflow.set_terminated(run_id, status="FINISHED")
        
        print(f" View run verify_{payload.market_hash_name} at: http://localhost:5000/#/experiments/{experiment_id}/runs/{run_id}")
        print(f" View experiment at: http://localhost:5000/#/experiments/{experiment_id}")
        
        return response.text
            
    except Exception as e:
        print(f"[AGENT] Failed during verification loop execution: {e}")
        try:
            client_mlflow.set_tag(run_id, "status", "FAILED")
            with tempfile.TemporaryDirectory() as tmpdir:
                err_path = Path(tmpdir) / "error.txt"
                err_path.write_text(str(e), encoding="utf-8")
                client_mlflow.log_artifact(run_id, str(err_path))
            client_mlflow.set_terminated(run_id, status="FAILED")
        except Exception:
            pass
        raise e
    finally:
        # Guarantee ContextVar cleanup
        run_telemetry.reset(token)


