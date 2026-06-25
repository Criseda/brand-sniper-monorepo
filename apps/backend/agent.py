import os
import sys
import asyncio
import time
import mlflow
from pathlib import Path
from google import genai
from google.genai import types
from langfuse import observe, propagate_attributes

# Add project root and apps/backend to sys.path
PROJECT_ROOT = Path(r"c:\Users\ilaur\git\brand-sniper-monorepo")
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / "apps" / "backend"))

from tools import get_market_context, verify_float_value, simulate_checkout_payload
from schemas import AnomalyAlertPayload
from telemetry import run_telemetry

# Configure local MLflow server target and experiment
mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
mlflow.set_experiment("sniper-verifier")

@observe()
async def run_verification_loop(payload: AnomalyAlertPayload, float_val: float = None):
    print(f"\n🧠 [AGENT] Starting AI Verification Loop for: {payload.market_hash_name}")
    
    # Initialize Google GenAI client
    client = genai.Client()
    tools_list = [get_market_context, verify_float_value, simulate_checkout_payload]
    
    # Build the GenAI Analyst prompt
    prompt = f"""
    You are a Lead Sniping Analyst for a digital asset arbitrage system.
    We have intercepted a potential pricing anomaly:
    - Asset Name: {payload.market_hash_name}
    - Alert Price: ${payload.price_usd:.2f} ({payload.price_cents} cents)
    - Edge Z-Score: {payload.z_score}
    
    Verify this opportunity using the available tools:
    1. Call 'get_market_context' to retrieve database baselines and target snipe thresholds.
    2. Check if the alert price is less than or equal to the calculated 'snipe_threshold_cents'.
    3. Evaluate the float wear if available (Alert float value: {float_val if float_val is not None else 'None'}) using 'verify_float_value'.
    4. If and ONLY IF the alert price is verified to be a genuine deep discount (less than or equal to the threshold), trigger 'simulate_checkout_payload' to secure the asset.
    5. Formulate a final summary response detailing your reasoning, baseline values, target threshold, wear premium checks, and the purchase status.
    """
    
    # Initialize the local ContextVar tracking state for this run
    token = run_telemetry.set({
        "checkout_triggered": False,
        "float_value": float_val,
        "market_hash_name": payload.market_hash_name,
        "alert_price_cents": payload.price_cents,
        "alert_price_usd": payload.price_usd,
        "edge_z_score": payload.z_score,
        "transaction_id": None,
        "checkout_price_cents": None,
    })
    
    start_time = time.time()
    
    try:
        # Open the MLflow run scope
        with mlflow.start_run(run_name=f"verify_{payload.market_hash_name}"):
            # Log initial alert parameters
            mlflow.log_params({
                "market_hash_name": payload.market_hash_name,
                "alert_price_cents": payload.price_cents,
                "alert_price_usd": payload.price_usd,
                "edge_z_score": payload.z_score,
                "alert_float_value": float_val if float_val is not None else "None"
            })
            
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
            
            # Log resolved parameters and metrics
            mlflow.log_params({
                "historical_steam_avg_cents": t_data.get("historical_steam_avg_cents"),
                "historical_skinport_avg_cents": t_data.get("historical_skinport_avg_cents"),
                "cash_equivalent_avg_cents": t_data.get("cash_equivalent_avg_cents"),
                "snipe_threshold_cents": t_data.get("snipe_threshold_cents"),
                "is_liquid": t_data.get("is_liquid"),
                "regime_shift_detected": t_data.get("regime_shift_detected"),
                "downtrend_detected": t_data.get("downtrend_detected"),
            })
            
            # Log metrics
            mlflow.log_metric("gemini_latency_seconds", latency)
            
            if t_data.get("downtrend_severity") is not None:
                mlflow.log_metric("downtrend_severity", t_data.get("downtrend_severity"))
                
            checkout_success = 1 if t_data.get("checkout_triggered") else 0
            mlflow.log_metric("checkout_executed", checkout_success)
            
            if checkout_success:
                mlflow.log_metric("purchase_price_cents", t_data.get("checkout_price_cents"))
                mlflow.set_tag("transaction_id", t_data.get("transaction_id"))
                
            # Log tracking tags
            mlflow.set_tag("status", "APPROVED" if checkout_success else "REJECTED")
            mlflow.set_tag("market_hash_name", payload.market_hash_name)
            
            # Save prompt and final reasoning as text artifacts
            mlflow.log_text(prompt, "prompt.txt")
            mlflow.log_text(response.text, "reasoning.txt")
            
            return response.text
            
    except Exception as e:
        print(f"❌ [AGENT] Failed during verification loop execution: {e}")
        try:
            mlflow.set_tag("status", "FAILED")
            mlflow.log_text(str(e), "error.txt")
        except Exception:
            pass
        raise e
    finally:
        # Guarantee ContextVar cleanup
        run_telemetry.reset(token)


