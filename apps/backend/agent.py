import os
import sys
import asyncio
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
    
    try:
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
            
            print("\n=== VERIFICATION DECISION SUMMARY ===")
            print(response.text)
            print("======================================\n")
            
            return response.text
            
    except Exception as e:
        print(f"❌ [AGENT] Failed during verification loop execution: {e}")
        raise e

