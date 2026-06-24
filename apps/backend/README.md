# Backend Compute Node & Agent Verification Engine

This service hosts the core API server, verifies pricing anomalies via an LLM agent reasoning loop, and serves the MCP (Model Context Protocol) tool integration.

## Core Features

### 1. Market Context Valuation Query
The valuation query layer (`get_item_market_context` in `queries.py`) calculates fair-value cash-equivalent baselines and snipe thresholds dynamically:
* Converts Steam list prices to cash equivalents by applying type-specific cash discounts:
  * **Knives & Gloves:** 25% discount
  * **Stickers & Patches:** 35% discount
  * **Default/Skins:** 30% discount

### 2. Market Safety Guardrails
To prevent locking capital in bad or illiquid trades, the backend implements two dynamic checks using macro metrics:

#### 1. Liquidity Guardrail
* Evaluates the rolling 30-day average transaction volume (`avg_volume_30d` from the macro pipeline).
* If an item averages **less than 0.5 sales per day** (less than 1 transaction every 2 days), it is flagged as `is_liquid = False` and the backend blocks the alert from executing checks or purchasing.

#### 2. Concept Drift / 2025 Regime Shift Mitigation
* Structural updates (like the 2025 CS2 update enabling trade-ups for knives) can crash some items' prices and spike others, rendering pre-2025 historical data outdated.
* The backend compares the long-term historical Steam cash-equivalent baseline against recent live Skinport listings.
* If they deviate by **more than 35%**, the system flags `regime_shift_detected = True` and pivots to use **recent live Skinport cash averages** directly as the baseline floor, bypassing outdated historical data.

#### 3. Real-Time Sales History API Validation
* To guarantee absolute accuracy, the backend queries the **Skinport Sales History API** (`/v1/sales/history`) on-demand for every edge alert verification step.
* Since the endpoint has a strict rate limit (8 requests per 5 minutes), it is queried specifically for the target skin when validating an alert, rather than in bulk.
* It extracts the most recent active median price (prioritizing 24-hour volume, falling back to 7-day, 30-day, or 90-day intervals).

#### 4. Active Downtrend Penalty (Anti-Falling-Knife)
* The system evaluates recent pricing trends by comparing the 7-day sales median against the 30-day/90-day sales median.
* If an item's price is actively bleeding (7d median < 30d median), it flags `downtrend_detected = True`.
* It calculates the `downtrend_severity` and applies a dynamic penalty to the purchase threshold:
  $$\text{Applied Discount} = 15\% \text{ (Base)} + \min(15\%, \text{Downtrend Severity})$$
* For example, if a skin's price dropped by 10% over the last month, the system requires a **25% discount** (instead of the standard 15%) before the agent is allowed to execute a checkout, preventing buying into a falling knife.

---

## REST Endpoints

* **`POST /api/v1/alerts/anomaly`**
  Receives edge anomaly alerts (Z-score drops) and triggers the background verification reasoning loop.
* **`POST /api/v1/ingest/bulk`**
  Receives real-time market price ticks for bulk insertion into the database.

---

## MCP Server

* Server Name: `BrandSniperVerifier` (powered by `FastMCP`)
* Exposes the following tools to the AI reasoning agent:
  * `get_market_context`: Queries historical averages, checks liquidity, and retrieves snipe thresholds.
  * `verify_float_value`: Analyzes wear values (float standard vs premium bands).
  * `simulate_checkout_payload`: Performs secure checkout payload executions.
