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
* If they deviate by **more than 35%**, the system flags `regime_shift_detected = True` and pivoits to use **recent live Skinport cash averages** directly as the baseline floor, bypassing outdated historical data.

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
