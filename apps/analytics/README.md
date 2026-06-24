# Macro Analytics, Forecasting, and Ingestion Engine

This service handles bulk data validation, ingestion of millions of historical database records, and schedules long-term macroeconomic pricing analysis using Prefect.

## Script Directories

### 1. Seeding & Validation Scripts
* **`validate_historical.py`**
  Scans Kaggle transaction CSV files for format corruptions, null entries, or invalid prices. Outputs dry-run reports before write phases.
* **`seed_historical.py`**
  A high-performance bulk seeder. Drops postgres indexes, parses CS2 item types using the shared classifier, and streams vectorized data into the `historical_prices` table using `asyncpg` copy.
* **`verify_seed_historical.py`**
  A post-ingestion sweep verifying database integrity, row counts, index health, and duplication groups.

### 2. Prefect Orchestration: Macro Trends Pipeline
* **`long_term_macro.py`**
  The central analytical cron script. Orchestrated via Prefect 3.0 tasks and flows. It computes the following indicators for each tracked asset:
  * **Moving Averages:** 30-day and 90-day pricing baselines.
  * **Price Drift:** Shift percentages to identify macro market trends.
  * **Volatility:** 30-day daily standard deviation to quantify risk.
  * **30-Day Volume:** Historical liquidity indicator (used for trade authorization).
  * **Support Floor:** 10th percentile pricing representing buy-zones.
  * **Seasonality:** Evaluates average monthly returns to capture cyclical market shifts.
* Recalculations are written directly back to the database `item_macro_baselines` table using high-speed upsert tasks.

---

## Getting Started

### 1. Set Up Configuration
Verify that the `PREFECT_API_URL` environment setting exists in `apps/analytics/.env` to route workflow telemetry to your local Prefect container server:
```ini
PREFECT_API_URL="http://localhost:4200/api"
```

### 2. Running Analytics Locally
To run the macro trend calculations on a local subset of assets for debugging:
```bash
uv run python apps/analytics/long_term_macro.py
```

Check details and execution graphs directly on your local Prefect Dashboard:
`http://localhost:4200`
