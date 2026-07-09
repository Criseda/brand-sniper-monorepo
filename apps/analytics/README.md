# Analytics & Agentic Evaluation Pipeline

The Analytics application is the **Cold Path** of the Brand Sniper engine. It operates entirely offline and asynchronously to the live trading mechanisms.

Its primary function is to act as the **Adversarial CFO**: an Agentic AI pipeline designed to brutally audit the synchronous paper trades executed by the Deterministic Rules Engine (DRE).

## The Adversarial CFO

To prevent the "Circular Feedback Loop" (where an AI grades its own performance using the same stale database metrics that triggered the trade), the CFO is equipped with tool functions that act as adversarial market checkers. These are registered as OpenAI-compatible function tools and passed to the Groq LLM as callable functions.

### Tool Functions (`tools.py`)
- `fetch_live_market_floor`: Checks the live floor price of an asset via the backend REST API. Returns simulated data when the backend is unavailable.
- `search_macro_trends`: Searches for macro market trends affecting item prices. Proxied through the backend.
- `verify_float_value`: Evaluates if an item's float wear value carries a price premium based on CS2 wear tier thresholds.

## Setup & Execution

### 1. Environment Configuration
Copy the example environment file and insert your API keys:
```bash
cp .env.example .env
```
Ensure `GROQ_API_KEY` is populated in the root `.env` or `apps/analytics/.env`.

### 2. Run the Evaluation & Macro flows

You can run the flows directly using `uv`, or inside the Docker container using `docker compose run` from the `deployments/server-stack/` directory.

#### Running directly with uv:
```bash
# Execute the Daily CFO Evaluation flow
uv run python evaluate_performance.py

# Execute the Long-Term Macro Trend Calculation & Edge Redis Sync
uv run python long_term_macro.py
```

#### Running via Docker Compose:
```bash
# Execute the Daily CFO Evaluation flow
docker compose run --rm analytics

# Execute the Long-Term Macro Trend Calculation & Edge Redis Sync
docker compose run --rm analytics uv run python long_term_macro.py
```

For more details on deploying the analytics container as a scheduled daily cron job in production, see the central [deployment guide](../../docs/deployment.md#running-the-analytics-container-periodic-jobs).


### 3. Review Audits in MLflow
Once the pipeline finishes, the CFO's Confidence Score (0-100) and its full reasoning are immutably logged.
Open your local **MLflow Tracking Server** (default: `http://localhost:5000`) and navigate to the `cfo-evaluation` experiment to view the artifact traces.
