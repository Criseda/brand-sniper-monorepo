# Analytics & Agentic Evaluation Pipeline

The Analytics application is the **Cold Path** of the Brand Sniper engine. It operates entirely offline and asynchronously to the live trading mechanisms. 

Its primary function is to act as the **Adversarial CFO**: an Agentic AI pipeline designed to brutally audit the synchronous paper trades executed by the Deterministic Rules Engine (DRE).

## The Adversarial CFO

To prevent the "Circular Feedback Loop" (where an AI grades its own performance using the same stale database metrics that triggered the trade), the CFO is equipped with tool functions that act as adversarial market checkers. These are registered via FastMCP and passed directly to the Gemini SDK as callable tools (bypassing the MCP server transport in the current prototype).

### Tool Functions (`tools.py`)
- `fetch_live_market_floor`: Intended to check the live floor price of an asset. Currently returns simulated adversarial data (prototype mock — replace with real API calls for production).
- `search_macro_trends`: Intended to scrape news/patch notes for macro market trends. Currently returns simulated responses (prototype mock — replace with a search API for production).

## Setup & Execution

### 1. Environment Configuration
Copy the example environment file and insert your API keys:
```bash
cp .env.example .env
```
Ensure `GEMINI_API_KEY` is populated. The pipeline automatically enforces rate-limiting safeguards for Free-Tier Gemini keys (query limits and cooldowns).

### 2. Update Edge Baselines
The Prefect pipeline calculates macro baseline statistics over the Postgres history and syncs them to the Edge Node's local Redis cache, providing the DRE with O(1) intelligence.
```bash
uv run python update_baselines.py
```

### 3. Run the Evaluation Flow
The Prefect flow queries the latest simulated trades and orchestrates the Gemini Agent to audit them.

```bash
# Execute the Daily CFO Evaluation flow
uv run python evaluate_performance.py
```

### 3. Review Audits in MLflow
Once the pipeline finishes, the CFO's Confidence Score (0-100) and its full reasoning rant are immutably logged.
Open your local **MLflow Tracking Server** (default: `http://localhost:5000`) and navigate to the `cfo-evaluation` experiment to view the artifact traces.
