# Analytics & Agentic Evaluation Pipeline

The Analytics application is the **Cold Path** of the Brand Sniper engine. It operates entirely offline and asynchronously to the live trading mechanisms. 

Its primary function is to act as the **Adversarial CFO**: an Agentic AI pipeline designed to brutally audit the synchronous paper trades executed by the Deterministic Rules Engine (DRE).

## 🧠 The Adversarial CFO

To prevent the "Circular Feedback Loop" (where an AI grades its own performance using the same stale database metrics that triggered the trade), the CFO is equipped with real-world scraping capabilities via the **Model Context Protocol (MCP)**.

### FastMCP Tools (`tools.py`)
- `fetch_live_market_floor`: Reaches out to the live internet to check the actual current floor price of an asset, identifying if the bot's internal baseline was stale.
- `search_macro_trends`: Scrapes recent news and patch notes to determine if a severe macro market crash (falling knife) is occurring.

## 🚀 Setup & Execution

### 1. Environment Configuration
Copy the example environment file and insert your API keys:
```bash
cp .env.example .env
```
Ensure `GEMINI_API_KEY` is populated. The pipeline automatically enforces rate-limiting safeguards for Free-Tier Gemini keys (query limits and cooldowns).

### 2. Run the Evaluation Flow
The Prefect flow queries the latest simulated trades and orchestrates the Gemini Agent to audit them.

```bash
# Execute the Daily CFO Evaluation flow
uv run python evaluate_performance.py
```

### 3. Review Audits in MLflow
Once the pipeline finishes, the CFO's Confidence Score (0-100) and its full reasoning rant are immutably logged.
Open your local **MLflow Tracking Server** (default: `http://localhost:5000`) and navigate to the `cfo-evaluation` experiment to view the artifact traces.
