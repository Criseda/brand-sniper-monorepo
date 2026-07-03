# Backend: Deterministic Rules Engine (DRE) & API

The Backend application is the **Hot Path** of the Brand Sniper engine. It is designed entirely for hyper-speed and deterministic execution, stripping away all heavy dependencies like Large Language Models (LLMs) or synchronous tracking architectures.

## ⚡ Core Components

### 1. Deterministic Rules Engine (`rules_engine.py`)
Replaces the old AI verifier. When a market tick arrives, the DRE queries the Redis Hot Cache to fetch the asset's rolling standard deviation (Z-Score) baseline. Because it uses native math and `O(1)` memory lookups instead of ML inference, it evaluates pricing anomalies in under a millisecond.

### 2. Paper Executor (`executor.py`)
When the DRE validates a `True` signal (e.g. Z-Score < -2.5), the executor instantly commits a `SimulatedTrade` row into the PostgreSQL database via `asyncpg`. This decouples the trading logic from external HTTP API bottlenecks during paper-trading phases.

### 3. Real-Time Observability (`telemetry.py`)
MLflow has been completely stripped from the hot path. Instead, the backend leverages non-blocking `prometheus_client` instruments:
- **`paper_trading_estimated_profit_total`**: A Gauge tracking total un-realized PnL.
- **`total_trade_executions`**: A Counter of successful snipes.
- **`rules_engine_latency_seconds`**: A Histogram measuring the microseconds spent verifying the trade.

## 🚀 Setup & Execution

### 1. Environment Configuration
Copy the example environment file:
```bash
cp .env.example .env
```
*(No AI API keys are required for the Backend app).*

### 2. Run the API Server
Ensure your Docker Compose stack (Redis, Postgres) is running first, then launch FastAPI:

```bash
uv run fastapi dev main.py --port 8080
```

### 3. View the Dashboards
- **FastAPI Swagger Docs**: `http://localhost:8080/docs`
- **Prometheus Metrics Scrape Endpoint**: `http://localhost:8080/metrics`
- **Grafana Live Dashboard**: `http://localhost:3000` (Use the `grafana_dashboard.json` file in this directory to import the visualizer).
