# Backend: API Core & Telemetry Gateway

The Backend application is a high-throughput **Cold Path API Node** running on the Windows Compute Engine. It has been stripped of all heavy processing and decision-making logic (the DRE was moved to the Edge Node). It acts as the central router for data persistence and telemetry observability.

## Core Components

### 1. Fast Data Ingestion (`main.py`)
Provides REST API endpoints (`/api/v1/ingest/bulk` and `/api/v1/ingest/trade`) for the Edge Node to asynchronously push market ticks and simulated trade logs. It utilizes native `SQLModel` async sessions to insert data into PostgreSQL efficiently, minimizing locking overhead.

### 2. Real-Time Observability (`telemetry.py`)
Acts as the central scraping target for the local Prometheus server. It leverages non-blocking `prometheus_client` instruments:
- **`paper_trading_estimated_profit_total`**: A Gauge tracking total un-realized PnL of simulated trades.
- **`paper_trades_executed_total`**: A Counter tracking the number of successful snipes executed by the Edge node.
- **`rules_engine_latency_seconds`**: A Histogram tracking DRE evaluation latency (sub-millisecond to 2s buckets).

## Setup & Execution

### 1. Environment Configuration
Copy the example environment file:
```bash
cp .env.example .env
```
*(No AI API keys are required for the Backend app).*

### 2. Run the API Server
Ensure your Docker Compose stack (Postgres, Prometheus, Grafana) is running first, then launch FastAPI:

```bash
uv run python main.py
```

### 3. View the Dashboards
- **FastAPI Swagger Docs**: `http://localhost:8080/docs`
- **Prometheus Metrics Scrape Endpoint**: `http://localhost:8080/metrics`
- **Grafana Live Dashboard**: `http://localhost:3000` (Use the `apps/backend/grafana_dashboard.json` file to import the visualizer).
