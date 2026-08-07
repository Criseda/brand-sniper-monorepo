# Backend: API Core & Telemetry Gateway

The Backend application is a high-throughput **Cold Path API Node** running on the server node. It has been stripped of all heavy processing and decision-making logic (the DRE was moved to the Edge Node). It acts as the central router for data persistence and telemetry observability.

## Core Components

### 1. Fast Data Ingestion (`main.py`)
Provides REST API endpoints (`/api/v1/ingest/bulk` and `/api/v1/ingest/trade`) for the Edge Node to asynchronously push market ticks and simulated trade logs. It utilizes native `SQLModel` async sessions to insert data into PostgreSQL efficiently, minimizing locking overhead.

### 2. Real-Time Observability (`telemetry.py`)
Acts as the central scraping target for the local Prometheus server. It leverages non-blocking `prometheus_client` instruments:
- **`paper_trading_estimated_profit_total`**: A Gauge tracking total un-realized PnL of simulated trades.
- **`paper_trades_executed_total`**: A Counter tracking the number of successful snipes executed by the Edge node.
- **`rules_engine_latency_seconds`**: A Histogram tracking DRE evaluation latency (sub-millisecond to 2s buckets).

### 3. Stable API Errors (`api_errors.py`)

All API errors use the RFC 9457 `application/problem+json` format. Invalid
requests return `422`, unknown market items return `404`, unavailable database
dependencies return `503`, and unexpected failures return a sanitized `500`.
Every occurrence includes an opaque `instance` identifier for log correlation;
internal exception details are logged but never returned to clients.

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
- **Grafana Live Dashboard**: `http://localhost:3000` — auto-provisioned with the [sniper_dashboard.json](../deployments/server-stack/config/grafana/dashboards/sniper_dashboard.json) (18 panels covering trading, ingestion, anomaly detection, performance, and system health).
