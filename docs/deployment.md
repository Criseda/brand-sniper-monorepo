# Deployment

## Docker Stacks

Two Docker Compose stacks are provided for different deployment scenarios:

| Stack | Directory | Use Case |
|-------|-----------|----------|
| **Server Stack** | `deployments/server-stack/` | Full system: backend, listener, analytics, infra |
| **Edge Stack** | `deployments/edge-stack/` | Lightweight: Redis + listener for constrained devices |

### Server Stack Services

```bash
cd deployments/server-stack
docker compose up -d
```

| Service | Image | Container Name | Profile |
|---------|-------|----------------|---------|
| Redis 8 | `redis:8-alpine` | `sniper_edge_redis` | always |
| PostgreSQL 18 | `postgres:18-alpine` | `sniper_postgres` | disabled (uncomment to enable local dev) |
| Prefect Server | `prefecthq/prefect:3-latest` | `sniper_prefect_server` | always |
| MLflow | `ghcr.io/mlflow/mlflow:v3.14.0` | `sniper_mlflow_server` | always |
| Prometheus | `prom/prometheus:latest` | `sniper_prometheus` | always |
| Grafana | `grafana/grafana:latest` | `sniper_grafana` | always |
| Backend | custom build | `sniper_backend` | always |
| Listener | custom build | `sniper_listener` | always |
| Analytics | custom build | `sniper_analytics` | manual (`docker compose run --rm analytics`) |

### Edge Stack Services

```bash
cd deployments/edge-stack
docker compose up -d
```

| Service | Container Name | Notes |
|---------|----------------|-------|
| Redis 8 | `sniper_edge_redis` | Port 6380, `--save "" --appendonly no` (volatile RAM only) |
| Listener | `sniper_listener` | Connects to a remote backend via `COMPUTE_NODE_URL` env var |

The edge stack is designed for constrained environments (Raspberry Pi, low-power VPS).
It contains only the hot-path services; the server node handles the cold path and infra.

## Running the Analytics Container (Periodic Jobs)

The Analytics container (`analytics`) is the cold-path evaluation and macro analysis system. It is configured with the `manual` profile to prevent it from running as a persistent daemon. Instead, it is designed to be executed periodically (typically daily) as scheduled batch jobs.

### Why Periodic Runs are Required

1. **Macro Baseline Updates**: The hot-path Deterministic Rules Engine (DRE) checks real-time prices against long-term baselines stored in the Edge Redis cache. If the macro pipeline does not run, these baselines become stale, leading to incorrect Z-score anomaly detection.
2. **Adversarial CFO Audits**: The LLM-powered CFO audits recent trades to ensure decision quality, logging the confidence scores and structured reasoning traces to MLflow.

### How to Run the Jobs

Ensure you are in the server-stack directory:
```bash
cd deployments/server-stack
```

#### 1. Daily Macro Baseline Calculation & Edge Redis Sync
This calculates the 30-day and 90-day rolling price averages, support floors, price drift, and volatility metrics based on historical database entries, and then pushes them to the Edge Redis cache.
```bash
docker compose run --rm analytics uv run python long_term_macro.py
```

#### 2. Daily CFO Performance Audit
This triggers the LLM agent to audit the bot's logged simulated trades against live floors and macro news to check trade quality.
```bash
docker compose run --rm analytics
```
*(By default, the container runs `uv run python evaluate_performance.py` as its entrypoint command).*

### Production Scheduling (Cron)

In a production environment, schedule these jobs to run once a day. For example, using system cron:

```text
# Run macro baseline calculation at 00:00 every day
0 0 * * * cd /path/to/deployments/server-stack && docker compose run --rm analytics uv run python long_term_macro.py >> /var/log/sniper_macro.log 2>&1

# Run CFO performance evaluation at 01:00 every day
0 1 * * * cd /path/to/deployments/server-stack && docker compose run --rm analytics >> /var/log/sniper_cfo.log 2>&1
```

## Environment Variables

Secrets are configured via `.env` files (not committed):

1. **Root `.env`** — global config (database, API keys, anomaly params)
2. **`apps/analytics/.env`** — overrides for the analytics app
3. **`apps/backend/.env`** — overrides for the backend app
4. **`apps/listener/.env`** — overrides for the listener app

Each app's Docker container uses `env_file:` to load the root `.env`, then applies
additional environment variables from the compose file for Docker-internal networking
(e.g. `COMPUTE_NODE_IP=backend`).

### Required Secrets

| Variable | Source |
|----------|--------|
| `DATABASE_URL` | Azure PostgreSQL connection string, or local postgres |
| `GROQ_API_KEY` | [Groq Console](https://console.groq.com/keys) |
| `SKINPORT_CLIENT_ID` | [Skinport API](https://docs.skinport.com/) dashboard |
| `SKINPORT_CLIENT_SECRET` | [Skinport API](https://docs.skinport.com/) dashboard |

### Configuration Variables

| Variable | Description | Default (Local Dev) |
|----------|-------------|---------------------|
| `CORS_ORIGINS` | Comma-separated list of allowed origins for CORS. Used to explicitly whitelist domains since credentials are enabled (wildcards are not permitted with credentials). | `http://localhost:3000,http://localhost:8080` |

## Database Migrations

The project uses **Alembic** with an async engine (`asyncpg`).

```bash
cd deployments

# Apply all pending migrations
uv run alembic upgrade head

# Generate a new migration
uv run alembic revision --autogenerate -m "description of change"
```

## Building Custom Images

Images are built automatically by `docker compose up` when the Dockerfile changes.
To rebuild explicitly:

```bash
cd deployments/server-stack

# Single service
docker compose build backend

# All custom services
docker compose build backend listener analytics
```

## Container Networking

Services communicate by container name within the Docker network (`sniper_network`):

- Backend: `http://backend:8080`
- Redis: `redis://redis:6379`
- Prefect: `http://prefect-server:4200`
- MLflow: `http://mlflow-server:5000`

Docker-internal URLs are set via environment overrides in the compose file
and take precedence over the root `.env` values.

## Monitoring

- **Prometheus** — `http://localhost:9090` — scrapes `backend:8080/metrics`
- **Grafana** — `http://localhost:3000` — pre-built dashboards (admin/admin)
- **MLflow** — `http://localhost:5000` — model registry, CFO audit traces
- **Prefect** — `http://localhost:4200` — pipeline runs and task logs

## Production Considerations

- Replace the default Grafana admin password via `GRAFANA_ADMIN_PASSWORD` in `.env`
- Use a managed PostgreSQL (Azure, RDS) instead of the local postgres service
- Set `MLFLOW_TRACKING_URI` and `PREFECT_API_URL` to reachable endpoints
- Configure Prometheus retention and alerting rules for production uptime
