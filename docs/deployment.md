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
| PostgreSQL 16 | `postgres:16-alpine` | `sniper_postgres` | disabled (uncomment to enable local dev) |
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
| Redis 7 | `sniper_edge_redis` | Port 6380, `--save "" --appendonly no` (volatile RAM only) |
| Listener | `sniper_listener` | Connects to a remote backend via `COMPUTE_NODE_URL` env var |

The edge stack is designed for constrained environments (Raspberry Pi, low-power VPS).
It contains only the hot-path services; the server node handles the cold path and infra.

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
| `GEMINI_API_KEY` | [Google AI Studio](https://aistudio.google.com/app/apikey) |
| `SKINPORT_CLIENT_ID` | [Skinport API](https://docs.skinport.com/) dashboard |
| `SKINPORT_CLIENT_SECRET` | [Skinport API](https://docs.skinport.com/) dashboard |

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
