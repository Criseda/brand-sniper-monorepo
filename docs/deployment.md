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

## Local Docker Compose Overrides

Do not edit the committed `docker-compose.yml` files for personal local settings. Each stack has a tracked example override you can copy to a local file that Git ignores:

```bash
cd deployments/server-stack
cp docker-compose.override.example.yml docker-compose.override.yml
```

Docker Compose automatically merges `docker-compose.yml` and `docker-compose.override.yml` when both files are in the same directory:

```bash
docker compose up -d
```

Use the same workflow for the edge stack:

```bash
cd deployments/edge-stack
cp docker-compose.override.example.yml docker-compose.override.yml
docker compose up -d
```

The edge-stack override requires `COMPUTE_NODE_IP` to be set in the shell
environment or `deployments/edge-stack/.env` (it is not read from the root
`.env`). Point it at the central backend, e.g. `COMPUTE_NODE_IP=192.168.1.20`.

The override file is for local-only changes such as adding a local PostgreSQL service, setting development environment variables, or adding bind mounts while working on app code. To be explicit about the files Compose should use, run:

```bash
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d
```

To ignore the local override and run only the committed base stack:

```bash
docker compose -f docker-compose.yml up -d
```

Stop containers without removing them:

```bash
docker compose stop
```

Stop and remove containers and networks:

```bash
docker compose down
```

Stop and remove containers, networks, and named volumes:

```bash
docker compose down -v
```

If you need a completely separate local stack, create a full Compose file such as `docker-compose.local.yml` and run it explicitly with `docker compose -f docker-compose.local.yml up -d`. A partial override file usually cannot run by itself because it depends on services defined in the base file.

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

#### 1. Macro Baseline Calculation & Edge Redis Sync

* **Initial Seeding**: On first setup (or after wiping Redis), run a full calculation to build the baseline database table and populate the Redis cache for all 22k+ skins:
  ```bash
  # Trigger full calculation in the background
  docker compose run -d --rm analytics uv run python long_term_macro.py --limit 0
  ```
* **Daily Updates**: Because rolling averages (30d/90d averages, drift, and volatility) naturally shift as new daily transactions accrue, the pipeline must run periodically to update these metrics. A daily cron job (detailed below) recalculates baselines to keep Z-score anomaly detections accurate.
* **Testing/Dev**: You can run the pipeline without flags to quickly process a small, safe default subset:
  ```bash
  # Calculates baselines for the first 100 items to check Stack functionality
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
| `REDIS_PASSWORD` | Strong password used for securing the Edge Redis cache service |
| `MLFLOW_BACKEND_STORE_URI` | PostgreSQL connection URL with psycopg2 driver schema for MLflow data storage |

### Configuration Variables

| Variable | Description | Default (Local Dev) |
|----------|-------------|---------------------|
| `CORS_ORIGINS` | Comma-separated list of allowed origins for CORS. Used to explicitly whitelist domains since credentials are enabled (wildcards are not permitted with credentials). | `http://localhost:3000,http://localhost:8080` |
| `LISTENER_BATCH_MAX_ATTEMPTS` | Maximum delivery attempts for retryable bulk-ingestion failures. | `5` |
| `LISTENER_BATCH_RETRY_BASE_SECONDS` | Initial exponential-backoff delay for batch retries. | `0.5` |
| `LISTENER_BATCH_RETRY_MAX_SECONDS` | Maximum exponential-backoff delay for batch retries. | `8` |

### Bulk-ingestion recovery

The listener writes each outbound batch to the Edge Redis pending stream before
clearing its in-memory buffer. Successful requests are removed from the stream;
permanent errors and exhausted retries are moved to a dead-letter stream. Pending
batches are rescheduled automatically when the listener starts.

To replay up to 100 dead-letter batches after correcting the underlying error:

```bash
cd apps/listener
uv run python replay_batches.py --limit 100
```

The limit applies to attempted batches. The command exits non-zero if any
attempt fails, making it safe to use from operational automation. Malformed
stream records are isolated in `listener:ingest:malformed` so that one poison
record cannot block recovery of valid pending or dead-letter batches.

Batch IDs make replays idempotent at the backend. Reusing an ID with different
content returns HTTP 409. The stream uses the existing volatile Edge Redis
configuration, so it protects against listener/backend restarts but not an Edge
Redis restart or host loss. Use persistent Redis if that durability boundary is
not acceptable for a deployment.

### MLflow resource and credential settings

The repository uses synchronous MLflow tracking calls and does not use MLflow
job execution. The server stack therefore disables job execution, runs one web
worker, and enforces a 1 GiB memory limit. The server database URI is read from
`MLFLOW_BACKEND_STORE_URI`; it is not included in the process command line.
`MLFLOW_DATABASE_URL` remains a temporary entrypoint fallback for stale local
environment files and should be migrated.

Deployments that use MLflow online scoring or other server-managed jobs can opt
back in through a Compose override:

```yaml
services:
  mlflow-server:
    environment:
      MLFLOW_SERVER_ENABLE_JOB_EXECUTION: "true"
```

After deploying this change, rotate any MLflow database credential that was
previously visible in container process arguments, update the environment file,
and recreate the MLflow container.

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
- **Grafana** — `http://localhost:3000` — pre-built dashboards (using credentials in `.env`)
- **MLflow** — `http://localhost:5000` — model registry, CFO audit traces
- **Prefect** — `http://localhost:4200` — pipeline runs and task logs

## Production Considerations

- Configure Grafana admin credentials via `GF_SECURITY_ADMIN_USER` and `GF_SECURITY_ADMIN_PASSWORD` in `.env`
- Use a managed PostgreSQL (Azure, RDS) instead of the local postgres service
- Set `MLFLOW_TRACKING_URI` and `PREFECT_API_URL` to reachable endpoints
- Configure Prometheus retention and alerting rules for production uptime
