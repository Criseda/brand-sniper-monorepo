# Getting Started

Run the full Brand Sniper stack on your machine in 5 minutes.

## Option: Open in GitHub Codespaces

Open the repo in a pre-provisioned container — Python 3.12, uv, and the VS Code extensions (ruff, mypy, TOML) come ready:

1. Click **Code > Codespaces > Create codespace on main** on the repo page (or run `gh codespace create`).
2. Wait for `postCreateCommand` to finish — it runs `uv sync --all-packages --group dev`, copies `.env.example` to `.env` if missing, and installs the pre-commit hooks.
3. You still need the **Docker service stacks** (Redis, Prefect, MLflow, Grafana): the container does not bundle Docker or Compose, so run `docker compose up -d` in `deployments/server-stack/` on your Docker host. Redis is container-internal; the backend is published on `8080`, while administrative UIs bind to the Docker host's loopback interface.

## Option: Manual setup

## Prerequisites

- **Python 3.12** — install from [python.org](https://python.org)
- **uv** — `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`
- **Docker & Docker Compose** — [Docker Desktop](https://docker.com/products/docker-desktop)

## 1. Clone & Env Setup

```bash
git clone https://github.com/Criseda/brand-sniper-monorepo.git
cd brand-sniper-monorepo
cp .env.example .env
```

Open `.env` and at minimum set:
- `DATABASE_URL` — use Azure PostgreSQL, or copy `deployments/server-stack/docker-compose.override.example.yml` to `docker-compose.override.yml` for local PostgreSQL
- `GROQ_API_KEY` — get a free key at [console.groq.com/keys](https://console.groq.com/keys)
- `SKINPORT_CLIENT_ID` / `SKINPORT_CLIENT_SECRET` — your [Skinport API](https://docs.skinport.com/) creds
- `BACKEND_API_KEY` — generate a shared 256-bit key with `openssl rand -hex 32` (or PowerShell: `[Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLower()`)

Use the same `BACKEND_API_KEY` in the root `.env` on the server and every edge
node. Backend `/api/v1/**` routes reject requests without it; the listener and
analytics clients attach it automatically.

## 2. Install Dependencies

```bash
uv sync --all-packages
```

## 3. Start Infrastructure

```bash
cd deployments/server-stack
docker compose up -d
```

This starts 8 long-running services. Infrastructure ports marked "loopback" are reachable only from the Docker host:

| Service | Host port | Role |
|---------|-----------|------|
| Grafana | `127.0.0.1:3000` | Dashboards (configured in `.env`) |
| Prometheus | `127.0.0.1:9090` | Metrics collection |
| Prefect Server | `127.0.0.1:4200` | Pipeline orchestration |
| MLflow | `127.0.0.1:5000` | Model registry and audit logs |
| Redis | Not published | Market cache (volatile RAM) |
| Redis exporter | Not published | Redis metrics bridge |
| Backend | `0.0.0.0:8080` | REST API and health |
| Listener | Not published | Market data ingestion |

Analytics uses the manual profile and starts only with `docker compose run --rm analytics`.

Verify the backend is healthy:

```bash
curl http://localhost:8080/health
# {"status":"healthy","version":"1.0.0"}
```

## 4. Run Database Migrations

```bash
cd deployments
uv run alembic upgrade head
```

## 5. What's Running?

- **Listener** — streaming Skinport sales data, detecting anomalies in real-time
- **Backend** — serving the API at `http://localhost:8080/docs`
- **Analytics** (manual) — run `docker compose run --rm analytics` to trigger the Adversarial CFO (see [deployment.md](deployment.md#running-the-analytics-container-periodic-jobs) for details on periodic execution and macro sync)

Check the Grafana dashboard at `http://localhost:3000` (using credentials configured in `.env`) and MLflow at `http://localhost:5000`.

## Running Outside Docker

### Listener (Edge)
```bash
cd apps/listener
uv run python main.py
```

### Backend
```bash
cd apps/backend
uv run python main.py
```

### Analytics (CFO)
```bash
cd apps/analytics
uv run python evaluate_performance.py
```
