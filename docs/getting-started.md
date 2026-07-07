# Getting Started

Run the full Brand Sniper stack on your machine in 5 minutes.

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
- `DATABASE_URL` — use Azure PostgreSQL, or uncomment `postgres:` in `deployments/server-stack/docker-compose.yml` for local dev
- `GEMINI_API_KEY` — get a free key at [aistudio.google.com](https://aistudio.google.com/app/apikey)
- `SKINPORT_CLIENT_ID` / `SKINPORT_CLIENT_SECRET` — your Skinport API creds

## 2. Install Dependencies

```bash
uv sync --all-packages
```

## 3. Start Infrastructure

```bash
cd deployments/server-stack
docker compose up -d
```

This starts 8 services:

| Service | Port | Role |
|---------|------|------|
| Grafana | `3000` | Dashboards (admin/admin) |
| Prometheus | `9090` | Metrics collection |
| Prefect Server | `4200` | Pipeline orchestration |
| MLflow | `5000` | Model registry & audit logs |
| Redis | `6379` | Market cache (volatile RAM) |
| Backend | `8080` | REST API + health |
| Listener | — | Market data ingestion |
| Analytics | — | CFO evaluation (manual profile) |

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
- **Analytics** (manual) — run `docker compose run --rm analytics` to trigger the Adversarial CFO

Check the Grafana dashboard at `http://localhost:3000` (admin/admin) and MLflow at `http://localhost:5000`.

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
