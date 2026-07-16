<div align="center">
  <h1>Brand Sniper</h1>
  <p><strong>Algorithmic CS2 Market Sniping & Macro Arbitrage Engine</strong></p>
  <p><em>A production-grade, distributed data and AI system engineered to detect real-time market pricing anomalies and forecast long-term macroeconomic asset trends.</em></p>
</div>

<p align="center">
  <a href="https://github.com/Criseda/brand-sniper-monorepo/actions/workflows/ci.yml">
    <img src="https://github.com/Criseda/brand-sniper-monorepo/actions/workflows/ci.yml/badge.svg" alt="CI Status">
  </a>
  <a href="https://www.python.org/downloads/release/python-3120/">
    <img src="https://img.shields.io/badge/python-3.12-blue?logo=python" alt="Python 3.12">
  </a>
  <a href="https://github.com/astral-sh/uv">
    <img src="https://img.shields.io/badge/package%20manager-uv-8A2BE2" alt="uv">
  </a>
  <a href="https://github.com/astral-sh/ruff">
    <img src="https://img.shields.io/badge/code%20style-ruff-000000" alt="Ruff">
  </a>
  <a href="https://mypy-lang.org/">
    <img src="https://img.shields.io/badge/types-mypy-blue" alt="Mypy">
  </a>
</p>

---

A **Deterministic Rules Engine (DRE)** on the hot path instantly paper-trades statistical anomalies, while an **Agentic AI Pipeline (The Adversarial CFO)** independently audits those trades asynchronously using Groq.

---

## Quick Start

```bash
cp .env.example .env          # Fill in DATABASE_URL, GROQ_API_KEY, SKINPORT_*
uv sync --all-packages
cd deployments/server-stack && docker compose up -d
curl http://localhost:8080/health
```

See [docs/getting-started.md](docs/getting-started.md) for the full walkthrough.

---

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

---

## Tech Stack

| Category | Technology |
|---|---|
| **Runtime** | Python 3.12 (uv) |
| **Database** | PostgreSQL (SQLModel, Alembic, asyncpg) |
| **Cache** | Redis (volatile RAM, port 6380) |
| **Orchestration** | Prefect Server |
| **AI** | Groq (OpenAI-compatible) |
| **Tracking** | MLflow |
| **Observability** | Prometheus, Grafana |
| **Infra** | Docker Compose, uv workspaces |

---

## Repository Structure

```
apps/
  backend/       FastAPI REST API (port 8080)
  listener/      Edge telemetry daemon (DRE hot path)
  analytics/     Prefect CFO pipeline (cold path)
packages/
  shared_utils/  SQLModel models, DB connection, classifiers
deployments/
  server-stack/  Full system Docker Compose (8 services)
  edge-stack/    Minimal edge Docker Compose (Redis + listener)
docs/
  getting-started.md  5-minute quickstart
  architecture.md     System design, topology, data flow
  deployment.md       Docker stacks, migrations, operations
```

---

## Documentation

| Guide | Description |
|---|---|
| [docs/getting-started.md](docs/getting-started.md) | Clone, configure, and run the full stack in 5 minutes |
| [docs/architecture.md](docs/architecture.md) | Hot path / cold path architecture, topology diagram |
| [docs/deployment.md](docs/deployment.md) | Docker stacks, env vars, migrations, monitoring |

---

## Quality Assurance

```bash
uv run ruff check
uv run ruff format --check
uv run mypy apps/backend/ apps/listener/ apps/analytics/
uv run pytest
```

All checks run via GitHub Actions on every push/PR. See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow.

---

## Contributing

1. Branch from `main` (`feat/`, `fix/`, etc.)
2. Make changes following [AGENTS.md](AGENTS.md) conventions
3. Run quality checks (above)
4. Open a PR against `main`

---

## License

GNU Affero General Public License v3.0 (AGPLv3). See [LICENSE](LICENSE).
