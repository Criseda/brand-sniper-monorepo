# Brand Sniper Monorepo — Agent Guide

Python 3.12 monorepo (uv workspaces) — algorithmic market sniping engine with 3 apps + 1 shared package.

## Commands

- **Docs**: user-facing guides live in `docs/` (getting-started, architecture, deployment)
- **Sync all deps** (must use `--all-packages`): `uv sync --all-packages`
- **Add dep**: run `uv add <pkg>` inside the target `apps/*` or `packages/*` directory, then `uv sync --all-packages` from root
- **Run app**: `uv run python main.py` from the app's directory (e.g. `apps/backend`, `apps/listener`)
- **Run tests**: `uv run pytest` from any app/package directory (or root); tests use `pytest` + `pytest-asyncio`
- **Lint**: `uv run ruff check` from root
- **Format**: `uv run ruff format` from root
- **Typecheck**: `uv run mypy apps/backend/ apps/listener/ apps/analytics/` from root
- **All quality checks**: `uv run ruff check && uv run ruff format --check && uv run mypy apps/backend/ apps/listener/ apps/analytics/`
- **Branch protection**: `main` branch requires PRs, CI status checks (`quality`, `test`), and linear history — configured in GitHub repo Settings > Branches
- **PR template**: `.github/pull_request_template.md` — filled automatically on new PRs
- **Contributing guide**: `CONTRIBUTING.md` — development workflow, branch naming, CI expectations
- **CI**: GitHub Actions workflow at `.github/workflows/ci.yml` — runs lint, format check, typecheck, and tests on push/PR to any branch
- **Config**: ruff and mypy configured in `pyproject.toml` and `mypy.ini` at root
- **Alembic migrations**: `uv run alembic upgrade head` from `deployments/` dir
- **Infra (Docker)**: `docker compose up -d` from `deployments/server-stack/` or `deployments/edge-stack/`

## Package boundaries

| Path | Role | Entrypoint |
|------|------|------------|
| `apps/backend` | FastAPI REST API (ingest, health, market context), FastMCP agent server, Prometheus metrics | `main.py:app` — uvicorn on `:8080` |
| `apps/listener` | Edge telemetry daemon — async scraping, Z-score anomaly detection, DRE | `main.py:process_live_telemetry_stream` — asyncio |
| `apps/analytics` | Prefect macro pipeline + Adversarial CFO (Gemini + FastMCP) | `evaluate_performance.py` (CFO), plus standalone scripts |
| `packages/shared_utils` | Shared SQLModel models, DB connection, item classifier, pricing utils | Re-exported via `__init__.py` |

## Key conventions

- **No emojis or emoji** in source code, logs, or comments
- **Prefix-based logging**: `[ANOMALY]`, `[BATCH FLUSH]`, `[ALERT APPROVED]`, `[PAPER TRADE]`, `[CFO]`, `[AGENT]`, `[SKINPORT]`
- **All DB models** in `packages/shared_utils/src/shared_utils/models.py` — do not add local models in apps
- **`contextvars.ContextVar`** for thread/async-safe telemetry (no global dicts)
- **Listener must be non-blocking**: use `aiohttp`, never `requests`
- **Edge Redis on `localhost:6380`** (not default 6379), `--save "" --appendonly no` (volatile RAM only)
- **FastMCP** for Gemini tool registration (`@mcp.tool()`), not ad-hoc JSON execution
- **CFO tools** in `apps/analytics/tools.py`, **backend tools** in `apps/backend/tools.py`
- **Listener spawns Node.js sidecar** for WebSocket — lives in `scrapers/skinport_websocket/`

## Testing quirks

- `pyproject.toml` has `pythonpath` set for test discovery (`apps/*`, `shared_utils/src`)
- Backend test must `sys.path.insert(0, ...)` to ensure `import main` resolves to `apps/backend/main.py` over root `main.py`
- Listener tests need `@pytest.mark.asyncio`
- Backend tests use FastAPI `TestClient` (synchronous) with a SQLite in-memory engine — no PostgreSQL needed
- Analytics tests mock `gemini_client` and `mlflow` globally; set `GEMINI_API_KEY` env var
- shared_utils tests are pure unit tests (no I/O)
- No integration test suite that requires Docker services
- Run `uv run pytest` from any app/package directory (or root)

## Migrations

- Alembic in `deployments/` — async engine (`asyncpg`), SQLModel `target_metadata`
- Generate: `uv run alembic revision --autogenerate -m "message"` (from `deployments/`)
- Apply: `uv run alembic upgrade head`
