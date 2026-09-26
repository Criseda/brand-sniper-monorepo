# Brand Sniper Monorepo — Agent Guide

Python 3.12 monorepo (uv workspaces) — algorithmic market sniping engine with 3 apps + 1 shared package.

## Commands

- **Docs**: user-facing guides live in `docs/` (getting-started, architecture, deployment)
- **Sync all deps** (must use `--all-packages`): `uv sync --all-packages`
- **Add dep**: run `uv add <pkg>` inside the target `apps/*` or `packages/*` directory, then `uv sync --all-packages` from root
- **Run app**: `uv run python main.py` from the app's directory (e.g. `apps/backend`, `apps/listener`)
- **Run tests**: `uv run pytest` from any app/package directory (or root); tests use `pytest` + `pytest-asyncio`
- **Run coverage**: `uv run coverage run -m pytest && uv run coverage report` from root; `fail_under` in `pyproject.toml` is enforced in CI
- **Lint**: `uv run ruff check` from root
- **Format**: `uv run ruff format` from root
- **Typecheck**: `uv run mypy apps/backend/ apps/listener/ apps/analytics/` from root
- **All quality checks**: `uv run ruff check && uv run ruff format --check && uv run mypy apps/backend/ apps/listener/ apps/analytics/`
- **Pre-commit gate** (ruff check --fix, ruff format, mypy): install once with `uv run pre-commit install`; runs on every `git commit`. Force full pass: `uv run pre-commit run --all-files`. Config: `.pre-commit-config.yaml` (local hooks resolving via `uv run` — the locked toolchain)
- **Task shortcuts**: the root `Makefile` wraps the commands above — `make setup` (sync), `make check` (full CI gate: lint + format-check + typecheck + tests + coverage), `make testcov` (tests + coverage report), `make migrate` (alembic from `deployments/`), `make docker-up STACK=server-stack|edge-stack`, `make docker-down`, `make help` (all targets). Bare `make` runs `check`.
- **Branch protection**: `main` branch requires PRs, CI status checks (`quality`, `test`), and linear history — configured in GitHub repo Settings > Branches
- **PR template**: `.github/pull_request_template.md` — filled automatically on new PRs
- **Contributing guide**: `CONTRIBUTING.md` — development workflow, branch naming, CI expectations
- **CI**: GitHub Actions workflow at `.github/workflows/ci.yml` — runs lint, format check, typecheck, and tests on push/PR to any branch
- **Config**: ruff and mypy configured in `pyproject.toml` and `mypy.ini` at root
- **Alembic migrations**: `uv run alembic upgrade head` from `deployments/` dir
- **Infra (Docker)**: `docker compose up -d` from `deployments/server-stack/` or `deployments/edge-stack/`

## Active Milestone: Proven Edge (Milestone 5)

> [!IMPORTANT]
> **Milestone Lifecycle Notice**:
> This section is active during execution of GitHub [Milestone 5](https://github.com/Criseda/brand-sniper-monorepo/milestone/5).
> Direction, label definitions, the feature contract, promotion rules, and anti-patterns are documented in [`docs/roadmap_proven_edge.md`](docs/roadmap_proven_edge.md).
> **Post-Milestone Action**: Once all scheduled milestone issues are closed, delete `docs/roadmap_proven_edge.md`, update `docs/architecture.md`, trim this section from `AGENTS.md`, and update `README.md`.

**Direction**: make Brand Sniper prove its edge. Record every listing, label it by what the market actually did (fee-aware P&L), backtest the current rules as the baseline, and only then adopt a learned model through shadow mode. LLM output is never used as training labels.

### Execution Sequence Matrix

Respect the prerequisites below. After #232, the tracks #233, #248, #18, and #33 may proceed in parallel:

| Step | Issue | Focus | Upstream Prerequisite |
|:---:|:---:|:---|:---|
| **1** | [#232](https://github.com/Criseda/brand-sniper-monorepo/issues/232) | `[PE-01]` Raw feed capture & listing-level tick persistence | None |
| **2** | [#233](https://github.com/Criseda/brand-sniper-monorepo/issues/233) | `[PE-02]` Fee-aware P&L function & market-outcome labeling | #232 |
| **3** | [#248](https://github.com/Criseda/brand-sniper-monorepo/issues/248) | `[PE-03]` Deterministic replay & backtest harness | #232 |
| **4** | [#249](https://github.com/Criseda/brand-sniper-monorepo/issues/249) | `[PE-04]` Baseline scorecard for the current Z-score DRE | #233, #248 |
| **5** | [#175](https://github.com/Criseda/brand-sniper-monorepo/issues/175) | `[PE-05]` Ingress-to-decision latency benchmark (Rust decision gate) | #248 |
| **6** | [#234](https://github.com/Criseda/brand-sniper-monorepo/issues/234) | `[PE-06]` Shared feature module & walk-forward student model (ONNX) | #249 |
| **7** | [#236](https://github.com/Criseda/brand-sniper-monorepo/issues/236) | `[PE-07]` Shadow-mode in-process ONNX scoring with promotion gate | #234, #175 |
| **8** | [#250](https://github.com/Criseda/brand-sniper-monorepo/issues/250) | `[PE-08]` CFO calibration experiment against realized outcomes | #233, #249 |
| **9** | [#235](https://github.com/Criseda/brand-sniper-monorepo/issues/235) | `[PE-09]` Realized-performance & feature drift monitoring | #236 |
| **10** | [#18](https://github.com/Criseda/brand-sniper-monorepo/issues/18) | `[PE-10]` Discord/Telegram alerts with direct listing links | #232 |
| **11** | [#33](https://github.com/Criseda/brand-sniper-monorepo/issues/33) | `[PE-11]` Multi-venue scrapers (CSFloat & Steam) | #232 |

**Parked**: #237, #238, #239 (compiled Rust edge engine) are not scheduled. Do not start them; a dedicated decision session will review the #175 evidence first.

### Agent Instructions for Working on an Issue
1. **Check Prerequisites**: Ensure preceding issues in the table are completed before starting downstream work.
2. **Review Architecture**: Read [`docs/roadmap_proven_edge.md`](docs/roadmap_proven_edge.md) for label definitions, the feature contract, promotion rules, and anti-patterns to avoid.
3. **Bound Scope**: Do not expand code changes beyond the assigned issue ticket.
4. **Update Changelog**: When submitting a PR or closing an issue, add a 1-line entry in [`CHANGELOG.md`](CHANGELOG.md) under `[Unreleased]`.
5. **Maintain CI Gates**: Ensure `uv run pytest`, `uv run ruff check`, and `uv run mypy` pass with 0 errors and coverage >= 70%.


## Package boundaries

| Path | Role | Entrypoint |
|------|------|------------|
| `apps/backend` | FastAPI REST API (ingest, health, market context), Prometheus metrics | `main.py:app` — uvicorn on `:8080` |
| `apps/listener` | Edge telemetry daemon — async scraping, Z-score anomaly detection, DRE | `main.py:process_live_telemetry_stream` — asyncio |
| `apps/analytics` | Prefect macro pipeline + Adversarial CFO (Groq) | `evaluate_performance.py` (CFO), plus standalone scripts |
| `packages/shared_utils` | Shared SQLModel models, DB connection, item classifier, pricing utils | Re-exported via `__init__.py` |

## Key conventions

- **Readability over clever compactness**: Prefer clean, explicit, and human-readable code over hyper-compact or "clever" one-liners, provided it does not compromise execution speed or latency budgets. Use descriptive variable names and clear control flow.
- **No emojis or emoji** in source code, logs, or comments
- **Prefix-based logging**: `[ANOMALY]`, `[BATCH FLUSH]`, `[ALERT APPROVED]`, `[PAPER TRADE]`, `[CFO]`, `[AGENT]`, `[SKINPORT]`
- **All DB models** in `packages/shared_utils/src/shared_utils/models.py` — do not add local models in apps
- **`contextvars.ContextVar`** for thread/async-safe telemetry (no global dicts)
- **Listener must be non-blocking**: use `aiohttp`, never `requests`
- **Edge Redis on `localhost:6380`** (not default 6379), `--save "" --appendonly no` (volatile RAM only)
- **CFO tools** in `apps/analytics/tools.py` — plain functions, registered as OpenAI-compatible function tools
- **Script bootstrap**: analytics scripts call `setup_script_environment(__file__)` at the top and `validate_required_env([...])` inside `__main__` (never at import) — do not re-add `load_dotenv`/`sys.path`/`reconfigure` boilerplate
- **Service bootstrap**: `apps/backend/main.py`, `apps/listener/main.py`, and `apps/listener/replay_batches.py` call `setup_service_environment(__file__)` at the top (`apps/listener/backtest/__main__.py` passes its package directory so the listener `.env` loads) (same dotenv/stream setup, no `sys.path` mutation)
- **Listener spawns Node.js sidecar** for WebSocket — lives in `scrapers/skinport_websocket/`
- **Data sources**: read [`docs/data_sources.md`](docs/data_sources.md) before touching baselines, backtests, training data, or a new venue. It records what data exists, the Kaggle Steam dataset (pre-crash, wrong venue, static: not a live reference price), the Skinport data we hold, and that the stacks run on a PC that is not always on
- **Skinport API**: read [`docs/skinport_feed.md`](docs/skinport_feed.md) before touching Skinport code. It links the official docs ([sale feed](https://docs.skinport.com/websocket/sale-feed), [items](https://docs.skinport.com/items), [sales history](https://docs.skinport.com/sales/history), [account transactions](https://docs.skinport.com/account/transactions)) and records where the live feed differs from them (e.g. `saleId` is always null; `productId` is the listing key)

## Testing quirks

- `pyproject.toml` has `pythonpath` set for test discovery (`apps/*`, `shared_utils/src`)
- Backend tests rely on the pytest `pythonpath` entry for `apps/backend` so `import main` resolves to `apps/backend/main.py` over root `main.py`
- Listener tests need `@pytest.mark.asyncio`
- Backend tests use FastAPI `TestClient` (synchronous) with a SQLite in-memory engine — no PostgreSQL needed
- Analytics tests mock `client` and `mlflow` globally; set the `LLM_*` env vars (`LLM_BASE_URL`, `LLM_MODEL`, `LLM_API_KEY`) — see `apps/analytics/tests/conftest.py`
- shared_utils tests are pure unit tests (no I/O)
- No integration test suite that requires Docker services
- Run `uv run pytest` from any app/package directory (or root)

## Migrations

- Alembic in `deployments/` — async engine (`asyncpg`), SQLModel `target_metadata`
- Generate: `uv run alembic revision --autogenerate -m "message"` (from `deployments/`)
- Apply: `uv run alembic upgrade head`
