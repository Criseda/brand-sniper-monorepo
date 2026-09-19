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

## Active Milestone: Edge-Inference & Distillation Engine (Milestone 5)

> [!IMPORTANT]
> **Milestone Lifecycle Notice**:
> This section is active during execution of GitHub [Milestone 5](https://github.com/Criseda/brand-sniper-monorepo/milestone/5).
> Detailed architectural blueprints, feature vectors, and mathematical specifications are documented in [`docs/roadmap_edge_distillation.md`](docs/roadmap_edge_distillation.md).
> **Post-Milestone Action**: Once all 11 milestone issues are closed, delete `docs/roadmap_edge_distillation.md`, update `docs/architecture.md`, trim this section from `AGENTS.md`, and update `README.md`.

### Execution Sequence Matrix

Work MUST proceed in strict sequential order. Agents must not jump ahead to compiled runtimes (Rust) before upstream active learning datasets, ONNX exports, and benchmark baselines are established:

| Step | Issue | Phase | Focus | Upstream Prerequisite |
|:---:|:---:|:---:|:---|:---|
| **1** | [#232](https://github.com/Criseda/brand-sniper-monorepo/issues/232) | Phase 1 | `[EID-01]` Reservoir sampling for borderline ticks & near-misses | None |
| **2** | [#233](https://github.com/Criseda/brand-sniper-monorepo/issues/233) | Phase 1 | `[EID-02]` CFO Oracle automated ground-truth labeling pipeline | #232 |
| **3** | [#234](https://github.com/Criseda/brand-sniper-monorepo/issues/234) | Phase 1 | `[EID-03]` Walk-forward student model training & ONNX export | #233 |
| **4** | [#235](https://github.com/Criseda/brand-sniper-monorepo/issues/235) | Phase 1 | `[EID-04]` Wasserstein distance & PSI distribution drift monitor | #234 |
| **5** | [#175](https://github.com/Criseda/brand-sniper-monorepo/issues/175) | Phase 2 | `[PERFORMANCE]` Ingress-to-decision latency benchmark suite | None |
| **6** | [#236](https://github.com/Criseda/brand-sniper-monorepo/issues/236) | Phase 2 | `[EID-05]` In-memory ring buffers & in-process ONNX in Python | #175, #234 |
| **7** | [#237](https://github.com/Criseda/brand-sniper-monorepo/issues/237) | Phase 3 | `[EID-06]` Unified Rust daemon: direct WebSocket & SIMD-JSON | #236 |
| **8** | [#238](https://github.com/Criseda/brand-sniper-monorepo/issues/238) | Phase 3 | `[EID-07]` Lock-free in-memory ring buffer & in-process ORT (<50µs) | #237 |
| **9** | [#239](https://github.com/Criseda/brand-sniper-monorepo/issues/239) | Phase 3 | `[EID-08]` Atomic model hot-reloading & latency collapse report | #238 |
| **10** | [#33](https://github.com/Criseda/brand-sniper-monorepo/issues/33) | Phase 4 | `[INGESTION]` Expand multi-venue scrapers (CSFloat & Steam) | #238 |
| **11** | [#18](https://github.com/Criseda/brand-sniper-monorepo/issues/18) | Phase 4 | `[FEATURE]` Discord & Telegram real-time trade alerts | #238 |

### Agent Instructions for Working on an Issue
1. **Check Prerequisites**: Ensure preceding issues in the table are completed before starting downstream work.
2. **Review Architecture**: Read [`docs/roadmap_edge_distillation.md`](docs/roadmap_edge_distillation.md) for data schemas, mathematical formulations, and anti-patterns to avoid.
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
- **Service bootstrap**: `apps/backend/main.py`, `apps/listener/main.py`, and `apps/listener/replay_batches.py` call `setup_service_environment(__file__)` at the top (same dotenv/stream setup, no `sys.path` mutation)
- **Listener spawns Node.js sidecar** for WebSocket — lives in `scrapers/skinport_websocket/`

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
