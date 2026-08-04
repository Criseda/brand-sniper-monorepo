# Brand Sniper Monorepo — task shortcuts.
# Bare `make` runs the full CI gate (see `check`).

.PHONY: setup  ## Sync all workspace deps (incl. dev group)
setup:
	uv sync --all-packages --group dev

.PHONY: lint  ## Ruff lint
lint:
	uv run ruff check

.PHONY: format  ## Auto-format with Ruff
format:
	uv run ruff format

.PHONY: format-check  ## Verify formatting (CI mode)
format-check:
	uv run ruff format --check

.PHONY: typecheck  ## Mypy on the three apps
typecheck:
	uv run mypy apps/backend/ apps/listener/ apps/analytics/

.PHONY: test  ## Run the test suite (coverage-instrumented, exactly like CI)
test:
	uv run coverage run -m pytest

.PHONY: testcov  ## Run tests and print the coverage gate report (fail_under 70%)
testcov: test
	uv run coverage report

.PHONY: check  ## Full quality gate: lint, format-check, typecheck, tests + coverage
check: lint format-check typecheck testcov

.DEFAULT_GOAL := check

.PHONY: migrate  ## Apply Alembic migrations (runs from deployments/)
migrate:
	cd deployments && uv run alembic upgrade head

STACK ?= server-stack
.PHONY: docker-up  ## docker compose up -d for $(STACK) (use STACK=edge-stack for edge)
docker-up:
	cd deployments/$(STACK) && docker compose up -d

.PHONY: docker-down  ## docker compose down for $(STACK)
docker-down:
	cd deployments/$(STACK) && docker compose down

.PHONY: help  ## Show all targets
help:
	@grep -E '^.PHONY: .*?## .*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ".PHONY: |## "}; {printf "\033[36m%-14s\033[0m %s\n", $$2, $$3}'
