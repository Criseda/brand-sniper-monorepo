FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

COPY packages/shared_utils/ packages/shared_utils/
COPY apps/analytics/pyproject.toml apps/analytics/

RUN uv sync --frozen --no-dev --package analytics

COPY apps/analytics/ apps/analytics/

FROM python:3.12-slim

COPY --from=builder /app /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app/apps/analytics

CMD ["uv", "run", "python", "evaluate_performance.py"]
