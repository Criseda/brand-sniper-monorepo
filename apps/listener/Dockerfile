FROM python:3.12-slim AS python-builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

COPY packages/shared_utils/ packages/shared_utils/
COPY apps/listener/pyproject.toml apps/listener/

# The latency benchmark (deployments/latency-bench) builds with the dev group, which brings psutil.
ARG LISTENER_SYNC_FLAGS=--no-dev
RUN uv sync --frozen ${LISTENER_SYNC_FLAGS} --package listener

COPY apps/listener/ apps/listener/

FROM python:3.12-slim

COPY --from=python-builder /app /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app/apps/listener

CMD ["uv", "run", "python", "main.py"]
