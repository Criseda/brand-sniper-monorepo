FROM python:3.12-slim AS python-builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

COPY packages/shared_utils/ packages/shared_utils/
COPY apps/listener/pyproject.toml apps/listener/

RUN uv sync --frozen --no-dev --package listener

COPY apps/listener/ apps/listener/

FROM python:3.12-slim

RUN apt-get update && apt-get install -y curl gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_24.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=python-builder /app /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app/apps/listener/scrapers/skinport_websocket
RUN npm install --production

WORKDIR /app/apps/listener

CMD ["uv", "run", "python", "main.py"]
