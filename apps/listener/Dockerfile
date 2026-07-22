FROM node:24-slim AS node-base

FROM python:3.12-slim AS python-builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

COPY packages/shared_utils/ packages/shared_utils/
COPY apps/listener/pyproject.toml apps/listener/

RUN uv sync --frozen --no-dev --package listener

COPY apps/listener/ apps/listener/

FROM python:3.12-slim

COPY --from=node-base /usr/local/bin/node /usr/local/bin/
COPY --from=node-base /usr/local/lib/node_modules /usr/local/lib/node_modules/
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx

COPY --from=python-builder /app /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app/apps/listener/scrapers/skinport_websocket
RUN npm install --production

WORKDIR /app/apps/listener

CMD ["uv", "run", "python", "main.py"]
