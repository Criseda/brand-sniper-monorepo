# Brand Sniper dev container — Python 3.12 + uv, matching the CI toolchain.
# Image carries no Docker/Compose: run the service stacks with Docker Desktop
# on the host and reach them via forwarded ports (see devcontainer.json).
FROM python:3.12-slim-bookworm

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/root/.local/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends git curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
