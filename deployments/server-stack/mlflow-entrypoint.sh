#!/bin/sh
set -eu

if [ -z "${MLFLOW_BACKEND_STORE_URI:-}" ]; then
    if [ -n "${MLFLOW_DATABASE_URL:-}" ]; then
        echo "[MLFLOW] MLFLOW_DATABASE_URL is deprecated; use MLFLOW_BACKEND_STORE_URI." >&2
        export MLFLOW_BACKEND_STORE_URI="$MLFLOW_DATABASE_URL"
    else
        echo "[MLFLOW] MLFLOW_BACKEND_STORE_URI is required." >&2
        exit 1
    fi
fi

exec mlflow server \
    --host 0.0.0.0 \
    --port 5000 \
    --artifacts-destination /mlruns \
    --serve-artifacts
