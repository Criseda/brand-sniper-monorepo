#!/bin/sh
set -eu

# POSTGRES_EXPORTER_DSN wins when set (a read-only monitoring user, see docs/deployment.md).
# Otherwise the exporter reuses DATABASE_URL: the SQLAlchemy driver suffix (+asyncpg) is dropped
# and asyncpg's ssl= query parameter becomes libpq's sslmode=, so any PostgreSQL URL the apps use works.
if [ -n "${POSTGRES_EXPORTER_DSN:-}" ]; then
    DATA_SOURCE_NAME="$POSTGRES_EXPORTER_DSN"
elif [ -n "${DATABASE_URL:-}" ]; then
    DATA_SOURCE_NAME=$(printf '%s' "$DATABASE_URL" | sed -e 's#^postgres\(ql\)\{0,1\}+[a-z0-9_]*://#postgresql://#' -e 's#\([?&]\)ssl=#\1sslmode=#')
else
    echo "[POSTGRES EXPORTER] Set POSTGRES_EXPORTER_DSN or DATABASE_URL." >&2
    exit 1
fi
export DATA_SOURCE_NAME

exec /bin/postgres_exporter "$@"
