FROM ghcr.io/mlflow/mlflow:v3.14.0

# Install psycopg2-binary to allow PostgreSQL backend storage support
RUN pip install --no-cache-dir psycopg2-binary

COPY deployments/server-stack/mlflow-entrypoint.sh /usr/local/bin/mlflow-entrypoint
RUN chmod 0555 /usr/local/bin/mlflow-entrypoint

ENTRYPOINT ["/usr/local/bin/mlflow-entrypoint"]
