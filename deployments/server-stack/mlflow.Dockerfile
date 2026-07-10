FROM ghcr.io/mlflow/mlflow:v3.14.0

# Install psycopg2-binary to allow PostgreSQL backend storage support
RUN pip install --no-cache-dir psycopg2-binary
