# =============================================================================
# Custom Airflow Image
# =============================================================================
# Base: apache/airflow:2.9.3-python3.11
# Adds: boto3 (MinIO/S3), structlog, pydantic, confluent-kafka, dbt-core
#
# Build: docker build -t thelook/airflow:2.9.3 docker/airflow/
# =============================================================================

ARG AIRFLOW_VERSION=2.9.3
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

# Switch to root to install system packages
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    # Required for psycopg2 / PostgreSQL client
    libpq-dev \
    gcc \
    # Required for Kafka C library
    librdkafka-dev \
    # Utilities
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for pip installs
USER airflow

# Copy and install Python dependencies
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt

# Set PYTHONPATH so `src` module is importable in DAGs
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"

# Default command (overridden by docker-compose)
CMD ["airflow", "webserver"]
