# =============================================================================
# Airflow 3 image (W4.D22.1, ADR-0010, D-098)
# =============================================================================
# Base: apache/airflow:3.0.6-python3.11  (Python 3.11 = runtime parity with the Spark image).
# UNVERIFIED until the owner's first build: the tag was chosen from memory of the Airflow 3.0.x
# release line (3.0.6 is more than two weeks old) and has not been pulled or built here.
#
# Adds
#   Java 17 (headless JRE) + the Spark 3.5.9 distribution COPIED FROM THE CLUSTER IMAGE
#       (crm/spark:3.5.9, build it first: `docker compose --profile core build spark-master`).
#       This is the Spark client for `spark-submit --deploy-mode client`; copying it instead of
#       `pip install pyspark==3.5.x` guarantees the client equals the cluster (3.5.9) and brings the
#       pinned connector JARs (Delta, S3A/hadoop-aws, Kafka, PostgreSQL). Whether pyspark 3.5.9 is
#       published on PyPI was not checked, and PyPI wheels carry none of those JARs anyway.
#   delta-spark 3.3.3 (--no-deps, Python API only; same as the Spark image) + the few runtime
#       dependencies of the `lakehouse` package. pydantic / structlog / boto3 come from Airflow's
#       own constrained set.
#   dbt in a SEPARATE virtualenv (/opt/dbt-venv) from requirements/dbt.txt, so dbt's dependency tree
#       never constrains Airflow's (same reason as .venv-dbt on the host, D-060).
#   The `lakehouse` package is NOT installed: docker-compose bind-mounts ./src to /opt/airflow/src
#       and PYTHONPATH points there (the DAG imports lakehouse.orchestration; Spark jobs ship the
#       code as a content-addressed zip with --py-files).
#
# Build (owner approval needed):   docker compose --profile orchestration build airflow-init
# `requirements` is an additional build context (compose `additional_contexts`).
# =============================================================================

ARG SPARK_IMAGE=crm/spark:3.5.9
ARG SPARK_VERSION=3.5.9
ARG AIRFLOW_VERSION=3.0.6
ARG PYTHON_VERSION=3.11

FROM ${SPARK_IMAGE} AS spark

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG SPARK_VERSION
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER root

# Java 17 for the Spark client; curl and procps for health checks and Spark's launcher scripts.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Spark client = the cluster's distribution (JARs included). Config comes from the bind-mounted
# spark-defaults.conf (docker-compose), no credentials in the image.
COPY --from=spark /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark-${SPARK_VERSION}-bin-hadoop3
RUN ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=/usr/local/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3 \
    PYTHONPATH=/opt/airflow/src

# dbt venv (own dependency tree). requirements/dbt.txt comes from the `requirements` context.
COPY --from=requirements dbt.txt /tmp/dbt.txt
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt.txt \
    && rm -f /tmp/dbt.txt \
    && chown -R airflow:0 /opt/dbt-venv

USER airflow

# Delta Python API (no deps: PySpark itself is the one from /opt/spark), then the rest under the
# official Airflow constraints so the Airflow install is not disturbed.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir --no-deps delta-spark==3.3.3 \
    && pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Command, healthchecks and the Airflow 3 service split (api-server, scheduler, dag-processor)
# are set per service in docker-compose.yml.
