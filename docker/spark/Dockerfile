# =============================================================================
# Spark image (ADR-0007): Python 3.11 + Java 17 + Spark 3.5.9 + Delta 3.3.3
# =============================================================================
# Why not apache/spark:*-python3: that image ships Python 3.8, while every runtime of this
# project (host venv, Airflow, Glue 5.0, CI) is Python 3.11. Driver and executors here run the
# same interpreter, so Pydantic contracts behave identically on both sides.
#
# Layout
#   base          python:3.11-slim-bookworm (Python 3.11)  +  openjdk-17-jre-headless
#   /opt/spark    Apache binary distribution spark-3.5.9-bin-hadoop3 (Hadoop 3.3.4, Scala 2.12)
#   JARs          pinned by SHA-256 (trust chain: Maven Central .sha1 -> SHA-256, see fetch below)
#   conf          NOT baked in: docker-compose mounts docker/spark/spark-defaults.conf read-only.
#                 No credentials anywhere in the image; S3A reads AWS_* env vars at runtime.
#
# Build (owner approval needed, ~2-3 GB download on first build):
#   docker compose --profile core build spark-master
# =============================================================================

# Base pinned by multi-arch index digest (resolved 2026-11-10 with
# `docker buildx imagetools inspect python:3.11-slim-bookworm`); the tag stays for readability.
FROM python:3.11-slim-bookworm@sha256:a83ba18aef0f865b1a4a611c2cf460fe446f2795673bbb9fe43be570439e3b39

ARG SPARK_VERSION=3.5.9
# SHA-512 of spark-3.5.9-bin-hadoop3.tgz as published by the Apache Spark project.
ARG SPARK_TGZ_SHA512=66097faf48b12f8673dc13256a44d731c27c86a5831cd8ca930c6fd9dd9bdf31d40593095e959461351d55da1a8e3ad5ed95e0b4b51bea87ca3989ab002ea8ce

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# --- OS packages: Java 17 (Spark runtime), curl (healthchecks, downloads), procps (Spark scripts)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates procps \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:${PATH}

# --- Spark binary distribution (archive.apache.org is permanent; downloads.apache.org keeps
#     only current releases and serves as a fallback). The pinned SHA-512 guards both.
RUN set -eux; \
    f=spark-${SPARK_VERSION}-bin-hadoop3.tgz; \
    curl -fsSL --retry 3 -o /tmp/spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${f}" \
      || curl -fsSL --retry 3 -o /tmp/spark.tgz "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/${f}"; \
    echo "${SPARK_TGZ_SHA512}  /tmp/spark.tgz" | sha512sum -c -; \
    tar -xzf /tmp/spark.tgz -C /opt; \
    ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark; \
    rm -f /tmp/spark.tgz

# --- Connector JARs. hadoop-aws MUST equal the Hadoop version bundled with Spark 3.5.x (3.3.4);
#     aws-java-sdk-bundle 1.12.262 is the version hadoop-aws 3.3.4 was built against.
#     Usage: fetch <maven path> <file> <sha256>. Pins = SHA-256 of the exact bytes that were first
#     verified against Maven Central's published .sha1 (W1.D2 build), computed from that image on
#     2026-11-10 (sha1sum re-checked equal, then sha256sum).
RUN set -eux; \
    cd /opt/spark/jars; \
    M=https://repo1.maven.org/maven2; \
    fetch() { curl -fsSL --retry 3 -o "$2" "$M/$1/$2"; echo "$3  $2" | sha256sum -c -; }; \
    fetch org/apache/hadoop/hadoop-aws/3.3.4 hadoop-aws-3.3.4.jar 53f9ae03c681a30a50aa17524bd9790ab596b28481858e54efd989a826ed3a4a; \
    fetch com/amazonaws/aws-java-sdk-bundle/1.12.262 aws-java-sdk-bundle-1.12.262.jar 873fe7cf495126619997bec21c44de5d992544aea7e632fdc77adb1a0915bae5; \
    fetch org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9 spark-sql-kafka-0-10_2.12-3.5.9.jar fed393dcc3ef70611fe3d4f35ae78a8ea7c55301fd4cdd66c5dc97fcfaa5a00f; \
    fetch org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9 spark-token-provider-kafka-0-10_2.12-3.5.9.jar e82612e6070438c5ea48b08c74f367a6d214a390bdf6dcf3ff316dca592ddda5; \
    fetch org/apache/kafka/kafka-clients/3.4.1 kafka-clients-3.4.1.jar 9d0a9058c8ede79db6e54ae378ed16fe8d1bead2894635a4aa6aa0b7981668c6; \
    fetch org/apache/commons/commons-pool2/2.11.1 commons-pool2-2.11.1.jar ea0505ee7515e58b1ac0e686e4d1a5d9f7d808e251a61bc371aa0595b9963f83; \
    fetch io/delta/delta-spark_2.12/3.3.3 delta-spark_2.12-3.3.3.jar 4912bf902a1c453aba3bd838ff212dedd357155212f9bb37a6b47d97f2827ebc; \
    fetch io/delta/delta-storage/3.3.3 delta-storage-3.3.3.jar 45ad193053fd30cebe6d9ab1889873cde39b223a0f210e00ebee8b6642476901; \
    fetch org/postgresql/postgresql/42.7.4 postgresql-42.7.4.jar 188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e

# --- Python packages. PySpark itself comes from the Spark distribution (same 3.5.9 code on the
#     JVM and Python side), exposed through a .pth file; delta-spark is installed without deps
#     so pip does not pull a second pyspark. Versions follow the ranges in pyproject.toml.
RUN set -eux; \
    pip install delta-spark==3.3.3 --no-deps; \
    pip install importlib-metadata==8.5.0 zipp==3.21.0 \
        pydantic==2.7.1 pydantic-settings==2.3.4 structlog==24.2.0 pytest==8.3.3; \
    sp="$(python -c 'import site; print(site.getsitepackages()[0])')"; \
    printf '%s\n' /opt/spark/python "$(ls /opt/spark/python/lib/py4j-*-src.zip)" /opt/spark-apps/src \
      > "$sp/spark_runtime.pth"

# --- Python parity: driver and executors use the same interpreter (3.11)
ENV PYSPARK_PYTHON=/usr/local/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

# --- Unprivileged runtime user; writable dirs for logs, worker scratch and mounted code
RUN groupadd --gid 185 spark \
    && useradd --uid 185 --gid 185 --home-dir /home/spark --create-home --shell /bin/bash spark \
    && mkdir -p /opt/spark/logs /opt/spark/work /opt/spark-apps/src /workspace \
    && chown -R spark:spark /opt/spark/logs /opt/spark/work /opt/spark-apps /workspace

WORKDIR /opt/spark
USER spark
