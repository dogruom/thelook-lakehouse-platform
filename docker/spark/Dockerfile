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
#   JARs          pinned and SHA-1 verified against Maven Central (see fetch calls below)
#   conf          NOT baked in: docker-compose mounts docker/spark/spark-defaults.conf read-only.
#                 No credentials anywhere in the image; S3A reads AWS_* env vars at runtime.
#
# Build (owner approval needed, ~2-3 GB download on first build):
#   docker compose --profile core build spark-master
# =============================================================================

FROM python:3.11-slim-bookworm

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
#     Usage: fetch <maven path> <file> <sha1>
RUN set -eux; \
    cd /opt/spark/jars; \
    M=https://repo1.maven.org/maven2; \
    fetch() { curl -fsSL --retry 3 -o "$2" "$M/$1/$2"; echo "$3  $2" | sha1sum -c -; }; \
    fetch org/apache/hadoop/hadoop-aws/3.3.4 hadoop-aws-3.3.4.jar a65839fbf1869f81a1632e09f415e586922e4f80; \
    fetch com/amazonaws/aws-java-sdk-bundle/1.12.262 aws-java-sdk-bundle-1.12.262.jar 02deec3a0ad83d13d032b1812421b23d7a961eea; \
    fetch org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9 spark-sql-kafka-0-10_2.12-3.5.9.jar 17ef44bb094642e6cbe4c31283b49b0e524225a3; \
    fetch org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9 spark-token-provider-kafka-0-10_2.12-3.5.9.jar a925bf703b98506dd130cb56584eb2e8bd325380; \
    fetch org/apache/kafka/kafka-clients/3.4.1 kafka-clients-3.4.1.jar a2f55251deb3d31a21903a0cfbd4d1fa6b7463ca; \
    fetch org/apache/commons/commons-pool2/2.11.1 commons-pool2-2.11.1.jar 8970fd110c965f285ed4c6e40be7630c62db6f68; \
    fetch io/delta/delta-spark_2.12/3.3.3 delta-spark_2.12-3.3.3.jar 8083a378e42f6d04c5a78cb6e8566f59f4e9ed1c; \
    fetch io/delta/delta-storage/3.3.3 delta-storage-3.3.3.jar a5bf8cebb3a669f83b14a8f4a707545ed861303d; \
    fetch org/postgresql/postgresql/42.7.4 postgresql-42.7.4.jar 264310fd7b2cd76738787dc0b9f7ea2e3b11adc1

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
