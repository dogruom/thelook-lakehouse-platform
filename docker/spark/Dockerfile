# =============================================================================
# Custom Spark Image
# =============================================================================
# Base: apache/spark:3.5.1-python3
# Adds: S3A (Hadoop-AWS) JAR, Kafka connector JAR, Delta Lake JAR, PySpark deps
#
# JAR versions MUST match:
#   - Hadoop version bundled with Spark 3.5.1 → Hadoop 3.3.4
#   - Kafka connector version → 3.5.x compatible
#   - Delta Lake → 3.x for Spark 3.5
#
# Build: docker build -t thelook/spark:3.5.1 docker/spark/
# =============================================================================

ARG SPARK_VERSION=3.5.1
FROM apache/spark:${SPARK_VERSION}-python3

USER root

# --- System dependencies ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# --- Download JARs into Spark's jars directory ---
ENV SPARK_JARS_DIR=/opt/spark/jars

# Hadoop AWS / S3A — enables s3a:// protocol (Spark ↔ MinIO)
# Must match Hadoop version bundled with Spark 3.5.1 = hadoop-3.3.4
RUN wget -q -O ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" && \
    wget -q -O ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

# Spark-Kafka connector — enables Kafka as Structured Streaming source/sink
# scala version: 2.12 (matches Spark 3.5.1 default)
RUN wget -q -O ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar" && \
    wget -q -O ${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" && \
    wget -q -O ${SPARK_JARS_DIR}/kafka-clients-3.4.1.jar \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" && \
    wget -q -O ${SPARK_JARS_DIR}/commons-pool2-2.11.1.jar \
    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"

# Delta Lake — enables ACID transactions over Parquet (optional, used in Gold layer)
RUN wget -q -O ${SPARK_JARS_DIR}/delta-spark_2.12-3.2.0.jar \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar" && \
    wget -q -O ${SPARK_JARS_DIR}/delta-storage-3.2.0.jar \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar"

# --- Python dependencies for PySpark ---
COPY requirements-spark.txt /requirements-spark.txt
RUN pip install --no-cache-dir -r /requirements-spark.txt

# --- Default Spark configuration ---
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Ensure spark-apps directory exists
RUN mkdir -p /opt/spark-apps && chmod 777 /opt/spark-apps

USER spark
