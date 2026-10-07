#!/bin/sh
# =============================================================================
# Kafka Topic Initialization Script
# =============================================================================
# Creates required Kafka topics using kafka-topics.sh.
# Runs via the apache/kafka Docker image (same as the broker).
#
# Topics:
#   clickstream_events   — Raw clickstream events from the producer
#   clickstream_dlq      — Dead Letter Queue for malformed events
# =============================================================================

set -e

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TOPIC="${CLICKSTREAM_TOPIC:-clickstream_events}"
DLQ_TOPIC="${TOPIC}_dlq"
PARTITIONS="${CLICKSTREAM_PARTITIONS:-3}"
REPLICATION="${CLICKSTREAM_REPLICATION_FACTOR:-1}"
RETENTION_MS="${CLICKSTREAM_RETENTION_MS:-604800000}"   # 7 days

KAFKA_TOPICS_CMD="/opt/kafka/bin/kafka-topics.sh"

echo "=== Kafka Init: Connecting to ${BOOTSTRAP} ==="

# Wait for Kafka to be ready
MAX_ATTEMPTS=30
ATTEMPT=0
until ${KAFKA_TOPICS_CMD} --bootstrap-server "${BOOTSTRAP}" --list > /dev/null 2>&1; do
    ATTEMPT=$((ATTEMPT + 1))
    if [ "${ATTEMPT}" -ge "${MAX_ATTEMPTS}" ]; then
        echo "ERROR: Kafka not reachable after ${MAX_ATTEMPTS} attempts. Aborting."
        exit 1
    fi
    echo "Waiting for Kafka... attempt ${ATTEMPT}/${MAX_ATTEMPTS}"
    sleep 3
done

echo "--- Connected to Kafka at ${BOOTSTRAP} ---"

# ---------------------------------------------------------------------------
# Helper function: create topic if not exists
# ---------------------------------------------------------------------------
create_topic_if_not_exists() {
    TOPIC_NAME="$1"
    NUM_PARTITIONS="$2"
    REPL_FACTOR="$3"
    RETENTION="$4"

    if ${KAFKA_TOPICS_CMD} --bootstrap-server "${BOOTSTRAP}" --list | grep -qx "${TOPIC_NAME}"; then
        echo "Topic '${TOPIC_NAME}' already exists — skipping."
    else
        ${KAFKA_TOPICS_CMD} \
            --bootstrap-server "${BOOTSTRAP}" \
            --create \
            --topic "${TOPIC_NAME}" \
            --partitions "${NUM_PARTITIONS}" \
            --replication-factor "${REPL_FACTOR}" \
            --config retention.ms="${RETENTION}" \
            --config cleanup.policy=delete \
            --config compression.type=snappy
        echo "Topic '${TOPIC_NAME}' created (partitions=${NUM_PARTITIONS}, retention=${RETENTION}ms)."
    fi
}

# ---------------------------------------------------------------------------
# Create topics
# ---------------------------------------------------------------------------

# Main clickstream topic — 3 partitions for parallelism
create_topic_if_not_exists \
    "${TOPIC}" \
    "${PARTITIONS}" \
    "${REPLICATION}" \
    "${RETENTION_MS}"

# Dead Letter Queue — 1 partition (low volume)
create_topic_if_not_exists \
    "${DLQ_TOPIC}" \
    "1" \
    "${REPLICATION}" \
    "${RETENTION_MS}"

# ---------------------------------------------------------------------------
# Display topic summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Kafka Init: Complete ==="
echo "Topics:"
${KAFKA_TOPICS_CMD} --bootstrap-server "${BOOTSTRAP}" --list | while read -r t; do
    echo "  - ${t}"
done

echo ""
echo "Topic details for '${TOPIC}':"
${KAFKA_TOPICS_CMD} \
    --bootstrap-server "${BOOTSTRAP}" \
    --describe \
    --topic "${TOPIC}"
