#!/bin/sh
# =============================================================================
# Kafka topic initialisation (idempotent)
# =============================================================================
# Runs in the apache/kafka image (same as the broker) via the kafka-init compose service.
#
# Topics (Charter v8):
#   customer_cdc         CDC events; key = {brand_id}:{customer_id}
#   clickstream_events   click events; key = {brand_id}:{session_id}
# There is deliberately NO Kafka dead-letter topic: contract failures go to the Delta DLQ
# (dead_letter.dlq_*) with a _bronze_ref, parse failures stay in Bronze.
# =============================================================================

set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
CDC_TOPIC="${CDC_TOPIC:-customer_cdc}"
CLICKSTREAM_TOPIC="${CLICKSTREAM_TOPIC:-clickstream_events}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
RETENTION_MS="${TOPIC_RETENTION_MS:-604800000}"   # 7 days
REPLICATION=1                                     # single local broker

KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

echo "=== Kafka init: connecting to ${BOOTSTRAP} ==="

MAX_ATTEMPTS=30
ATTEMPT=0
until "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --list > /dev/null 2>&1; do
    ATTEMPT=$((ATTEMPT + 1))
    if [ "${ATTEMPT}" -ge "${MAX_ATTEMPTS}" ]; then
        echo "ERROR: Kafka not reachable after ${MAX_ATTEMPTS} attempts." >&2
        exit 1
    fi
    echo "Waiting for Kafka... attempt ${ATTEMPT}/${MAX_ATTEMPTS}"
    sleep 3
done

for TOPIC in "${CDC_TOPIC}" "${CLICKSTREAM_TOPIC}"; do
    # --if-not-exists makes re-runs a no-op; an existing topic keeps its configuration.
    "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" \
        --create --if-not-exists \
        --topic "${TOPIC}" \
        --partitions "${PARTITIONS}" \
        --replication-factor "${REPLICATION}" \
        --config retention.ms="${RETENTION_MS}" \
        --config cleanup.policy=delete \
        --config compression.type=snappy
done

# Fail loudly if a topic is still missing (the create above would already have exited, this is
# a belt-and-braces check on the broker's actual state).
TOPICS="$("${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --list)"
for TOPIC in "${CDC_TOPIC}" "${CLICKSTREAM_TOPIC}"; do
    if ! printf '%s\n' "${TOPICS}" | grep -qx "${TOPIC}"; then
        echo "ERROR: topic '${TOPIC}' missing after init." >&2
        exit 1
    fi
done

echo "=== Kafka init: complete ==="
printf '%s\n' "${TOPICS}" | sed 's/^/  - /'
