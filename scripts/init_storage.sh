#!/bin/sh
# =============================================================================
# Object storage initialisation (idempotent)
# =============================================================================
# Runs in the amazon/aws-cli image via the seaweedfs-init compose service. Credentials come
# from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in the container environment (never printed).
#
# One bucket, layer prefixes per Charter v8:
#   landing/       raw files / generator landing zone  landing/{brand_id}/{entity}/{date}/
#   bronze/        append-only raw events (Delta)
#   silver/        Spark-owned current state and SCD2 (Delta)
#   gold/          dbt-owned marts (Delta)
#   dead_letter/   DLQ tables (Delta)
#   ops/           pipeline metrics, audit and smoke-test scratch
#   checkpoints/   Structured Streaming checkpoints, one directory per job
# A zero-byte ".keep" object makes each prefix visible in listings; Delta tables are created
# below these prefixes by the pipeline jobs.
# =============================================================================

set -eu

ENDPOINT="http://${S3_HOST:-seaweedfs}:${S3_PORT:-9000}"
BUCKET="${LAKEHOUSE_BUCKET:-lakehouse}"
PREFIXES="landing bronze silver gold dead_letter ops checkpoints"

s3() { aws --endpoint-url "${ENDPOINT}" --no-cli-pager "$@"; }

echo "=== Storage init: ${ENDPOINT}, bucket '${BUCKET}' ==="

MAX_ATTEMPTS=30
ATTEMPT=0
until s3 s3api list-buckets > /dev/null 2>&1; do
    ATTEMPT=$((ATTEMPT + 1))
    if [ "${ATTEMPT}" -ge "${MAX_ATTEMPTS}" ]; then
        echo "ERROR: S3 endpoint not reachable (or credentials rejected) after ${MAX_ATTEMPTS} attempts." >&2
        exit 1
    fi
    echo "Waiting for S3 endpoint... attempt ${ATTEMPT}/${MAX_ATTEMPTS}"
    sleep 2
done

if s3 s3api head-bucket --bucket "${BUCKET}" > /dev/null 2>&1; then
    echo "Bucket '${BUCKET}' already exists."
else
    s3 s3api create-bucket --bucket "${BUCKET}" > /dev/null
    echo "Bucket '${BUCKET}' created."
fi

for PREFIX in ${PREFIXES}; do
    # put-object is idempotent: re-running overwrites the empty marker.
    s3 s3api put-object --bucket "${BUCKET}" --key "${PREFIX}/.keep" > /dev/null
    echo "  prefix ready: s3://${BUCKET}/${PREFIX}/"
done

# Verify the end state (fail if anything is missing).
LISTING="$(s3 s3 ls "s3://${BUCKET}/")"
for PREFIX in ${PREFIXES}; do
    if ! printf '%s\n' "${LISTING}" | grep -q "PRE ${PREFIX}/"; then
        echo "ERROR: prefix '${PREFIX}/' missing in s3://${BUCKET}/." >&2
        exit 1
    fi
done

echo "=== Storage init: complete ==="
