#!/bin/sh
# =============================================================================
# Object Storage Initialization Script
# =============================================================================
# Creates the lakehouse bucket with bronze/silver/gold prefixes.
# Runs via amazon/aws-cli container against SeaweedFS S3 endpoint.
#
# Bucket structure:
#   lakehouse/
#   ├── bronze/          ← Raw ingested data (Parquet/CSV, immutable)
#   │   ├── orders/
#   │   ├── users/
#   │   ├── order_items/
#   │   ├── products/
#   │   ├── inventory_items/
#   │   └── distribution_centers/
#   ├── silver/          ← Cleaned, validated data
#   │   └── clickstream/
#   ├── gold/            ← Aggregated analytics-ready data
#   └── checkpoints/     ← Spark Structured Streaming checkpoints
# =============================================================================

set -e

ENDPOINT="http://${MINIO_HOST:-seaweedfs}:${MINIO_PORT:-9000}"
BUCKET="${LAKEHOUSE_BUCKET:-lakehouse}"

echo "=== Storage Init: Connecting to ${ENDPOINT} ==="

# AWS CLI flags for SeaweedFS (path-style, no SSL verification needed)
AWS_OPTS="--endpoint-url=${ENDPOINT} --no-cli-pager"

# ---------------------------------------------------------------------------
# Wait for S3 endpoint to be ready
# ---------------------------------------------------------------------------
MAX_ATTEMPTS=30
ATTEMPT=0
until aws s3 ls ${AWS_OPTS} > /dev/null 2>&1; do
    ATTEMPT=$((ATTEMPT + 1))
    if [ "${ATTEMPT}" -ge "${MAX_ATTEMPTS}" ]; then
        echo "ERROR: S3 endpoint not reachable after ${MAX_ATTEMPTS} attempts. Aborting."
        exit 1
    fi
    echo "Waiting for S3 endpoint... attempt ${ATTEMPT}/${MAX_ATTEMPTS}"
    sleep 2
done

echo "--- Connected to S3 endpoint at ${ENDPOINT} ---"

# ---------------------------------------------------------------------------
# Create main bucket (idempotent)
# ---------------------------------------------------------------------------
if aws s3 ls ${AWS_OPTS} "s3://${BUCKET}" > /dev/null 2>&1; then
    echo "Bucket '${BUCKET}' already exists — skipping creation."
else
    aws s3 mb ${AWS_OPTS} "s3://${BUCKET}"
    echo "Bucket '${BUCKET}' created."
fi

# ---------------------------------------------------------------------------
# Create folder structure via empty .keep files
# ---------------------------------------------------------------------------
PREFIXES="
bronze/orders
bronze/users
bronze/order_items
bronze/products
bronze/inventory_items
bronze/distribution_centers
silver/clickstream
gold/dim_users
gold/dim_products
gold/dim_distribution_centers
gold/fct_orders
gold/fct_clickstream_sessions
gold/kpi_hourly_gmv
gold/kpi_cart_abandonment
gold/kpi_conversion_funnel
checkpoints/streaming
"

for PREFIX in ${PREFIXES}; do
    echo "" | aws s3 cp ${AWS_OPTS} - "s3://${BUCKET}/${PREFIX}/.keep" > /dev/null 2>&1 || true
    echo "  Created prefix: s3://${BUCKET}/${PREFIX}/"
done

# ---------------------------------------------------------------------------
# Display bucket summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Storage Init: Complete ==="
echo "Bucket: s3://${BUCKET}"
aws s3 ls ${AWS_OPTS} --recursive "s3://${BUCKET}" | head -20 || true
echo ""
echo "  S3 endpoint: ${ENDPOINT}"
echo "  Access Key:  ${AWS_ACCESS_KEY_ID}"
