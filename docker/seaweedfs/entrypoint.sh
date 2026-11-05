#!/bin/sh
# =============================================================================
# SeaweedFS entrypoint: render the S3 identities from the environment, then start the server.
# The credentials never exist in a tracked file; the rendered file lives only in the container's
# /tmp (mode 600). Requires S3_ACCESS_KEY and S3_SECRET_KEY (compose passes them via ${VAR:?}).
# =============================================================================
set -eu

TEMPLATE=/config/s3.json.template
RENDERED=/tmp/s3.json

: "${S3_ACCESS_KEY:?S3_ACCESS_KEY is not set (run scripts/bootstrap_env.py)}"
: "${S3_SECRET_KEY:?S3_SECRET_KEY is not set (run scripts/bootstrap_env.py)}"

# Values are substituted into JSON with sed: restrict them to a safe alphabet so that quotes,
# backslashes, '&' or the sed delimiter can never alter the document.
for name in S3_ACCESS_KEY S3_SECRET_KEY; do
    eval "value=\${$name}"
    case "${value}" in
        *[!A-Za-z0-9_-]* | "")
            echo "ERROR: ${name} may contain only letters, digits, '_' and '-'." >&2
            exit 1
            ;;
    esac
done

umask 077
sed -e "s|@S3_ACCESS_KEY@|${S3_ACCESS_KEY}|" \
    -e "s|@S3_SECRET_KEY@|${S3_SECRET_KEY}|" \
    "${TEMPLATE}" > "${RENDERED}"

# All-in-one server: master + volume + filer + S3 gateway.
exec weed server \
    -dir=/data \
    -master.port=9333 \
    -master.defaultReplication=000 \
    -volume.port=8080 \
    -filer \
    -filer.port=9001 \
    -s3 \
    -s3.port=9000 \
    -s3.config="${RENDERED}" \
    -s3.allowEmptyFolder=true
