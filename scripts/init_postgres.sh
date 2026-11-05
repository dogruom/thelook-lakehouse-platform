#!/bin/bash
# =============================================================================
# PostgreSQL initialisation (ADR-0007)
# =============================================================================
# Creates the only two databases the platform needs, each with its own login role:
#   airflow_metadata   Airflow metadata / task state
#   hive_metastore     Hive metastore for Spark (table catalog; AWS uses the Glue Catalog)
#
# Runs through docker-entrypoint-initdb.d on the FIRST start of an empty data volume, as the
# POSTGRES_USER superuser. Designed to be idempotent (roles and databases are created only if
# missing; a manual re-run is untested). Passwords have no defaults, are handed to psql through
# the environment (\getenv) and quoted by format(%L), so they do not appear in the SQL text or
# on command lines (server statement logging, e.g. log_statement=all, would still record them).
# A reused data volume skips this script entirely: use a fresh crm_* volume after rotation.
#
# The official entrypoint may `source` this file, so strict mode is confined to a subshell.
# =============================================================================

# create_db <database> <owner role> <password>
create_db() {
    local db="$1" role="$2"
    echo "--- ${db} (owner: ${role}) ---"
    INIT_PW="$3" psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${ROOT_DB}" \
        -v db="${db}" -v role="${role}" <<'EOSQL'
\getenv pw INIT_PW
SELECT format('CREATE ROLE %I WITH LOGIN', :'role')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'role') \gexec
SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'role', :'pw') \gexec
SELECT format('CREATE DATABASE %I OWNER %I ENCODING ''UTF8'' TEMPLATE template0', :'db', :'role')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'db') \gexec
SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', :'db', :'role') \gexec
EOSQL
    psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${db}" \
        -v role="${role}" <<'EOSQL'
SELECT format('GRANT ALL ON SCHEMA public TO %I', :'role') \gexec
EOSQL
}

init_main() {
    : "${POSTGRES_USER:?POSTGRES_USER is not set}"
    ROOT_DB="${POSTGRES_DB:-postgres}"

    local airflow_db="${AIRFLOW_DB_NAME:-airflow_metadata}"
    local airflow_user="${AIRFLOW_DB_USER:-airflow}"
    : "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD is not set}"

    local hive_db="${HIVE_METASTORE_DB_NAME:-hive_metastore}"
    local hive_user="${HIVE_METASTORE_DB_USER:-hive}"
    : "${HIVE_METASTORE_DB_PASSWORD:?HIVE_METASTORE_DB_PASSWORD is not set}"

    echo "=== PostgreSQL init: start ==="
    create_db "${airflow_db}" "${airflow_user}" "${AIRFLOW_DB_PASSWORD}"
    create_db "${hive_db}" "${hive_user}" "${HIVE_METASTORE_DB_PASSWORD}"
    echo "=== PostgreSQL init: complete ==="
}

( set -euo pipefail; init_main )
