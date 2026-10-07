#!/bin/bash
# =============================================================================
# PostgreSQL Initialization Script
# =============================================================================
# Creates two databases with dedicated users:
#   1. airflow_metadata  — Airflow metadata / task state storage
#   2. thelook_dwh       — TheLook Data Warehouse (dbt target)
#
# This script runs automatically as part of PostgreSQL's
# docker-entrypoint-initdb.d/ mechanism on FIRST startup.
# It runs as the POSTGRES_USER (superuser).
# =============================================================================

set -e

echo "=== PostgreSQL Init: Starting ==="

# ---------------------------------------------------------------------------
# Read environment variables with defaults
# ---------------------------------------------------------------------------
AIRFLOW_DB="${AIRFLOW_DB_NAME:-airflow_metadata}"
AIRFLOW_USER="${AIRFLOW_DB_USER:-airflow}"
AIRFLOW_PASS="${AIRFLOW_DB_PASSWORD:-airflow_secret_2024}"

DWH_DB="${DWH_DB_NAME:-thelook_dwh}"
DWH_USER="${DWH_DB_USER:-thelook_dwh}"
DWH_PASS="${DWH_DB_PASSWORD:-dwh_secret_2024}"

# ---------------------------------------------------------------------------
# Create Airflow database + user
# ---------------------------------------------------------------------------
echo "--- Creating Airflow database: ${AIRFLOW_DB} ---"
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB:-postgres}" <<-EOSQL
    -- Create user if not exists
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${AIRFLOW_USER}') THEN
            CREATE ROLE "${AIRFLOW_USER}" WITH LOGIN PASSWORD '${AIRFLOW_PASS}';
        END IF;
    END
    \$\$;

    -- Create database if not exists
    SELECT 'CREATE DATABASE "${AIRFLOW_DB}" OWNER "${AIRFLOW_USER}"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${AIRFLOW_DB}')\gexec

    -- Grant all privileges
    GRANT ALL PRIVILEGES ON DATABASE "${AIRFLOW_DB}" TO "${AIRFLOW_USER}";
EOSQL

# Connect to airflow DB and set schema permissions
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${AIRFLOW_DB}" <<-EOSQL
    GRANT ALL ON SCHEMA public TO "${AIRFLOW_USER}";
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "${AIRFLOW_USER}";
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "${AIRFLOW_USER}";
EOSQL

echo "--- Airflow database created successfully ---"

# ---------------------------------------------------------------------------
# Create DWH database + user
# ---------------------------------------------------------------------------
echo "--- Creating DWH database: ${DWH_DB} ---"
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB:-postgres}" <<-EOSQL
    -- Create user if not exists
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${DWH_USER}') THEN
            CREATE ROLE "${DWH_USER}" WITH LOGIN PASSWORD '${DWH_PASS}';
        END IF;
    END
    \$\$;

    -- Create database if not exists
    SELECT 'CREATE DATABASE "${DWH_DB}" OWNER "${DWH_USER}"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${DWH_DB}')\gexec

    -- Grant all privileges
    GRANT ALL PRIVILEGES ON DATABASE "${DWH_DB}" TO "${DWH_USER}";
EOSQL

# Connect to DWH DB and create schemas
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${DWH_DB}" <<-EOSQL
    -- dbt schemas: staging, intermediate, marts
    CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION "${DWH_USER}";
    CREATE SCHEMA IF NOT EXISTS intermediate AUTHORIZATION "${DWH_USER}";
    CREATE SCHEMA IF NOT EXISTS marts AUTHORIZATION "${DWH_USER}";
    CREATE SCHEMA IF NOT EXISTS kpis AUTHORIZATION "${DWH_USER}";
    CREATE SCHEMA IF NOT EXISTS snapshots AUTHORIZATION "${DWH_USER}";

    -- Grant usage on all schemas
    GRANT ALL ON SCHEMA public TO "${DWH_USER}";
    GRANT ALL ON SCHEMA staging TO "${DWH_USER}";
    GRANT ALL ON SCHEMA intermediate TO "${DWH_USER}";
    GRANT ALL ON SCHEMA marts TO "${DWH_USER}";
    GRANT ALL ON SCHEMA kpis TO "${DWH_USER}";
    GRANT ALL ON SCHEMA snapshots TO "${DWH_USER}";

    -- Default privileges for future objects
    ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO "${DWH_USER}";
    ALTER DEFAULT PRIVILEGES IN SCHEMA intermediate GRANT ALL ON TABLES TO "${DWH_USER}";
    ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT ALL ON TABLES TO "${DWH_USER}";
    ALTER DEFAULT PRIVILEGES IN SCHEMA kpis GRANT ALL ON TABLES TO "${DWH_USER}";
    ALTER DEFAULT PRIVILEGES IN SCHEMA snapshots GRANT ALL ON TABLES TO "${DWH_USER}";
EOSQL

echo "--- DWH database + schemas created successfully ---"
echo "=== PostgreSQL Init: Complete ==="
