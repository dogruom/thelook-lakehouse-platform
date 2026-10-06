# Tech Stack Rules & Coding Standards

> **Version**: 2.0  
> **Last Updated**: 2026-10-06  
> **Purpose**: Enforceable engineering standards for every contributor (human or agentic).

---

## 1. Python Standards

### 1.1 Runtime & Packaging
- **Python**: 3.11+ (pinned in Docker images and `pyproject.toml`)
- **Package Manager**: `pip` with pinned versions in `requirements/*.txt` (split: `base.txt`, `dev.txt`, `airflow.txt`, `spark.txt`)
- **Virtual Environments**: Not used inside containers; local dev uses `venv` or `uv`

### 1.2 Code Style & Linting
- **Formatter/Linter**: `ruff` (replaces black, isort, flake8 — single tool, fast)
  ```toml
  # pyproject.toml
  [tool.ruff]
  target-version = "py311"
  line-length = 120
  
  [tool.ruff.lint]
  select = ["E", "F", "I", "N", "W", "UP", "B", "SIM", "PTH"]
  ```
- **Type Hints**: Mandatory on all function signatures. Use `from __future__ import annotations` for forward refs.
- **Docstrings**: Google-style docstrings on all public functions and classes.

### 1.3 Data Validation
- **Pydantic v2**: For all ingestion schemas and configuration models.
  - Use `model_validator` for cross-field validation.
  - Use `Field(...)` with `description`, `ge`, `le`, `pattern` constraints.
- **Great Expectations**: For data quality suites on DataFrames (batch and streaming output).
  - Expectation suites stored as JSON in `src/quality/expectations/`.
  - Validation results persisted to `docs/gx_reports/`.

### 1.4 Testing
- **Framework**: `pytest` (with `pytest-cov`, `pytest-mock`)
- **Conventions**:
  - Test files: `tests/unit/test_<module>.py`, `tests/integration/test_<flow>.py`
  - Fixtures: `tests/fixtures/` for sample data files, `conftest.py` for shared fixtures.
  - Minimum coverage target: **80%** on `src/` modules (enforced in CI).
- **What to test**:
  - Pydantic schema validation (valid + invalid inputs)
  - Ingestion functions (mocked S3/MinIO calls)
  - Kafka message serialization/deserialization
  - dbt model logic (via `dbt test`)
  - Airflow DAG import and structure validation

---

## 2. Apache Kafka Rules

### 2.1 Deployment
- **Mode**: KRaft (Zookeeper-free) — single broker, single controller
- **Image**: `apache/kafka:3.7.0` (official Apache image, not Confluent)
- **Persistence**: Docker volume for `/var/lib/kafka/data`

### 2.2 Topic Configuration
- **Topic**: `clickstream_events`
  - Partitions: `3` (allows parallel consumption)
  - Replication Factor: `1` (single broker)
  - Retention: `168h` (7 days)
  - Cleanup Policy: `delete`

### 2.3 Producer Rules
- **Serialization**: JSON with UTF-8 encoding
- **Key**: `user_id` (string) — ensures user-level ordering within partition
- **Idempotency**: `enable.idempotence=true` (prevents duplicates on retry)
- **Compression**: `compression.type=snappy`
- **Error Handling**: Failed sends logged with full payload; no silent drops

### 2.4 Consumer Rules (Spark)
- **Offset Management**: Spark checkpoint-based (not Kafka consumer groups)
- **Deserialization**: JSON → StructType with explicit schema
- **No infinite `poll()` loops**: Spark Structured Streaming handles this internally

### 2.5 Anti-Patterns 🚫
- ❌ `print()` in consumer loops (use structured logging)
- ❌ Auto-creating topics in application code (create via Docker entrypoint or admin script)
- ❌ Hardcoded `bootstrap.servers` (use environment variables)
- ❌ `auto.offset.reset=latest` without checkpoint (data loss risk)

---

## 3. Apache Spark (PySpark) Rules

### 3.1 Deployment
- **Image**: `bitnami/spark:3.5` (Master + 1 Worker)
- **Jars**: `spark-sql-kafka`, `hadoop-aws`, `aws-java-sdk-bundle` (for MinIO S3A)

### 3.2 Structured Streaming Mandates
```python
# MANDATORY patterns in every streaming job:

# 1. Explicit schema (never infer from stream)
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    # ... all fields explicitly defined
])

# 2. Watermarking for late-arriving data
.withWatermark("event_timestamp", "10 minutes")

# 3. Checkpoint to persistent storage (MinIO/S3, never local filesystem)
.option("checkpointLocation", "s3a://lakehouse/checkpoints/clickstream/")

# 4. Trigger interval (prevents small file problem)
.trigger(processingTime="30 seconds")

# 5. Output mode appropriate to aggregation
.outputMode("append")  # for non-aggregated
.outputMode("update")  # for windowed aggregations
```

### 3.3 Performance Rules
- **Partition Pruning**: Filter on partition columns before joins
- **Broadcast Joins**: Use for dimension tables < 10MB (`broadcast(df)`)
- **No `collect()`** on large DataFrames (use `take()` or `show()` for debugging)
- **Coalesce before write**: Reduce output file count: `.coalesce(1)` for small batches

### 3.4 S3A (MinIO) Configuration
```python
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
spark.conf.set("spark.hadoop.fs.s3a.access.key", "${MINIO_ACCESS_KEY}")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "${MINIO_SECRET_KEY}")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
```

---

## 4. dbt Rules

### 4.1 Project Structure
```
dbt_project/
├── dbt_project.yml          # Project config
├── profiles.yml             # Connection profiles (gitignored, templated)
├── packages.yml             # dbt packages (dbt-utils, dbt-expectations)
├── models/
│   ├── staging/             # 1:1 source mirrors, renamed, retyped
│   │   ├── _stg__sources.yml
│   │   ├── stg_orders.sql
│   │   └── ...
│   ├── intermediate/        # Business logic, joins, dedup (optional)
│   │   └── int_order_items_enriched.sql
│   ├── marts/
│   │   ├── core/            # Kimball star schema
│   │   │   ├── dim_users.sql
│   │   │   ├── dim_products.sql
│   │   │   └── fct_orders.sql
│   │   ├── analytics/       # Streaming-derived models
│   │   │   └── fct_clickstream_sessions.sql
│   │   └── kpi/             # Pre-aggregated business metrics
│   │       ├── kpi_hourly_gmv.sql
│   │       ├── kpi_cart_abandonment.sql
│   │       └── kpi_conversion_funnel.sql
│   └── _models.yml          # Global model configs
├── macros/                  # Reusable SQL macros
├── seeds/                   # Static lookup data (CSV)
├── snapshots/               # SCD Type 2 snapshots
├── tests/
│   ├── generic/             # Custom generic tests
│   └── singular/            # One-off data assertions
└── analyses/                # Ad-hoc analytical queries
```

### 4.2 Naming Conventions
| Layer | Prefix | Example | Materialization |
|-------|--------|---------|-----------------|
| Staging | `stg_` | `stg_orders` | `view` |
| Intermediate | `int_` | `int_order_items_enriched` | `view` or `ephemeral` |
| Dimensions | `dim_` | `dim_users` | `table` |
| Facts | `fct_` | `fct_orders` | `incremental` |
| KPIs | `kpi_` | `kpi_hourly_gmv` | `incremental` or `table` |

### 4.3 Mandatory Model Properties
```yaml
# Every model MUST have in its YAML:
models:
  - name: fct_orders
    description: "Grain: one row per order line item"
    config:
      contract:
        enforced: true       # Schema enforcement
      tags: ['core', 'daily']
    columns:
      - name: order_id
        data_type: integer
        description: "Primary key"
        tests:
          - unique
          - not_null
```

### 4.4 Incremental Strategy
```sql
-- REQUIRED pattern for incremental models:
{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

SELECT ...
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

### 4.5 SQL Style (enforced by SQLFluff)
- **Keywords**: UPPERCASE (`SELECT`, `FROM`, `WHERE`, `JOIN`)
- **Indentation**: 4 spaces
- **CTEs**: Preferred over subqueries; named descriptively
- **Trailing commas**: Yes (easier diffs)
- **Line length**: Max 120 characters
- **Dialect**: `postgres` (SQLFluff config)

### 4.6 Anti-Patterns 🚫
- ❌ `SELECT *` in staging models (explicitly list all columns)
- ❌ Business logic in staging (staging = rename + retype only)
- ❌ Hardcoded dates or filter values (use `var()` or `env_var()`)
- ❌ Missing `description` on any model or column
- ❌ Models without at least `unique` + `not_null` on primary key

---

## 5. Apache Airflow Rules

### 5.1 Deployment
- **Version**: 2.8+ (official `apache/airflow:2.8.4-python3.11` image)
- **Executor**: `LocalExecutor` (sufficient for our DAG count, avoids Celery overhead)
- **Metadata DB**: PostgreSQL (shared instance, separate database `airflow_metadata`)
- **DAGs folder**: `/opt/airflow/dags` (mounted from `./dags/`)

### 5.2 DAG Design Rules
```python
# REQUIRED patterns:
from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": notify_on_failure,  # Always define
}

@dag(
    dag_id="thelook_batch_pipeline",
    schedule="0 6 * * *",        # Explicit cron, never @daily
    start_date=datetime(2024, 1, 1),
    catchup=False,                # Explicit catchup setting
    max_active_runs=1,            # Prevent parallel execution
    tags=["batch", "thelook", "production"],
    doc_md=__doc__,               # DAG-level documentation
)
```

### 5.3 Idempotency Rules
- All tasks must be safely re-runnable
- Use `INSERT ... ON CONFLICT` or dbt `merge` — never raw `INSERT`
- Use Airflow's `execution_date` / `data_interval_start` for partitioned processing
- Clean up partial outputs before retry (or use atomic writes)

### 5.4 Anti-Patterns 🚫
- ❌ `from airflow.operators.python_operator import PythonOperator` (use `@task` decorator)
- ❌ Heavy computation inside DAG file (import-time execution)
- ❌ `schedule_interval` parameter (deprecated; use `schedule`)
- ❌ `catchup=True` without explicit backfill strategy
- ❌ Secrets in DAG code (use Airflow Variables or Connections)

---

## 6. Docker & Infrastructure Rules

### 6.1 Container Naming
- `thelook-postgres`
- `thelook-minio`
- `thelook-kafka`
- `thelook-spark-master`
- `thelook-spark-worker`
- `thelook-airflow-webserver`
- `thelook-airflow-scheduler`

### 6.2 Network
- Single Docker network: `thelook-network` (bridge mode)
- All services communicate via container names (DNS resolution)

### 6.3 Volume Strategy
- **Named volumes** for persistent data: `postgres_data`, `minio_data`, `kafka_data`
- **Bind mounts** for code: `./dags`, `./src`, `./dbt_project`
- **No anonymous volumes** (makes cleanup unpredictable)

### 6.4 Environment Variables
- All configurable via `.env` file (`.env.example` committed, `.env` gitignored)
- Pattern: `THELOOK_<SERVICE>_<SETTING>` (e.g., `THELOOK_MINIO_ACCESS_KEY`)

### 6.5 Health Checks
- Every service must have a Docker `healthcheck` defined
- Dependent services use `depends_on.condition: service_healthy`

---

## 7. Git & CI/CD Rules

### 7.1 Branch Strategy
- `main` — stable, deployable
- `develop` — integration branch
- `feature/<ticket-or-description>` — feature branches
- `fix/<description>` — bugfix branches
- All changes via Pull Request; no direct push to `main`

### 7.2 Commit Messages
```
<type>(<scope>): <short description>

feat(streaming): add Kafka producer with clickstream schema
fix(dbt): resolve duplicate rows in fct_orders incremental
chore(docker): update Spark image to 3.5.3
docs(readme): add architecture diagram
test(ingestion): add GX expectation suite for orders
```

### 7.3 CI Pipeline (GitHub Actions)
```
On: push/PR to main, develop
Jobs:
  1. lint-python:     ruff check src/ tests/ dags/
  2. lint-sql:        sqlfluff lint dbt_project/models/
  3. test-python:     pytest tests/unit/ --cov=src --cov-fail-under=80
  4. test-dbt:        dbt compile + dbt test (against test DB)
  5. security:        pip-audit (dependency vulnerability scan)
```

### 7.4 .gitignore Essentials
```
.env
*.pyc
__pycache__/
.pytest_cache/
dbt_project/target/
dbt_project/dbt_packages/
dbt_project/logs/
*.egg-info/
.ruff_cache/
minio_data/
postgres_data/
logs/
```

---

## 8. Logging & Observability Standards

### 8.1 Python Logging
```python
import structlog

logger = structlog.get_logger(__name__)

# Always use structured logging
logger.info("batch_ingestion_complete",
    table="orders",
    rows_loaded=15234,
    duration_seconds=12.4,
    target="s3://lakehouse/bronze/orders/"
)
```

### 8.2 What to Log
- ✅ Row counts at each pipeline stage
- ✅ Data quality validation results (pass/fail + details)
- ✅ Execution duration for each step
- ✅ Failed record counts and dead-letter destinations
- ❌ PII (names, emails, IP addresses — redact in logs)
- ❌ Full data payloads (log schemas and counts instead)