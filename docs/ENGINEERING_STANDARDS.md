# Engineering Standards & Production Guidelines

> **Document Version**: 6.0  
> **Last Updated**: 2026-10-20  
> **Target Audience**: Senior Data Engineers, AI Pair-Programmers & Code Reviewers  
> **Enforcement**: Automated via pre-commit hooks and GitHub Actions CI

---

## 1. Python Code Standards

### 1.1 Python Runtime & Tooling
- **Runtime**: Python 3.11+ strictly pinned in `pyproject.toml`.
- **Linter & Formatter**: `ruff` (line-length: 100, rules: `E`, `F`, `I`, `N`, `W`, `UP`, `B`, `SIM`, `PTH`, `RUF`).
- **Static Typing**: `mypy` with explicit type hints on all public function parameters and return types.
- **Logging**: `structlog` for structured JSON logging. **Never use raw `print()` statements in production code.**

### 1.2 Data Modeling with Pydantic v2
- All producer models and contract specifications must subclass `pydantic.BaseModel`.
- Use `Field(..., description=...)` with mathematical and string constraints (`ge`, `le`, `pattern`).
- Cross-field consistency must be implemented via `@model_validator(mode="after")`.

### 1.3 Testing Standards (Pytest)
- **Minimum Code Coverage**: **80%** enforced in CI via `pytest-cov`.
- Unit tests (`tests/unit/`) must execute fully offline without network or external services.
- S3 interactions must be mocked using `moto[s3]`.
- Integration tests (`tests/integration/`) validate end-to-end containerized flows.

---

## 2. Apache Spark & Delta Lake Standards

### 2.1 Spark Session Configuration
Pipelines must initialize Delta Lake catalog extensions and S3A credentials using standard environment injection:

```python
from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://seaweedfs:9000")
        .config("spark.hadoop.fs.s3a.access.key", "lakehouse_admin")
        .config("spark.hadoop.fs.s3a.secret.key", "lakehouse_secret_dev")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
```

### 2.2 Delta Lake Ingestion (Bronze Layer)
- **Append-Only Invariant**: Bronze tables are strictly immutable append-only logs.
- Ingestion must enrich records with metadata columns:
  - `_ingest_ts`: Current timestamp of pipeline execution.
  - `_source_file`: Input filename or Kafka topic offset.
  - `_batch_id`: Ingestion run identifier.
  - `_brand_id`: Extracted tenant identifier.
- Schema drift must be handled automatically using `.option("mergeSchema", "true")`.

### 2.3 Delta Lake Reconciliation (Silver Layer)
- Deduplication and upserts must be executed using deterministic `MERGE INTO` expressions.
- **Never perform unbounded full-table scans during merge operations.** Use partition predicates in merge conditions:

```python
from delta.tables import DeltaTable

silver_table = DeltaTable.forPath(spark, "s3a://lakehouse/silver/customers")

silver_table.alias("target").merge(
    source_df.alias("source"),
    "target.brand_id = source.brand_id AND target.customer_id = source.customer_id"
).whenMatchedUpdateAll(
    condition="source._cdc_timestamp > target._cdc_timestamp"
).whenNotMatchedInsertAll(
).execute()
```

### 2.4 Performance Optimization (Z-Ordering & Compaction)
- High-throughput tables must execute periodic compaction and multi-dimensional clustering:
```python
DeltaTable.forPath(spark, "s3a://lakehouse/silver/orders").optimize().executeZOrderBy("brand_id", "order_timestamp")
DeltaTable.forPath(spark, "s3a://lakehouse/silver/clickstream").optimize().executeZOrderBy("brand_id", "event_timestamp")
```
- Data skew mitigation: Large-to-small dimension lookups must employ `broadcast(dim_df)` joins.

---

## 3. Analytics Engineering (dbt Core) Standards

### 3.1 Model Directory Architecture
```
dbt_project/
├── dbt_project.yml
├── profiles.yml
├── snapshots/                   # SCD Type 2 tracking (customers)
│   └── snap_customers.sql
├── models/
│   ├── staging/                 # 1:1 view wrappers over Silver Delta tables
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   └── stg_clickstream.sql
│   ├── intermediate/            # Business aggregations & join logic
│   │   ├── int_customer_orders.sql
│   │   └── int_customer_engagement.sql
│   └── marts/                   # Gold analytical marts & OBT
│       ├── dim_customers.sql
│       ├── fct_orders.sql
│       ├── obt_customer_360.sql
│       └── agg_rfm_segments.sql
└── tests/                       # Singular business assertions
    └── assert_positive_revenue.sql
```

### 3.2 Model Conventions & Contracts
- **Schema Contracts**: Analytical Gold models must enforce explicit YAML contracts:
```yaml
models:
  - name: dim_customers
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: string
        tests:
          - not_null
          - unique
      - name: email_hash
        data_type: string
        tests:
          - not_null
```
- **GDPR Compliance**: Customer emails must be hashed using SHA-256 (`sha2(lower(trim(email)), 256)`). Unmasked customer emails are prohibited from entering Gold tables.

---

## 4. Orchestration (Apache Airflow) Standards

### 4.1 DAG Design Principles
- **Idempotency**: Every task must be safely re-runnable without manual intervention or data corruption.
- **Dynamic Multi-Tenant Generation**: Instead of monolithic pipelines, iterate through active tenant configurations to spawn brand-isolated tasks.
- **Explicit Constraints**:
  - `retries`: 2
  - `retry_delay`: `timedelta(minutes=5)`
  - `execution_timeout`: `timedelta(minutes=30)`
  - `max_active_runs`: 1 (prevents concurrent race conditions)

---

## 5. Infrastructure as Code (Terraform) Standards

- AWS resources must be modularized under `infra/terraform/`:
  - `s3_buckets.tf`: Storage configuration with lifecycle policies and SSE-S3 encryption.
  - `iam.tf`: Least-privilege IAM execution roles for Databricks compute and ingestion.
  - `variables.tf`: Parameterized environment settings.
- All code must pass `terraform fmt -check` and `tflint`.

---

## 6. Git & CI/CD Conventions

### 6.1 Conventional Commits
All commit messages must adhere to the Conventional Commits format:
- `feat(generator)`: Add multi-tenant CDC customer emitter
- `feat(bronze)`: Implement Spark Structured Streaming Kafka ingestion
- `fix(silver)`: Correct merge condition on late-arriving clickstream
- `perf(spark)`: Add Z-Order clustering on orders table
- `docs(readme)`: Update architectural diagrams and benchmark metrics

### 6.2 Pre-Push & PR Checklist
Before submitting a Pull Request, ensure:
1. `ruff check . && ruff format --check .` passes without errors.
2. `pytest tests/unit/ -v --cov=src` reports ≥ 80% coverage.
3. Docker Compose initializes cleanly and `python scripts/smoke_test.py` completes 15/15 checks.