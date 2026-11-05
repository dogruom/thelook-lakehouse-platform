# Engineering Standards & Production Guidelines

> **Document Version**: 7.0
> **Last Updated**: 2026-11-05
> **Target Audience**: Data engineers, AI pair-programmers and code reviewers of this repository
> **Enforcement**: *Target* — pre-commit hooks and GitHub Actions CI are planned (Milestone 1 adds the lint/unit CI job). Until then, standards are enforced by review and the checks listed in [CLAUDE.md](../CLAUDE.md).

> [!NOTE]
> **v7.0 changes**: aligned with [PROJECT_CHARTER.md](PROJECT_CHARTER.md) v7 — credentials from configuration only; Bronze metadata names (`_source_ref`, `brand_id`); sequence-ordered CDC MERGE; Spark-owned SCD2 (no dbt snapshots); HMAC pseudonymisation instead of SHA-256 "GDPR compliance"; one parametrised Airflow DAG with dynamic task mapping.

---

## 1. Python Code Standards

### 1.1 Runtime & Tooling
- **Runtime**: Python 3.11+ (`requires-python = ">=3.11"`), project-local virtual environment `.venv/`.
- **Linter & Formatter**: `ruff` (line-length 100; rules configured in `pyproject.toml`).
- **Static Typing**: `mypy` with explicit type hints on public function parameters and return types.
- **Logging**: `structlog` JSON logging via `src/utils/logging.py`, with `brand_id` in the log context where applicable. No `print()` in pipeline code (CLI entry points and `scripts/` may print user-facing output).

### 1.2 Data Modeling with Pydantic v2
- Producer models and contract specifications subclass `pydantic.BaseModel`.
- Use `Field(..., description=...)` with constraints (`ge`, `le`, `pattern`).
- Cross-field consistency via `@model_validator(mode="after")`.
- Generators are deterministic: every generator accepts a seed and uses local RNG instances.

### 1.3 Testing Standards (pytest)
- Correctness and integration tests take priority over coverage numbers. Line coverage ≥ 80% on `src/generator/` is a **hygiene floor**, not a quality claim (Charter §7).
- Unit tests (`tests/unit/`) run fully offline — no network, Docker, Kafka or S3. S3 is mocked with `moto[s3]`.
- Integration tests (`tests/integration/`) run against the Docker Compose stack and cover the 🔺 correctness suites (restart/replay, CDC ordering, tenant isolation including a negative test, DLQ replay).

---

## 2. Apache Spark & Delta Lake Standards

### 2.1 Spark Session Configuration
Endpoints and credentials come from configuration (environment variables / secret store) — never from source code. Local defaults live in `.env.example`; cloud profiles use IAM roles instead of static keys.

```python
import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
    )
    endpoint = os.getenv("S3_ENDPOINT")  # set for SeaweedFS locally; unset on AWS
    if endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
        )
    return builder.getOrCreate()
```

### 2.2 Delta Lake Ingestion (Bronze Layer)
- **Append-only**: normal processing never updates or deletes Bronze. Sole exception: the audited erasure procedure (Charter §5), logged in `ops.erasure_log`.
- Metadata columns on every record: `_ingest_ts`, `_source_ref` (Kafka topic/partition/offset or file path), `_batch_id`, `brand_id`; stream sources also keep `_raw_value`.
- Unparseable records are stored raw with `_parse_error` — never dropped.
- Schema drift handled with `.option("mergeSchema", "true")`; evolutions are listed in [DATA_CONTRACTS.md](DATA_CONTRACTS.md).
- Each streaming query has its own checkpoint location `s3a://lakehouse/checkpoints/<job_name>/`.

### 2.3 Delta Lake Reconciliation (Silver Layer)
- Upserts and dedup use deterministic `MERGE` inside `foreachBatch`; re-running a batch yields the same table state.
- Every merge condition includes `brand_id`. CDC applies only newer sequences:

```python
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def merge_customers(spark: SparkSession, batch_df: DataFrame, target_path: str) -> None:
    # batch_df is already deduplicated to the latest _cdc_sequence per (brand_id, customer_id)
    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("t")
        .merge(
            batch_df.alias("s"),
            "t.brand_id = s.brand_id AND t.customer_id = s.customer_id",
        )
        .whenMatchedUpdate(
            condition="s._cdc_sequence > t._cdc_sequence AND s._cdc_action <> 'D'",
            set={...},  # current attributes, email_token, phone_masked, _cdc_sequence
        )
        .whenMatchedUpdate(
            condition="s._cdc_sequence > t._cdc_sequence AND s._cdc_action = 'D'",
            set={...},  # is_deleted = true, PII columns = NULL, _cdc_sequence
        )
        .whenNotMatchedInsert(values={...})
        .execute()
    )
```

- Bound merges where the data allows it (e.g. `event_date` lookback window for clickstream) to avoid unbounded target scans; document the bound.
- SCD2 history (`silver.customers_history`) is built by Spark from the CDC log, ordered by `_cdc_sequence` (Charter invariant 6).

### 2.4 Performance Work (Extended tier)
- Performance changes are justified by measurements following Charter §8 (fixed seed, machine spec, 1 warm-up + 5 runs, p50/p95, before/after — reported even when there is no improvement).
- Candidate techniques: `OPTIMIZE` + `ZORDER BY (brand_id, <timestamp>)` (verify support on Delta OSS 3.2), `broadcast()` for small dimensions, AQE skew-join vs. salting.

---

## 3. Analytics Engineering (dbt Core) Standards

### 3.1 Model Directory Architecture
```
dbt_project/
├── dbt_project.yml
├── profiles.yml                 # connection from env vars; no secrets committed
├── models/
│   ├── staging/                 # 1:1 views over Silver tables
│   │   ├── stg_customers.sql
│   │   ├── stg_customers_history.sql
│   │   ├── stg_orders.sql
│   │   └── stg_clickstream.sql
│   ├── intermediate/            # business aggregations & join logic
│   │   ├── int_customer_orders.sql
│   │   └── int_customer_engagement.sql
│   └── marts/                   # Gold
│       ├── dim_customers.sql
│       ├── dim_customers_history.sql
│       ├── fct_orders.sql
│       ├── obt_customer_360.sql
│       └── agg_rfm_segments.sql
├── macros/
└── tests/
    ├── generic/                 # composite (brand_id, key) relationship test
    └── singular/                # assert_positive_revenue.sql, assert_no_cross_tenant_joins.sql
```
No `snapshots/` for customers: SCD2 has a single owner (Spark).

### 3.2 Model Conventions & Contracts
- dbt reads Silver and writes Gold only.
- Every join and relationship test is keyed on `(brand_id, <key>)`.
- Enforced contracts where the adapter supports them (verified in the adapter capability spike, PROGRESS W3.D13.3); otherwise a documented alternative test.

```yaml
models:
  - name: dim_customers
    config:
      contract:
        enforced: true
    columns:
      - name: customer_sk
        data_type: string
        tests: [not_null, unique]
      - name: brand_id
        data_type: string
        tests: [not_null]
      - name: email_token
        data_type: string
```
- **Privacy**: raw email and phone never enter Gold. `email_token` (HMAC-SHA256 computed in Spark Silver) and `phone_masked` are passed through; dbt performs no hashing. These are privacy controls — the repository does not claim legal GDPR compliance.
- Reproducibility: time-relative metrics use an `as_of_date` variable, not `CURRENT_DATE`.

---

## 4. Orchestration (Apache Airflow) Standards

### 4.1 DAG Design Principles
- **Idempotency**: every task is safely re-runnable without manual intervention.
- **One parametrised DAG** per pipeline; tenants via dynamic task mapping (`.expand(brand_id=<config list>)`), not per-tenant DAG generation.
- **Streaming boundary**: Airflow triggers `Trigger.availableNow` Spark runs on a schedule; Spark owns checkpoints and stream state. Airflow does not host long-running streams.
- **Explicit constraints**: `retries=2`, `retry_delay=timedelta(minutes=5)`, `execution_timeout=timedelta(minutes=30)`, `max_active_runs=1`.
- Secrets via Airflow connections / environment variables, never in DAG code.

---

## 5. Infrastructure as Code (Terraform) Standards

- AWS resources under `infra/terraform/`: `s3_buckets.tf` (lifecycle rules, SSE, public access block), `iam.tf` (least-privilege role for the Spark job, one prefix-scoped tenant policy as isolation demonstration), `variables.tf`, budget alarm.
- `terraform fmt -check`, `terraform validate` and `tflint` pass before apply.
- `apply` and `destroy` are explicit, owner-approved actions; actual spend is recorded from billing, not estimated.
- State is never committed; credentials come from the AWS CLI profile / SSO, never from files in the repository.

---

## 6. Git & CI/CD Conventions

### 6.1 Conventional Commits
- `feat(generator): add multi-tenant customer CDC model`
- `feat(bronze): implement Kafka to Bronze structured streaming`
- `fix(silver): reject stale CDC updates by sequence`
- `perf(spark): add Z-Order on silver.orders (measured)`
- `docs(adr): record tenant isolation model`

Branching and safe Git usage: [GIT_CHEATSHEET.md](GIT_CHEATSHEET.md).

### 6.2 Pre-Push & PR Checklist
1. `ruff check src tests scripts` and `ruff format --check src tests scripts` pass.
2. `mypy` passes for touched packages.
3. Relevant unit tests pass offline; integration suites pass for pipeline changes.
4. For infrastructure changes: `docker compose up -d` and `python scripts/smoke_test.py` succeed.
5. The diff contains no secrets, `.env`, data files or generated artefacts.
6. Documentation and PROGRESS evidence updated when behaviour, contracts or status changed.
