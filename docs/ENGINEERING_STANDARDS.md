# Engineering Standards & Production Guidelines

> **Document Version**: 7.1
> **Last Updated**: 2026-11-05 (v7.1: runtime parity and package layout per ADR-0007/0009; Spark credentials via provider chain; Airflow 3; Terraform cost guardrails; new §7 secrets and credential management)
> **Target Audience**: Data engineers, AI pair-programmers and code reviewers of this repository
> **Enforcement**: *Target* — pre-commit hooks and GitHub Actions CI are planned (Milestone 1 adds the lint/unit CI job). Until then, standards are enforced by review and the checks listed in [CLAUDE.md](../CLAUDE.md).

> [!NOTE]
> **v7.0 changes**: aligned with [PROJECT_CHARTER.md](PROJECT_CHARTER.md) v7 — credentials from configuration only; Bronze metadata names (`_source_ref`, `brand_id`); sequence-ordered CDC MERGE; Spark-owned SCD2 (no dbt snapshots); HMAC pseudonymisation instead of SHA-256 "GDPR compliance"; one parametrised Airflow DAG with dynamic task mapping.

---

## 1. Python Code Standards

### 1.1 Runtime & Tooling
- **Runtime**: Python **3.11 in every runtime** — host `.venv/`, Spark image, Airflow image, AWS Glue 5.0, CI (ADR-0007/0011). No syntax newer than 3.11.
- **Package layout**: one installable package `lakehouse` (`src/lakehouse/`): `contracts`, `common` (shared) and the components `generator`, `ingestion`, `quality`, `silver`, `ops`. Cross-component imports are forbidden except where import-linter allows them (ADR-0009).
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
Endpoints come from configuration; **credentials never pass through Spark config or code**. Locally the S3A `EnvironmentVariableCredentialsProvider` reads `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` injected by Compose from `.env`; on AWS Glue the job's IAM role is used. Tables are addressed by name through the catalog (Hive metastore locally, Glue on AWS).

```python
from pyspark.sql import SparkSession

from lakehouse.common.settings import SparkSettings


def get_spark_session(app_name: str, settings: SparkSettings) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
    )
    if settings.environment == "local":  # SeaweedFS + Postgres-backed metastore
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
            )
        )
    # environment == "aws": AWS Glue 5.0 supplies the IAM role and the Glue Data Catalog
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

## 4. Orchestration (Apache Airflow 3.x) Standards

### 4.1 DAG Design Principles
- **Idempotency**: every task is safely re-runnable without manual intervention.
- **One parametrised DAG** per pipeline; tenants via dynamic task mapping (`.expand(brand_id=<config list>)`), not per-tenant DAG generation.
- **Streaming boundary**: Airflow triggers `Trigger.availableNow` Spark runs on a schedule; Spark owns checkpoints and stream state. Airflow does not host long-running streams.
- **Explicit constraints**: `retries=2`, `retry_delay=timedelta(minutes=5)`, `execution_timeout=timedelta(minutes=30)`, `max_active_runs=1`.
- Secrets via Airflow connections / environment variables, never in DAG code.

---

## 5. Infrastructure as Code (Terraform) Standards

- AWS resources under `infra/terraform/` (ADR-0008): `bootstrap/` (state bucket, native S3 lockfile) and modules `storage` (lifecycle, SSE-S3, public-access block), `catalog` (Glue databases), `identity` (Glue job role, prefix-scoped tenant demo policy, GitHub OIDC role), `compute` (Glue 5.0 job definitions: worker type/count, timeout, max concurrency 1), `budget` (5/20/40 USD alerts) — ADR-0011.
- `terraform fmt -check`, `terraform validate`, `tflint` and `trivy config` pass before `plan`; IAM changes get a `reviewer` pass.
- Cost guardrails: no NAT Gateway, MSK, EC2, EMR clusters or always-on endpoints; resources are destroyed after each cloud session (state bucket kept); CloudWatch log retention ≤ 14 days.
- `apply` and `destroy` are explicit, owner-approved actions; actual spend is recorded from billing, not estimated.
- State is never committed; credentials are short-lived — owner CLI access per W5.D25.1 (`aws login` or MFA role assumption; IAM Identity Center is unavailable on this account type) and GitHub OIDC in CI — never from files in the repository.

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
5. The diff contains no secrets, `.env`, data files or generated artefacts (pre-commit gitleaks).
6. Documentation and PROGRESS evidence updated when behaviour, contracts or status changed.

---

## 7. Secrets and Credential Management

The repository is private today and may become public as a portfolio; every control assumes it is public.

### 7.1 Where secrets live
| Context | Mechanism | Never |
|---|---|---|
| Local development | `.env` (gitignored), generated with random values by `scripts/bootstrap_env.py`; `.env.example` holds placeholders only | Secret fallback values in `docker-compose.yml`, Dockerfiles, `spark-defaults.conf`, code |
| Local AWS access | Short-lived credentials: `aws login` (console sign-in) where available, otherwise IAM user + MFA + role assumption with keys only in the OS credential store (ADR-0011) | Access keys in files, `.env` or the repository |
| CI (GitHub Actions) | GitHub OIDC → IAM role limited to this repository, `main` and the `aws-dev` environment; non-secret settings in `vars` | AWS keys in GitHub secrets; `echo` of any secret |
| AWS runtime | Glue job IAM role; secrets that must exist (e.g. `PII_HMAC_KEY`, webhook URL) in AWS Secrets Manager or SSM Parameter Store (SecureString) | Secrets in job arguments, Terraform variables files or state outputs |
| Frontend/backend projects (e.g. Supabase, future work) | service-role/secret keys server-side or in a secret store; frontend uses only the publishable/anon key protected by row-level security | Service-role keys in client bundles or `NEXT_PUBLIC_*` / `VITE_*` variables |

### 7.2 Preventive controls
- `.gitignore` covers `.env*` (except `.env.example`), keys, Terraform state/vars, `.terraform/`, local SeaweedFS identities.
- **gitleaks** in pre-commit and CI (W1.D1.3–D1.4); full-history scan before the repository is made public (W6.D36.2). GitHub secret scanning and push protection are enabled once the repository is public (availability for private repositories depends on the GitHub plan).
- Claude Code: deny rules for secret files in `.claude/settings.json` plus the `guard_commands.py` hook, which blocks shell commands that read secret files or dump environments/resolved configuration.
- Logs: structlog processors never receive secrets; DSNs are built without passwords in log context; `init_storage.sh` must not echo keys.
- Local services publish ports on `127.0.0.1` only.

### 7.3 Audit findings (2026-11-05) and remediation
| Finding | Risk | Remediation (task) |
|---|---|---|
| Full git history scanned for AWS keys, GitHub/Anthropic/Slack tokens, private keys, JWT/Supabase keys | — | No matches; `.env` never committed |
| Local-dev credential literals tracked in `docker-compose.yml` defaults, `.env.example`, `docker/spark/spark-defaults.conf`, `docker/seaweedfs/s3.json`, `scripts/init_postgres.sh`, `scripts/smoke_test.py`, README | Low (local-only defaults) but bad practice; Airflow Fernet key is a real key value | Placeholders in `.env.example` (done 2026-11-05); compose `${VAR:?}`, provider-based Spark credentials, templated SeaweedFS identities, smoke test reads env (W1.D2.2); **rotate**: generate a new Fernet key, Airflow secret key and all local passwords via `bootstrap_env.py` when the stack is rebuilt (new `crm_*` volumes) |
| `init_storage.sh` prints the access key ID | Low | Remove the echo (W1.D2.3) |
| Ports bound to `0.0.0.0` with default passwords | Medium on shared networks | Bind to `127.0.0.1` (W1.D2.2) |
| No CI / no secret scanning | Medium | gitleaks in pre-commit + CI (W1.D1.3–D1.4) |
| No cloud credentials in the repository; account on Free plan | — | Short-lived owner credentials + OIDC from the first AWS day (W5.D25) |
