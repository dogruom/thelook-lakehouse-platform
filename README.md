# Multi-Tenant CRM Lakehouse Platform

[![Status](https://img.shields.io/badge/Status-Early_Development_(Week_1%2F6)-orange?style=flat)](docs/PROGRESS.md)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose_v2-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.1-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.7_(KRaft)-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-OSS_3.2-003366?style=flat&logo=delta&logoColor=white)](https://delta.io/)
[![dbt Core](https://img.shields.io/badge/dbt--core-1.8-FF694B?style=flat&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.9.3-017CEE?style=flat&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> **A local-first, multi-tenant streaming lakehouse:** Kafka → Spark Structured Streaming → Delta Lake (Bronze / Silver) → dbt (Gold).  
> Built around **correctness under failure** — duplicate delivery, out-of-order CDC, late events, schema drift, restarts and cross-tenant leakage — and around proving it with tests rather than claims.

> [!IMPORTANT]
> **Project status: early development (Week 1 of 6).** The local Docker infrastructure and generator configuration are in place; most components described below are **planned**. Every section is marked ✅ Implemented · 🔄 In progress · 📋 Planned. Live tracker: [docs/PROGRESS.md](docs/PROGRESS.md).

> [!NOTE]
> **Data**: All data is **fully synthetic**, produced by `src/generator/` (Faker + Pydantic v2). The entity model is inspired by the *TheLook* e-commerce domain (hence the repository name) and reshaped into a multi-brand CRM event model. No real company or customer data is used.

---

## 1. What This Project Demonstrates

**Scenario (synthetic):** a CRM / loyalty data platform serving several retail brands (tenants) from one shared lakehouse.

The engineering questions it answers:

| Question | Approach | Status |
|---|---|---|
| How do you turn at-least-once streams into idempotent tables? | Spark checkpoints + `foreachBatch` `MERGE` on deterministic keys; restart & replay tests | 📋 |
| How do you apply out-of-order CDC and keep history? | Sequence-ordered merge (`_cdc_sequence`), Spark-owned SCD Type 2 | 📋 |
| How do you prevent cross-tenant leakage in shared tables — and prove it? | `brand_id` in every key/join, deliberate key collisions, negative tests | 📋 |
| How do you quarantine bad records without losing them? | Two-stage DLQ, reconciliation against a ground-truth manifest, controlled replay | 📋 |
| What changes between local and cloud? | Shared logic, thin environment adapters, per-environment test reports | 📋 |

---

## 2. Implementation Status

| Component | Status | Evidence / Location |
|---|---|---|
| Local stack: Kafka (KRaft), SeaweedFS, Spark, PostgreSQL, Airflow | ✅ | [docker-compose.yml](docker-compose.yml), [scripts/smoke_test.py](scripts/smoke_test.py) |
| Generator configuration (`pydantic-settings`) | ✅ | [src/generator/config.py](src/generator/config.py) |
| Synthetic generators (customer CDC, orders, products, clickstream) | 🔄 | Week 1 |
| Kafka producer (keyed, idempotent) | 📋 | Week 1 |
| Bronze ingestion (stream + batch), DLQ, replay | 📋 | Week 2 |
| Silver: CDC MERGE, dedup, tenant-isolation tests | 📋 | Week 2 |
| Silver SCD2 history + dbt Gold marts | 📋 | Week 3 |
| Customer 360 / RFM marts, measured benchmarks | 📋 | Week 4 |
| Airflow orchestration, GitHub Actions CI | 📋 | Week 5 |
| Terraform (AWS S3 + IAM), Spark job on AWS S3 | 📋 | Week 6 |
| Databricks Auto Loader / Asset Bundles | 📋 Stretch | Week 6 |

---

## 3. Target Architecture

```mermaid
flowchart TB
    subgraph Producers["1. Synthetic Producers (Python / Faker / Pydantic v2)"]
        direction TB
        GEN_CLICK["Clickstream Events<br/>(page_view, search, add_to_cart, purchase)"]
        GEN_CDC["Customer CDC<br/>(I / U / D + _cdc_sequence)"]
        GEN_TX["Orders, Items, Catalog<br/>(micro-batch files)"]
    end

    subgraph Ingestion["2. Ingestion"]
        direction TB
        KAFKA["Apache Kafka 3.7 (KRaft)<br/>key = brand_id + entity key"]
        S3_RAW["Landing Zone (SeaweedFS / AWS S3)<br/>landing/{brand_id}/{entity}/{date}/"]
    end

    subgraph Processing["3. Compute (Apache Spark 3.5)"]
        direction TB
        SPARK_STREAM["Structured Streaming<br/>checkpoints | availableNow"]
        SPARK_BATCH["Batch jobs"]
    end

    subgraph Medallion["4. Medallion Storage (Delta Lake OSS 3.2)"]
        direction TB
        BRONZE["Bronze: append-only raw records"]
        DLQ["DLQ: rejected records + replay metadata"]
        SILVER["Silver (Spark): CDC MERGE | dedup | SCD2 | PII tokens"]
        GOLD["Gold (dbt): dims, facts, obt_customer_360, agg_rfm_segments"]
    end

    subgraph Governance["5. Orchestration, Quality, IaC"]
        direction TB
        AIRFLOW["Airflow 2.9: parametrised DAG + task mapping"]
        QUALITY["Pydantic contracts | Spark checks | dbt tests"]
        TERRAFORM["Terraform: AWS S3 + IAM"]
    end

    GEN_CLICK --> KAFKA
    GEN_CDC --> KAFKA
    GEN_TX --> S3_RAW

    KAFKA --> SPARK_STREAM
    S3_RAW --> SPARK_BATCH
    SPARK_STREAM --> BRONZE
    SPARK_BATCH --> BRONZE

    BRONZE -->|"validation gate"| DLQ
    BRONZE -->|"MERGE / dedup"| SILVER
    DLQ -->|"controlled replay"| SILVER
    SILVER -->|"dbt models"| GOLD

    AIRFLOW -.->|"orchestrates"| Processing
    AIRFLOW -.->|"dbt build"| GOLD
    QUALITY -.->|"guards"| SILVER
    TERRAFORM -.->|"provisions"| S3_RAW
```

Full design, invariants and scope tiers: [docs/PROJECT_CHARTER.md](docs/PROJECT_CHARTER.md).

---

## 4. Key Design Decisions

### Delivery semantics (no blanket "exactly-once" claim)
| Stage | Guarantee |
|---|---|
| Generator → Kafka | At-least-once (idempotent producer; business duplicates injected on purpose) |
| Kafka → Bronze | Each offset committed once per checkpoint lineage (Spark checkpoint + Delta sink) |
| Bronze → Silver | Idempotent outcome — re-running a batch yields the same state |
| Checkpoint loss | Replay from Kafka; Silver `MERGE` absorbs re-reads (verified by test) |

### Multi-tenancy
Tenants share tables. `brand_id` is mandatory and part of every key and join. Partitioning is a performance choice, **not** a security boundary; isolation is demonstrated with tests that inject cross-tenant key collisions, including a negative test proving that a leaky join is caught.

### Ownership
Spark/Delta owns Bronze, DLQ and Silver (including SCD2 history). dbt owns Gold only and never writes Silver.

### Privacy controls
Emails are pseudonymised with **keyed HMAC-SHA256** in Silver (plain SHA-256 of low-entropy values is dictionary-reversible); phones are masked; raw PII never reaches Gold. An audited erasure procedure covers Bronze, Silver, Gold, time travel and `VACUUM`. This demonstrates privacy controls; it is **not** a legal compliance claim.

---

## 5. Local vs. Cloud: What Is Shared, What Differs

| Layer | Local ($0) | AWS (Extended / Stretch) | What actually differs |
|---|---|---|---|
| Infrastructure | [docker-compose.yml](docker-compose.yml) | Terraform (`infra/terraform/`) | Provisioning model |
| Object storage | SeaweedFS (S3-compatible API) | AWS S3 (SSE, lifecycle rules) | IAM, encryption, listing/error behaviour; endpoint config isolated in settings |
| Streaming | Kafka 3.7 (KRaft) | Not deployed in v1 | Kinesis is compared in ADR-0002 only |
| Compute | OSS Spark 3.5.1 standalone | Local Spark → AWS S3 (Extended); Databricks (Stretch) | Runtime versions, connectors, cluster config |
| Table format | Delta Lake OSS 3.2 | Delta OSS / Databricks Delta | Feature availability verified per runtime |
| File ingestion | Spark file source | Databricks Auto Loader (Stretch) | Different source implementation → separate adapter |
| Transformation | dbt Core 1.8 (`dbt-spark`) | `dbt-databricks` (Stretch) | Adapter capabilities verified per adapter |

Business logic is shared; environment specifics live in configuration and thin adapters. Results are reported **per environment**.

---

## 6. Medallion Layers (Planned)

```
Landing (S3) ──> Bronze (append-only) ──> Silver (CDC, dedup, SCD2) ──> Gold (dbt marts)
                    │       ▲
                    └─> DLQ ┘  (controlled replay)
```

### 🥉 Bronze — Append-Only Raw Records 📋
- No `UPDATE` / `DELETE` in normal processing; the only exception is the audited privacy erasure procedure.
- Malformed payloads are stored raw with `_parse_error` — never discarded.
- Metadata: `_ingest_ts`, `_source_ref` (file URI or Kafka topic/partition/offset), `_batch_id`, `brand_id`.

### 🥈 Silver — Conformed State & History 📋
**Sequence-ordered CDC merge** (illustrative; column lists abbreviated):

```sql
MERGE INTO silver.customers AS t
USING (
  -- keep only the latest event per key in this micro-batch,
  -- otherwise MERGE fails on multiple source matches
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY brand_id, customer_id ORDER BY _cdc_sequence DESC) AS rn
    FROM cdc_batch
  ) WHERE rn = 1
) AS s
ON  t.brand_id = s.brand_id
AND t.customer_id = s.customer_id
WHEN MATCHED AND s._cdc_sequence > t._cdc_sequence AND s._cdc_action = 'D' THEN
  UPDATE SET t.is_deleted = true, t.email_token = NULL, t.phone_masked = NULL,
             t._cdc_sequence = s._cdc_sequence
WHEN MATCHED AND s._cdc_sequence > t._cdc_sequence THEN
  UPDATE SET t.city = s.city, t.loyalty_tier = s.loyalty_tier, t.is_deleted = false,
             t._cdc_sequence = s._cdc_sequence  -- ...
WHEN NOT MATCHED AND s._cdc_action <> 'D' THEN
  INSERT (brand_id, customer_id, city, loyalty_tier, _cdc_sequence /* ... */)
  VALUES (s.brand_id, s.customer_id, s.city, s.loyalty_tier, s._cdc_sequence /* ... */);
```

- Stale events (`_cdc_sequence ≤` current) never overwrite newer state; duplicates are no-ops.
- **SCD Type 2** history (`silver.customers_history`: `valid_from`, `valid_to`, `is_current`) is built by Spark from the CDC log — a single source of truth, no dbt snapshots.
- Orders / clickstream are deduplicated on `(brand_id, <id>)`; orphan foreign keys are quarantined as *retryable* and re-evaluated on replay.

### 🥇 Gold — dbt Marts 📋
- Kimball `dim_*` / `fct_*` models, `obt_customer_360`, `agg_rfm_segments` (NTILE-based RFM).
- Composite-key (`brand_id`, key) relationship tests; tenant-scoped consumption views.
- Churn-risk feature mart: Stretch.

---

## 7. Data Quality & DLQ (Planned)

- **Two stages**: parse failures captured at Bronze ingest; contract / integrity failures captured at the Bronze → Silver gate.
- **Reconciliation**: per batch, `bronze = silver_accepted + dlq + duplicates_removed + late_beyond_window`, checked against the generator's ground-truth manifest of injected defects.
- **Replay**: fixed records are re-processed with their original `event_id`; replaying twice does not duplicate Silver.

```json
{
  "_dlq_id": "8f3b1e72-51a8-43d9-95a2-c128f73b64bc",
  "brand_id": "brand_alpha",
  "_bronze_ref": "bronze.raw_orders@batch=42/file=part-0003.json/row=118",
  "_error_category": "REFERENTIAL_INTEGRITY_VIOLATION",
  "_rejection_reason": "customer_id '9f82d1c0-...' not found in silver.customers for brand_alpha",
  "_retryable": true,
  "_rejected_at": "2026-10-20T02:15:30.124Z",
  "_pipeline_run_id": "<run id>",
  "_replay_status": "PENDING",
  "_replay_count": 0
}
```

---

## 8. Orchestration (Planned — Extended)

- One **parametrised DAG** using dynamic task mapping over the configured tenant list (no per-tenant DAG generation).
- Airflow triggers Spark jobs (`Trigger.availableNow` micro-batches) and `dbt build`; it does not host long-running streams — Spark owns checkpoints and stream lifecycle.
- Deterministic, seeded backfill; webhook alerts on failure and DLQ threshold breaches.

---

## 9. Local Quickstart

### What works today ✅

**Prerequisites**
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with Docker Compose v2
- Python 3.11+, Git
- Memory: running Kafka, Spark, Airflow, PostgreSQL and SeaweedFS together likely needs **≥ 8 GB RAM allocated to Docker** — *to be confirmed by a clean-machine measurement*.

```bash
git clone https://github.com/dogruom/thelook-lakehouse-platform.git
cd thelook-lakehouse-platform
cp .env.example .env

# Start the local stack
docker compose up -d --build
docker compose ps

# Infrastructure smoke test (networking, buckets, Kafka topics, service readiness)
python scripts/smoke_test.py
```

| Service | Local URL | Default Credentials (dev only) |
|---|---|---|
| Apache Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Spark Master UI | http://localhost:8181 | — |
| SeaweedFS S3 API | http://localhost:9000 | `lakehouse_admin` / `lakehouse_secret_dev` |
| SeaweedFS Filer | http://localhost:9001 | — |
| Kafka broker | `localhost:9094` | — |

### Pipeline run 📋 (planned — commands become valid as components land)

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements/dev.txt

python -m src.generator.batch_runner --batches 15 --batch-size 500
python -m src.generator.kafka_producer --events 5000

cd dbt_project && dbt build
```

---

## 10. Testing Strategy & CI (Planned)

| Suite | Runs where | Scope |
|---|---|---|
| Unit | Offline, every PR | Generators, contracts, pure transformation functions |
| Correctness (integration) | Docker profile, PRs to `main` | Restart/replay, CDC ordering, tenant isolation, DLQ replay, reconciliation |
| dbt | CI | `dbt compile`; `dbt build` in integration profile |
| Static | Every PR | `ruff check`, `ruff format --check`, `mypy` |

```bash
pytest tests/unit/ -v --cov=src --cov-report=term-missing
ruff check src/ tests/ && ruff format --check src/ tests/
mypy src/
```

A CI badge will be added after the first green workflow run.

---

## 11. Repository Layout (Target)

> Most paths below are **planned**; see the status table in §2 for what exists today.

```
thelook-lakehouse-platform/
├── .github/workflows/ci.yml       # CI: lint, types, unit tests, integration job, dbt compile
├── dags/                          # Airflow: parametrised multi-tenant pipeline
├── dbt_project/                   # dbt Core Gold models (reads Silver, writes Gold)
│   └── models/{staging,intermediate,marts}/
├── docker/                        # Container definitions
├── docs/
│   ├── PROJECT_CHARTER.md         # Scope tiers, invariants, design
│   ├── DATA_CONTRACTS.md          # Schemas, dirty-data budgets, DLQ specs
│   ├── ENGINEERING_STANDARDS.md   # Coding, Spark, dbt, Airflow, Git standards
│   ├── DELIVERY_PLAN.md           # 6-week execution plan
│   ├── PROGRESS.md                # Sprint tracker with evidence
│   └── adr/                       # Architecture Decision Records
├── infra/terraform/               # AWS S3 + IAM (Extended)
├── notebooks/                     # Databricks Auto Loader (Stretch)
├── scripts/                       # smoke_test.py, init scripts, DLQ replay, backfill
├── src/
│   ├── generator/                 # Synthetic multi-tenant generator + Kafka producer
│   ├── streaming/                 # Bronze / Silver Spark jobs
│   ├── quality/                   # Validation gate, DLQ router, reconciliation
│   └── utils/                     # Structured logging, shared helpers
├── tests/{unit,integration}/
├── docker-compose.yml
└── pyproject.toml
```

---

## 12. Future Work (Out of Scope for v1)

- Kinesis adapter behind the common event envelope (compared in ADR-0002).
- Reverse ETL / operational triggers (e.g. webhook to marketing tools).
- BI dashboard on Gold marts.
- Managed Airflow, Kubernetes deployment.

---

## 📚 Documentation

- 📋 [Project Charter — scope, invariants, design](docs/PROJECT_CHARTER.md)
- 📐 [Data Contracts & Entity Schemas](docs/DATA_CONTRACTS.md)
- 🛠️ [Engineering Standards](docs/ENGINEERING_STANDARDS.md)
- 📅 [6-Week Delivery Plan](docs/DELIVERY_PLAN.md)
- 📊 [Progress Tracker](docs/PROGRESS.md)
- 🏛️ [ADR-0001: Local S3 Object Storage with SeaweedFS](docs/adr/0001-local-object-storage-seaweedfs.md)

---

## 👤 Author

**Ömer Faruk Doğru**  
Senior Data / Analytics Engineer — Berlin, Germany  
- **Email**: [dogruomerfaruk@outlook.com](mailto:dogruomerfaruk@outlook.com)  
- **LinkedIn**: [linkedin.com/in/omer-faruk-dogru-2020d/](https://linkedin.com/in/omer-faruk-dogru-2020d/)  
- **GitHub**: [github.com/dogruom](https://github.com/dogruom)
