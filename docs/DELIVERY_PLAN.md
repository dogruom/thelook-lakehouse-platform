# Delivery Plan: Six Weeks, Three Demonstrable Milestones

> **Document Version**: 7.0
> **Last Updated**: 2026-11-05
> **Budget**: 6 weeks × 6 days × 6 hours ≈ 216 focused hours (owner + Claude Code as AI pair-programmer)
> **Relationship to other documents**: scope and invariants → [PROJECT_CHARTER.md](PROJECT_CHARTER.md); task-level acceptance criteria and evidence → [PROGRESS.md](PROGRESS.md) (task IDs `W<week>.D<day>.<n>` below refer to it). This plan decides **sequencing and milestone gates**.

> [!NOTE]
> **v7.0 re-plan**: the previous week-by-week plan (v6) conflicted with Charter v7 (dbt-snapshot SCD2, SHA-256 masking, fixed % performance targets, Databricks as a parallel second cloud). It is replaced by three two-week milestones, each independently demonstrable, sequenced using the Berlin job-market sample in §5. Superseded content remains available in Git history.

---

## 1. Principles

1. **Each milestone ships a working, tested, documented vertical result** — not a layer of half-finished components.
2. **Correctness before breadth**: the 🔺 correctness tests (restart/replay, CDC ordering, tenant isolation) gate later work (Charter §1 tier gate).
3. **One primary technology area per milestone**, so learning is deep enough to defend in an interview.
4. **Evidence or it did not happen**: a milestone is done only when its acceptance checks pass and PROGRESS.md links commit hashes and outputs.
5. **Cut scope, not quality**: each milestone lists what is deferred if time runs out.

| Milestone | Weeks | Primary area | Release tag | Demo checkpoint |
|---|---|---|---|---|
| **M1** Reliable streaming ingestion | 1–2 | Kafka + Spark Structured Streaming + Delta Bronze/DLQ | `v0.1.0` | End of week 2 |
| **M2** Data correctness & analytics products | 3–4 | CDC/SCD2, tenant isolation, dbt Gold (Customer 360, RFM), Airflow batch DAG | `v0.2.0` | End of week 4 |
| **M3** Cloud productionisation on AWS | 5–6 | Terraform, IAM, CI/CD, observability, cost; bounded Databricks exercise | `v1.0.0` | End of week 6 |

---

## 2. Milestone 1 — Weeks 1–2: Reliable Streaming Ingestion Slice 🟩

**Objective**: Synthetic multi-tenant events flow `Python generator → Kafka → Spark Structured Streaming → Bronze Delta + DLQ` on the local Docker stack, survive restarts without losing or double-recording offsets, and every injected defect is accounted for.

### Scope (PROGRESS task IDs)
| Order | Work | Tasks |
|---|---|---|
| 0 | Environment baseline: project-local venv, working `pytest` (pytest-cov), re-run smoke test and record Day 1 evidence, pin `seaweedfs`/`aws-cli` image versions | W1.D1.1–2 (evidence) |
| 1 | Align `GeneratorConfig` with Charter v7 (tenant list, seed, CDC topic, out-of-order and collision rates) | W1.D1.2 follow-up |
| 2 | Customer CDC model with `_cdc_sequence`, dirty injection, ground-truth manifest | W1.D2.1–3 |
| 3 | Clickstream model; keyed idempotent producer for `customer_cdc` and `clickstream_events`; streaming noise | W1.D5.1–3 |
| 4 | Unit test suite, ruff, mypy; **CI job 1** (lint + offline unit tests) on GitHub Actions — pulled forward from W5.D30.1 | W1.D6.1–2, W5.D30.1 (job 1) |
| 5 | Kafka → Bronze streaming with checkpoints, `_source_ref`, `_parse_error` | W2.D8.1 |
| 6 | 🔺 Restart & checkpoint-loss test | W2.D8.2 |
| 7 | Validation gate + DLQ for stream entities (customers, clickstream); DLQ reconciled against manifest | W2.D9.1–2 (stream subset) |
| 8 | ADR-0002 (Kafka, event envelope, keys vs. headers); ADR-0006 draft (delivery semantics); M1 demo runbook | — |

**Deferred to M2**: batch landing entities (orders, items, products: W1.D3, W1.D4, W2.D7) and all Silver work.

### Acceptance criteria (measurable)
- [ ] `pytest tests/unit -q` passes **offline**; coverage on `src/generator/` ≥ 80% (hygiene floor).
- [ ] Same seed + config → byte-identical generator output (test).
- [ ] Dirty-data rates within the tolerances in [DATA_CONTRACTS.md](DATA_CONTRACTS.md) over 10,000 records; manifest matches injected defects exactly.
- [ ] ≥ 5,000 clickstream and ≥ 1,000 CDC events for ≥ 3 brands produced; messages show composite keys and the `brand_id` header; a test asserts all events of one `(brand_id, customer_id)` land in one partition.
- [ ] Bronze row count = produced messages (including `_parse_error` rows).
- [ ] 🔺 After killing and restarting the stream with its checkpoint, every `(topic, partition, offset)` appears **exactly once** in Bronze; after deliberate checkpoint loss, re-reads are observed and documented (expected, absorbed by Silver MERGE in M2).
- [ ] DLQ contains exactly the manifest's validation-failing defects for stream entities; duplicates and late events are **not** in the DLQ.
- [ ] GitHub Actions job 1 green on `main` (run linked).

### Demo
`docs/runbooks/m1-streaming-demo.md`: `docker compose up -d` → smoke test → produce events → run Bronze stream (`availableNow`) → kill/restart → show Bronze offset uniqueness and DLQ counts vs. manifest.

### Portfolio result & credible interview claims
- "I designed composite Kafka keys (`brand_id:entity_id`) to keep per-entity ordering without a hot tenant partition, and kept `brand_id` as a header for routing — not for partitioning."
- "Producer delivery is at-least-once with an idempotent producer; Spark checkpoints plus the Delta sink commit each offset once per checkpoint lineage — I tested restart and checkpoint loss, and I can show the difference."
- "Bad records are never dropped: parse failures stay raw in Bronze, contract failures go to a DLQ that references Bronze, reconciled against a ground-truth manifest."
- **Not claimable**: exactly-once end-to-end, Silver correctness, cloud deployment.

**Learning focus**: Kafka partitions/keys/headers, producer idempotence, Structured Streaming triggers and checkpoints, Delta transaction log, test design with ground truth.

---

## 3. Milestone 2 — Weeks 3–4: Data Correctness & Analytics Products 🟩

**Objective**: Correct, idempotent, tenant-isolated Silver tables (CDC current state + Spark-owned SCD2) and meaningful dbt Gold products — Customer 360 and RFM — built by one orchestrated, re-runnable batch pipeline.

### Scope (priority order)
| Order | Work | Tasks |
|---|---|---|
| 1 | Batch landing generators (orders, items, products, schema evolution) and Bronze batch ingest | W1.D3, W1.D4, W2.D7 |
| 2 | Sequence-ordered customer CDC MERGE with tombstones and HMAC tokens; 🔺 CDC correctness suite | W2.D10.1–3 |
| 3 | Orders/items/clickstream dedup and FK gate; controlled DLQ replay | W2.D11, W2.D9.3 |
| 4 | Reconciliation, idempotent re-run, 🔺 tenant isolation suite (with negative test) | W2.D12.1–3 |
| 5 | SCD2 `silver.customers_history` + scenario tests | W3.D15 |
| 6 | dbt adapter capability spike, staging, dimensions, facts, tests, docs | W3.D13–D18 |
| 7 | `obt_customer_360` and `agg_rfm_segments` (per-brand NTILE) | W4.D19.1, W4.D20 |
| 8 | One parametrised Airflow batch DAG with dynamic task mapping + tenant failure isolation | W5.D26 |
| 9 | ADR-0003 (append-only Bronze & erasure), ADR-0004 (Spark/dbt ownership), ADR-0005 (tenant isolation & partitioning) | — |

**Deferred (cut list, in this order if time runs short)**: Airflow DAG (8) → M3; benchmarks W4.D19.2/W4.D22–24 → post-v1; churn mart W4.D21 and Great Expectations W5.D28.2 → stretch.

### Acceptance criteria
- [ ] 🔺 All six CDC scenarios pass (in-order, stale `U`, late `U` after `D`, duplicate, multi-event batch, cross-brand same `customer_id`).
- [ ] Re-running the full Bronze → Silver pipeline yields identical Silver content hashes; zero duplicate business keys.
- [ ] Reconciliation per batch: `bronze = silver_accepted + dlq + duplicates_removed + late_beyond_window`, matching the manifest.
- [ ] 🔺 Tenant isolation suite passes **and** the deliberately leaky join fixture fails the assertion.
- [ ] SCD2: identical history for in-order and shuffled delivery; Istanbul → Berlin produces two versions.
- [ ] `dbt build` passes (100% of tests) including composite `(brand_id, key)` relationship tests and `assert_no_cross_tenant_joins`.
- [ ] RFM: no segment at 0% or 100% for any brand; distribution reported as synthetic.
- [ ] No raw email/phone in Silver or Gold (test).

### Demo
`docs/runbooks/m2-correctness-demo.md`: run batch pipeline (Airflow DAG or script) → show CDC scenarios, SCD2 history, tenant isolation negative test → `dbt docs` lineage → Customer 360 / RFM query per brand.

### Credible interview claims
- "CDC is applied by source sequence, never by arrival time; stale and out-of-order events are no-ops — six scenario tests prove it."
- "SCD2 has a single owner (Spark); dbt builds Kimball marts on top — I can explain why two history owners would drift."
- "Tenant isolation is tested, including a negative test that proves the test catches a leaky join."
- "Customer 360 and RFM are reproducible via an `as_of_date` variable."
- **Not claimable**: legal GDPR compliance (HMAC tokens are pseudonymous), performance at scale.

**Learning focus**: Delta MERGE semantics, idempotency, SCD2 under late data, dimensional modelling, dbt testing/contracts, Airflow dynamic task mapping.

---

## 4. Milestone 3 — Weeks 5–6: Cloud Productionisation on AWS 🟦

**Decision**: **AWS is the primary hands-on cloud** (Terraform + S3 + IAM + the existing Spark/Delta job against S3). Rationale: it matches the Charter's Extended tier and appeared most often across employers in the job-market sample (§5); GCP demand was concentrated at one employer. **Databricks is a bounded second-platform exercise** (≤ 1 day), only after the AWS core is green — two complete cloud platforms in parallel are out of scope.

### Scope
| Order | Work | Tasks |
|---|---|---|
| 1 | Terraform: S3 buckets (SSE, lifecycle, public-access block), least-privilege IAM role, prefix-scoped tenant policy, budget alarm; `fmt`/`validate`/`tflint` | W6.D31 |
| 2 | `terraform apply` (owner-approved); run Bronze → Silver job from local Spark against AWS S3; reconcile counts; tenant prefix policy denies cross-tenant read (tested) | W6.D32 |
| 3 | `docs/LOCAL_VS_CLOUD.md`: every config/behaviour difference vs. SeaweedFS | W6.D32.3 |
| 4 | CI/CD: job 2 (Docker integration suites on PRs to `main`), Terraform checks in CI, pre-commit with secret scanning | W5.D30.1 (job 2), W5.D30.2 |
| 5 | Observability: pipeline metrics table (row counts, DLQ rate, `late_beyond_window`, consumer lag), failure/threshold alert payload, runbooks for replay and recovery | W5.D27.2, W5.D28.1 |
| 6 | Cost & teardown: actual spend from billing recorded; `terraform destroy` (owner-approved) | W6.D34.2 |
| 7 | 🟨 Databricks exercise: run the shared Bronze logic with Auto Loader as file-source adapter on a sample; document runtime/catalog differences. Verify the platform's free/trial terms and cost before starting | W6.D33 (reduced) |
| 8 | Release: README with evidence links, ADR set complete, interview guide, `v1.0.0` tag | W6.D35–36 |

**Kafka stays local**: managed Kafka (MSK) is not provisioned (cost and time); the cloud proof covers storage, identity, compute against S3, CI/CD and operations. This is stated explicitly in the README.

**Cut list**: Databricks exercise (7) → documented next-stage extension; backfill determinism W5.D29 → post-v1; streaming DAG W5.D27.1 → post-v1.

### Acceptance criteria
- [ ] `terraform plan` clean; `tflint` and `terraform fmt -check` pass in CI.
- [ ] IAM: Spark role can read/write only `lakehouse/*` prefixes it needs; tenant policy for `brand_alpha` is denied on `brand_beta` prefixes (CLI evidence).
- [ ] Same job code runs locally and against AWS S3 with configuration changes only; row counts reconcile.
- [ ] Both CI jobs green; secret scanning in pre-commit and CI.
- [ ] Alert fires on deliberate failure and DLQ-rate breach (payload in logs).
- [ ] Actual cloud spend recorded; resources destroyed and verified.
- [ ] README states only verified features, with evidence links; quickstart timed on a clean machine.

### Credible interview claims
- "I provisioned the AWS storage and IAM layer with Terraform, ran the same Spark/Delta job against S3 and documented what differs from the local S3-compatible store."
- "I designed least-privilege and prefix-scoped tenant policies and tested a denied cross-tenant read."
- "CI runs linting, offline unit tests, Terraform checks and Docker-based integration suites."
- If done: "I ran the Bronze logic on Databricks with Auto Loader as an exercise and documented the runtime differences." — **not** "production Databricks experience".

**Learning focus**: Terraform state and plans, IAM policy design, S3 semantics vs. S3-compatible stores, CI pipelines, operational metrics and runbooks, cost control.

---

## 5. Job-Market Evidence (Berlin, checked 2026-11-05)

> [!WARNING]
> **Sample quality is limited.** Most career pages were expired or blocked (403/404/410) when checked; of 11 postings read, only one still appeared open. The table shows what these advertisements asked for — it is a directional signal, **not** a statistically representative view of the market. One employer's stack is not treated as universal.

| # | Company — role | Status when checked | Notable requirements (R) / preferences (P) | Cloud |
|---|---|---|---|---|
| 1 | [Aroundhome — (Senior) Data Engineer](https://builtin.com/job/senior-data-engineer-modern-data-platform-ai-all-genders-berlin-hybrid-or-remote/9782679) | Removed 2026-10-28 | R: Databricks/Delta or Snowflake, dbt, SQL, Python, Spark, Airflow, Terraform, Kafka, Structured Streaming, Flink, Debezium, Unity Catalog, CI/CD, data modelling | AWS, Databricks |
| 2 | [Zalando — Senior CRM Data Engineer](https://jobs.zalando.com/de/jobs/2719711) | Unverified (404; aggregator reposts) | R (second-hand): Databricks/Spark/Delta in production, Terraform, Python, CI/CD, CRM, GDPR/PII with Unity Catalog | Databricks |
| 3 | [Trade Republic — Data Platform Engineer](https://jobs.accel.com/companies/trade-republic/jobs/44682895-data-engineer-berlin) | Appeared open | R: platform services, query engines/orchestration/lakehouse; stack: Spark, Iceberg, Airflow, dbt, Kubernetes | AWS |
| 4 | [Trade Republic — Analytics Engineer](https://builtin.com/job/analytics-engineer-berlin/9207447) | Removed 2026-08-10 | R: ingestion-to-marts pipelines; stack: S3, Snowflake, Airflow, dbt, Python | AWS |
| 5 | [Delivery Hero — Senior Data Engineer, Logistics](https://careers.deliveryhero.com/job/senior-data-engineer-python-logistics-data-and-ml-in-berlin-germany-jid-9641) | Expired | R: Kafka, Debezium, BigQuery, Spark/Flink, data quality/observability, on-call | GCP |
| 6 | [Delivery Hero — Data Engineer, Search & Recs](https://careers.deliveryhero.com/job/data-engineer-search-and-recommendations-all-genders-in-berlin-germany-jid-3061) | Expired | R: Python, SQL, data modelling; P: Spark/Flink; stack: Airflow, dbt, Terraform | GCP or AWS |
| 7 | [Delivery Hero — Data Engineer, Tech Foundations](https://careers.deliveryhero.com/job/data-engineer-tech-foundations-fixed-term-in-berlin-germany-jid-7751) | Expired | R: BigQuery, Airflow, data modelling, PySpark, pytest, PII/governance; P: Terraform, CI/CD | GCP |
| 8 | [Delivery Hero — Senior DE, Vendor Sales Ops](https://careers.deliveryhero.com/job/data-engineer-vendor-sales-operations-all-genders-in-berlin-germany-jid-2018) | Expired | R: pipelines, data modelling, Lambda/Kappa, Docker/Kubernetes, Terraform; P: CI/CD | GCP (or AWS) |
| 9 | [Ströer SSP — Senior Data Engineer](https://karriere.stroeer.com/job/senior-data-engineer-m-w-d-in-berlin-jid-199) | Expired | R: JVM language, distributed systems; stack: Kubernetes, Terraform | AWS |
| 10 | [Ströer SSP — Data Software Engineer](https://karriere.stroeer.com/en/job/mid-senior-data-software-engineer-f-m-d-in-berlin-jid-1178) | Expired | R: cloud, distributed systems; stack: Flink, Spark, Kafka, Hudi/Iceberg, Terraform | AWS |
| 11 | [Gemma Analytics — (Senior) Data & Analytics Engineer](https://join.com/companies/gemmaanalytics/14083658-senior-data-and-analytics-engineer) | Archived (posted 2025-04-17) | R: SQL, dbt in production, Kimball/Data Vault, Snowflake/BigQuery, Airflow, Git/CI/CD, Python | — |

**Recurring signals (counts are approximate, n = 11)**: Python (10 mentions), data modelling (5 required), Airflow (6), dbt (7), Spark (7), Terraform (7), CI/CD (5), streaming incl. Kafka/Flink (4 required, 1 preferred), AWS (~6 ads from 4 employers) vs. GCP/BigQuery (5 ads, 4 from one employer), Databricks (2). English was the working language in every ad; German appeared only as a bonus.

### How the evidence shaped this plan
| Signal | Plan response |
|---|---|
| Python, SQL, data modelling, Airflow, dbt are the common base | M2 centres on modelling, dbt and one solid Airflow DAG |
| Kafka/streaming is a senior differentiator, not universal | M1 does streaming deeply on one slice rather than broadly |
| Terraform and CI/CD recur across employers | CI job 1 pulled into M1; Terraform + CI/CD core of M3 |
| AWS appears across more employers than GCP; Databricks in 2 ads (one CRM-specific) | AWS primary in M3; Databricks as bounded exercise |
| PII/GDPR governance requested in CRM/platform roles | HMAC pseudonymisation, tombstones, erasure ADR in M2; no compliance claim |

**Known gaps (documented, not built in v1)**: Apache Iceberg (short Delta vs. Iceberg note in an ADR), Debezium/real database CDC (simulated via `_cdc_sequence`; next-stage extension), a cloud warehouse (Snowflake/BigQuery), Kubernetes, data catalog / Unity Catalog, managed observability tools.

---

## 6. Weekly Rhythm and Gates

- **Day 1 of each milestone**: confirm scope and cut list; create branch `feat/m<N>-<scope>` per work item.
- **Mid-milestone (day 6)**: review against acceptance criteria; trigger the cut list if behind.
- **Last day**: demo run from the runbook, PROGRESS evidence filled, ADRs updated, release tag, short "what I learned / what I can claim" note in PROGRESS.
- **Gate**: M2 starts after M1's 🔺 restart test passes; M3 starts after M2's 🔺 CDC and tenant isolation suites pass (Charter §1).

## 7. Out of Scope for v1
Kinesis implementation (ADR comparison only), managed Kafka (MSK), Reverse ETL, live dashboards, MWAA, Kubernetes, Flink, multi-cloud deployment, real-vendor TCO comparisons.
