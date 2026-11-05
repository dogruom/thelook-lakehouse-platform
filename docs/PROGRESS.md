# Project Implementation & Sprint Progress Tracker

> **Document Version**: 8.0  
> **Last Updated**: 2026-10-20  
> **Status Conventions**: ✅ Verified | 🔄 In Progress | ⏳ Not Started | ❌ Blocked  
> **Scope Tiers**: 🟩 Core (MVP vertical slice) | 🟦 Extended | 🟨 Stretch — see [PROJECT_CHARTER.md §1](PROJECT_CHARTER.md)  
> **Active Milestone**: Week 1 (Days 1–6) — Multi-Tenant Data Generator & Streaming Ingestion

> [!IMPORTANT]
> **Evidence rule**: A task may only be marked ✅ when its *Evidence* line references a commit hash **and** the output of its verification command (or a CI run link). ⏳ items are plans, not progress.

> [!NOTE]
> **v8.0 re-plan summary**: CDC ordering switched from timestamps to `_cdc_sequence`; SCD2 ownership moved from dbt snapshots to Spark (single history owner); fixed % performance targets replaced by the benchmark methodology (Charter §8); dynamic per-tenant DAGs replaced by one parametrised DAG with task mapping; GX, churn mart and Databricks moved to 🟨 Stretch; Kinesis removed from v1. Three **🔺 early-priority correctness tests** added: restart/replay (W2.D8.2), CDC correctness (W2.D10.3), tenant isolation (W2.D12.3).

---

## 📊 6-Week Macro Status

```
[████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░] 6% Complete
• Week 1: In Progress 🔄 (Day 1 complete; Day 2–6 pending)    🟩 Core
• Week 2: Not Started ⏳                                       🟩 Core
• Week 3: Not Started ⏳                                       🟩 Core
• Week 4: Not Started ⏳                                       🟦 Extended (Day 21 🟨)
• Week 5: Not Started ⏳                                       🟦 Extended (Day 28 GX 🟨)
• Week 6: Not Started ⏳                                       🟦 Extended (Days 33–34 🟨)
```

**Tier gate**: Week 4 starts only after the Week 2–3 correctness tests (🔺) pass.

---

## Week 1: Multi-Tenant Data Generator & Streaming Ingestion 🟩

### Day 1 — Project Scaffolding & Configuration Infrastructure ✅

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W1.D1.1 | Docker Compose health checks (Kafka KRaft, SeaweedFS, Spark, Postgres) | ✅ Done | `docker-compose.yml` (14KB, 7 services + 3 init containers) | All 7 services reach `healthy` status within 90s; `smoke_test.py` passes 15/15 checks | `docker compose up -d && python scripts/smoke_test.py` |
| W1.D1.2 | Implement `src/generator/config.py` using `pydantic-settings` | ✅ Done | `src/generator/config.py` (75 lines, 25 configurable fields) | `GeneratorConfig()` instantiates with defaults; env var override `GEN_BATCH_SIZE=1000` parsed correctly; all dirty injection rates within `[0, 50]` bounds | `python -c "from src.generator.config import GeneratorConfig; c = GeneratorConfig(); print(c.model_dump())"` |
| W1.D1.3 | Reorganize project documentation and eliminate contradictions | ✅ Done | 5 `.md` files under `docs/` (all v6.0+) | All documents reference consistent architecture; no orphan references to removed entities; version numbers aligned | Manual review — all docs cross-referenced |

**Evidence**: _commit `<hash>`; smoke test output `<link or pasted summary>`; host spec used (CPU / RAM / Docker allocation) — to be filled in._

**Day 1 Retrospective**: Foundation established. Config model validated with Pydantic v2. Docker stack boots and passes the smoke test on the development machine (clean-machine verification pending). Structured logging (`src/utils/logging.py`) in place.

---

### Day 2 — Customer CDC Generator with Mutation Simulation ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W1.D2.1 | Implement `CustomerModel` with lifecycle-valid CDC engine | ⏳ Not Started | `src/generator/models/customer.py` | Fields: `customer_id` (UUID), `brand_id` (from config list), `email` (RFC 5322), `phone`, `city`, `country` (ISO 3166-1 alpha-2), `loyalty_tier` (enum), `signup_date` (≤ today); CDC envelope: `event_id`, `_cdc_action` (`I`/`U`/`D`), `_cdc_sequence` (monotonic per `(brand_id, customer_id)`), `_cdc_ts`; lifecycle valid (first event `I`, `D` terminal); overall action mix ≈ 70% I / 25% U / 5% D | `python -c "from src.generator.models.customer import CustomerModel; c = CustomerModel.generate(brand_id='brand_alpha'); print(c.model_dump_json(indent=2))"` |
| W1.D2.2 | Inject calibrated dirty data + delivery anomalies | ⏳ Not Started | Dirty injection logic in `customer.py`; ground-truth manifest writer | Over 10,000 records: invalid email 3.0% ± 0.5%; null email 2.0% ± 0.5%; future signup date 2.0% ± 0.5%; **out-of-order delivery** (sequence preserved, emission shuffled) 5.0% ± 1.0%; **cross-tenant key collisions** (same `customer_id`/email in two brands) at configured rate; every injected defect recorded in a ground-truth manifest (not in the payload) | `pytest tests/unit/test_customer_model.py -v -k "dirty_data"` |
| W1.D2.3 | Unit test suite for customer model | ⏳ Not Started | `tests/unit/test_customer_model.py` (≥ 15 test cases) | Schema constraints validated; dirty injection rates within tolerance; CDC lifecycle and sequence monotonicity verified; manifest matches injected records exactly; seeded runs are reproducible | `pytest tests/unit/test_customer_model.py -v --tb=short` |

**Day 2 Depends On**: W1.D1.2 (GeneratorConfig must be importable)

**Day 2 Definition of Done**:
- [ ] `customer.py` generates 1,000 records without error
- [ ] Dirty data percentages within tolerance of DATA_CONTRACTS.md spec
- [ ] `_cdc_sequence` strictly increasing per `(brand_id, customer_id)` before shuffling
- [ ] Same seed → identical output (deterministic)
- [ ] All 15+ unit tests pass
- [ ] `ruff check src/generator/models/customer.py` — zero warnings

---

### Day 3 — Orders & Line Items Micro-Batch Generator ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W1.D3.1 | Implement `OrderModel` with financial calculations | ⏳ Not Started | `src/generator/models/order.py` | `gross_amount > 0`; `discount_amount >= 0`; `net_amount = gross_amount - discount_amount` (cross-field validator); `currency` in ISO 4217 set; `order_status` enum validated; `payment_method` enum validated | `python -c "from src.generator.models.order import OrderModel; o = OrderModel.generate(brand_id='brand_alpha', customer_id='test-uuid'); print(o.model_dump_json(indent=2))"` |
| W1.D3.2 | Implement `OrderItemModel` with basket simulation | ⏳ Not Started | `src/generator/models/order_item.py` | `quantity > 0`; `unit_price > 0`; `line_total = quantity * unit_price` (cross-field); FK `order_id` always links to parent; FK `product_id` references seed catalog of the **same brand** | `pytest tests/unit/test_order_model.py -v` |
| W1.D3.3 | Inject order-level anomalies | ⏳ Not Started | Dirty injection logic in `order.py`; manifest entries | Over 10,000 records: orphan FK 3.0% ± 0.5%; duplicate 4.0% ± 0.5%; future timestamp 2.0% ± 0.5%; negative `gross_amount` 1.0% ± 0.5%; **late-arriving parent** (order emitted before its customer's `I` event) at configured rate; all recorded in manifest | `pytest tests/unit/test_order_model.py -v -k "dirty"` |

**Day 3 Depends On**: W1.D2.1 (customer IDs needed as FK references for orders)

**Day 3 Definition of Done**:
- [ ] Orders and items generate correctly linked multi-item baskets (1–5 items per order)
- [ ] `net_amount` cross-field validation catches `gross - discount` mismatches
- [ ] Anomaly injection rates verified within tolerance; manifest complete
- [ ] `ruff check src/generator/models/order.py src/generator/models/order_item.py` — zero warnings

---

### Day 4 — Product Catalog & Schema Evolution Module ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W1.D4.1 | Build multi-brand product catalog seed | ⏳ Not Started | `src/generator/models/product.py` | Generates ≥ 500 unique SKUs; `product_id` format `PRD-XXXXX`; `category` in enum (`Apparel`, `Electronics`, `Home`, `Beauty`); `retail_price > 0`; `cost_price > 0` when present | `python -c "from src.generator.models.product import ProductModel; products = ProductModel.generate_catalog(count=500); print(f'Generated {len(products)} products')"` |
| W1.D4.2 | Implement schema evolution injector | ⏳ Not Started | `src/generator/schema_evolution.py` | Batches 1–9: standard schema (no `preferred_language` column); Batch 10+: 100% of records include `preferred_language: STRING` field; schema change is deterministic and repeatable | `pytest tests/unit/test_schema_evolution.py -v` |
| W1.D4.3 | Multi-tenant S3 micro-batch runner | ⏳ Not Started | `src/generator/batch_runner.py` | Uploads JSON/Parquet files to `s3://lakehouse/landing/{brand_id}/{entity}/{date}/`; writes the batch's ground-truth manifest alongside; creates correct partition structure for ≥ 3 brands; handles S3 connection failures with retry (3 attempts) | `python -m src.generator.batch_runner --batches 3 --batch-size 100 --dry-run` |

**Day 4 Depends On**: W1.D3.1 (order items reference product IDs), W1.D1.2 (S3 config)

**Day 4 Definition of Done**:
- [ ] `batch_runner.py --dry-run` prints correct S3 paths without uploading
- [ ] Batch 11 output contains `preferred_language` column; batch 5 does not
- [ ] Product catalog has realistic price distribution (not all same price)
- [ ] All unit tests pass

---

### Day 5 — Kafka Producers (Clickstream + Customer CDC) ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W1.D5.1 | Build clickstream event model | ⏳ Not Started | `src/generator/models/clickstream.py` | `event_id` (UUIDv4), `event_type` in enum (`page_view`, `search`, `add_to_cart`, `checkout`, `purchase`); `device_category` in enum; `event_timestamp` realistic; `session_id` UUID; funnel logic: `page_view` most frequent (~50%), `purchase` rarest (~5%) | `python -c "from src.generator.models.clickstream import ClickstreamModel; e = ClickstreamModel.generate(brand_id='brand_alpha'); print(e.model_dump_json(indent=2))"` |
| W1.D5.2 | Keyed, idempotent Kafka producer with `confluent-kafka` | ⏳ Not Started | `src/generator/kafka_producer.py` | Publishes to `clickstream_events` (key `{brand_id}:{session_id}`) **and** `customer_cdc` (key `{brand_id}:{customer_id}`); `brand_id` also sent as header; `enable.idempotence=true`; delivery callback logs success/failure; configurable events/second; graceful shutdown via `producer.flush()`; partition key strategy documented (avoids single-tenant hot partition) | `python -m src.generator.kafka_producer --events 5000 --topic clickstream_events` then: `docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic clickstream_events --from-beginning --max-messages 5 --property print.key=true --property print.headers=true` |
| W1.D5.3 | Inject streaming noise (late arrivals, duplicates, malformed) | ⏳ Not Started | Dirty injection logic in `clickstream.py` and `kafka_producer.py`; manifest entries | Late arrival 10.0% ± 1.0% (timestamps 10–60 min behind); duplicate events (same `event_id` re-sent) 4.0% ± 0.5%; null session 1.0% ± 0.3%; malformed JSON payloads at configured rate (for Bronze `_parse_error` path) | `pytest tests/unit/test_clickstream_model.py -v -k "dirty"` |

**Day 5 Depends On**: W1.D1.2 (Kafka config), W1.D2.1 (customer IDs and CDC events)

**Day 5 Definition of Done**:
- [ ] 5,000 clickstream events and ≥ 1,000 CDC events produced and consumed back
- [ ] Messages show composite keys and `brand_id` header
- [ ] Per-customer CDC events land in the same partition (ordering preserved per key)
- [ ] Event type distribution follows realistic funnel (page_view > search > add_to_cart > checkout > purchase)
- [ ] Kafka consumer lag returns to 0 after producer completes

---

### Day 6 — Unit Test Suite & Sprint 1 Consolidation ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W1.D6.1 | Comprehensive unit test suite across all generator models | ⏳ Not Started | `tests/unit/test_customer_model.py`, `tests/unit/test_order_model.py`, `tests/unit/test_product_model.py`, `tests/unit/test_clickstream_model.py`, `tests/unit/test_config.py` | ≥ 50 total test cases; line coverage ≥ 80% on `src/generator/` (hygiene floor, not a quality claim); all tests execute offline (no Docker/Kafka/S3 required); mocked S3 via `moto[s3]` | `pytest tests/unit/ -v --cov=src/generator --cov-report=term-missing` |
| W1.D6.2 | Code quality enforcement and PR merge | ⏳ Not Started | Clean `ruff` and `mypy` output; updated `docs/PROGRESS.md` | Zero `ruff` warnings; zero `mypy` errors on `src/generator/`; PROGRESS.md updated with Day 1–6 actuals **and evidence lines**; PR `feature/generator-streaming` created | `ruff check src/ tests/ && ruff format --check src/ tests/ && mypy src/generator/` |

**Day 6 Depends On**: W1.D2–D5 (all models must be complete)

**Day 6 Definition of Done**:
- [ ] `pytest tests/unit/ -v --cov=src/generator` → coverage ≥ 80%
- [ ] `ruff check . && ruff format --check .` → zero errors
- [ ] `mypy src/generator/` → zero errors
- [ ] Git branch `feature/generator-streaming` committed with conventional commit messages
- [ ] PROGRESS.md Week 1 section fully updated with actuals and evidence

---

## Week 2: Lakehouse Engine: Bronze & Silver Processing 🟩

### Day 7 — Bronze Batch Ingestion Engine (PySpark + Delta) ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W2.D7.1 | PySpark batch reader for S3 landing files | ⏳ Not Started | `src/streaming/bronze_batch_ingest.py` | Reads JSON/Parquet from `s3a://lakehouse/landing/{brand_id}/{entity}/`; enriches with `_ingest_ts`, `_source_ref`, `_batch_id`, `brand_id`; malformed rows kept raw with `_parse_error` (not dropped); handles schema evolution with `mergeSchema=true`; storage endpoint/credentials come from config (environment adapter) | `spark-submit src/streaming/bronze_batch_ingest.py --entity orders --brand brand_alpha` then verify: `spark.read.format("delta").load("s3a://lakehouse/bronze/raw_orders").count()` |
| W2.D7.2 | Delta Lake Bronze tables with initial partitioning | ⏳ Not Started | Delta tables at `s3a://lakehouse/bronze/raw_*` | Partitioning per Charter §4 (initial, not final); append-only in normal processing; Delta transaction log present; row count = landing rows (incl. `_parse_error` rows); file counts per table recorded as input for ADR-0005 | `SELECT brand_id, COUNT(*) FROM bronze.raw_orders GROUP BY brand_id` |

**Day 7 Depends On**: W1.D4.3 (batch_runner must have produced landing files in S3)

**Day 7 Definition of Done**:
- [ ] Bronze Delta tables exist for batch entities (orders, order_items, products)
- [ ] Metadata columns (`_ingest_ts`, `_source_ref`, `_batch_id`, `brand_id`) present in all tables
- [ ] Malformed landing rows are present in Bronze with `_parse_error`
- [ ] Re-running ingestion of a *new* batch appends without overwriting existing data

---

### Day 8 — Kafka to Bronze Structured Streaming ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W2.D8.1 | Spark Structured Streaming Kafka consumer | ⏳ Not Started | `src/streaming/bronze_stream_ingest.py` | Consumes `clickstream_events` and `customer_cdc`; stores raw value plus `_source_ref` (topic, partition, offset), key and headers; parses JSON into typed columns, unparseable payloads kept with `_parse_error`; writes Bronze Delta with `Trigger.availableNow=True` | `spark-submit src/streaming/bronze_stream_ingest.py` then verify: `spark.read.format("delta").load("s3a://lakehouse/bronze/raw_clickstream").count()` |
| W2.D8.2 | 🔺 Restart & replay test (early priority) | ⏳ Not Started | `tests/integration/test_stream_restart.py`; results in Day 8 notes | **(a) Restart with checkpoint**: kill job mid-stream → restart → each `(topic, partition, offset)` appears exactly once in Bronze. **(b) Checkpoint loss**: delete checkpoint → replay from earliest → offset re-reads appear in Bronze (expected, documented); Silver idempotency against this case re-verified in W2.D12.2. Delivery semantics recorded for ADR-0006 | `pytest tests/integration/test_stream_restart.py -v` |

**Day 8 Depends On**: W1.D5.2 (Kafka producer must have published events)

---

### Day 9 — Dead Letter Queue (DLQ) Quarantine & Replay ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W2.D9.1 | Validation gate + DLQ router with standardized error envelope | ⏳ Not Started | `src/quality/dlq_router.py` | Runs at Bronze → Silver; evaluates email regex, FK existence, timestamp bounds, price positivity, `_parse_error`; routes failures to `dead_letter.dlq_*` with `_dlq_id`, `_bronze_ref`, `_error_category`, `_rejection_reason`, `_retryable`, `_replay_status`, `_replay_count` | `pytest tests/unit/test_dlq_router.py -v` |
| W2.D9.2 | DLQ population reconciled against manifest | ⏳ Not Started | Delta tables at `s3a://lakehouse/dead_letter/dlq_*` | DLQ contains only invalid records; the set of DLQ'd records **exactly matches** the manifest's validation-failing defects (duplicates and late events are *not* DLQ material); clean records pass through to Silver | `pytest tests/integration/test_dlq_reconciliation.py -v` |
| W2.D9.3 | Controlled DLQ replay | ⏳ Not Started | `scripts/replay_dlq.py` | Replays a filtered DLQ subset (by table / error category / retryable); original `event_id` and `_bronze_ref` preserved; `_replay_status` / `_replay_count` updated; replaying the same subset twice creates **no** Silver duplicates | `python scripts/replay_dlq.py --table dlq_orders --category REFERENTIAL_INTEGRITY_VIOLATION && pytest tests/integration/test_dlq_replay.py -v` |

**Day 9 Depends On**: W2.D7.1 (Bronze tables must exist with dirty data)

---

### Day 10 — Silver Customer Reconciliation (Sequence-Ordered CDC MERGE) ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W2.D10.1 | Delta Lake MERGE INTO for customer CDC | ⏳ Not Started | `src/streaming/silver_customers.py` | `foreachBatch`: batch-local dedup to latest `_cdc_sequence` per `(brand_id, customer_id)`; MERGE applies only when `source._cdc_sequence > target._cdc_sequence`; `I`/`U` → upsert, `D` → tombstone; `email_token = HMAC-SHA256(key, lower(trim(email)))` with key from env var `PII_HMAC_KEY` (never committed); phone masked to last 4 digits | `spark-submit src/streaming/silver_customers.py` |
| W2.D10.2 | Delete / erasure behaviour | ⏳ Not Started | Verified Silver Delta table | After `D`: `is_deleted = true`; `email_token`, phone and other PII set to `NULL`; row kept as tombstone; no raw PII anywhere in Silver | `spark.sql("SELECT * FROM silver.customers WHERE is_deleted = true").show()` |
| W2.D10.3 | 🔺 CDC correctness test suite (early priority) | ⏳ Not Started | `tests/integration/test_cdc_correctness.py` | Scenarios, each asserting the expected final Silver state: (1) in-order `I→U→U`; (2) out-of-order `U` with lower sequence arrives after newer → ignored; (3) `D` then late lower-sequence `U` → stays deleted; (4) duplicate CDC event re-delivered → no-op; (5) multiple events for one key in the same micro-batch; (6) same `customer_id` in two brands → independent states | `pytest tests/integration/test_cdc_correctness.py -v` |

**Day 10 Depends On**: W2.D9.1 (validation gate filters invalid records before Silver merge)

**Day 10 Definition of Done**:
- [ ] Updating customer city from Istanbul to Berlin updates in place (no duplicate keys)
- [ ] Delete action produces a tombstone with PII nulled
- [ ] `_cdc_sequence` prevents stale events from overwriting newer state (all 6 scenarios pass)

---

### Day 11 — Silver Orders, Items & Clickstream Deduplication ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W2.D11.1 | Silver orders & items with referential integrity | ⏳ Not Started | `src/streaming/silver_orders.py`, `src/streaming/silver_order_items.py` | Dedup on `(brand_id, order_id)` (latest `_ingest_ts`) via idempotent MERGE; anti-join against `silver.customers` on `(brand_id, customer_id)`; orphans routed to DLQ with `_retryable = true` (parent may arrive late) and recovered via W2.D9.3 replay | `SELECT brand_id, order_id, COUNT(*) c FROM silver.orders GROUP BY 1,2 HAVING c > 1` → must return 0 rows |
| W2.D11.2 | Silver clickstream deduplication | ⏳ Not Started | `src/streaming/silver_clickstream.py` | `foreachBatch`: in-batch dedup on `(brand_id, event_id)` + insert-only MERGE into Silver bounded by an `event_date` lookback window (configurable, e.g. 2 days); events beyond the window are **counted** in a `late_beyond_window` metric and remain in Bronze (not lost); partitioning per Charter §4 | Silver has 0 duplicates on `(brand_id, event_id)`; `count(silver) = count(distinct keys in Bronze within window)` |

**Day 11 Depends On**: W2.D10.1 (Silver customers must exist for FK validation)

---

### Day 12 — Idempotency, Reconciliation & Tenant Isolation ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W2.D12.1 | End-to-end integration test with reconciliation | ⏳ Not Started | `tests/integration/test_bronze_silver.py` | landing/Kafka → Bronze → DLQ → Silver in one test; per batch: `input = bronze` and `bronze = silver_accepted + dlq + duplicates_removed + late_beyond_window`, checked against the manifest | `pytest tests/integration/test_bronze_silver.py -v --timeout=300` |
| W2.D12.2 | Crash simulation & idempotent re-run | ⏳ Not Started | Test report in Day 12 notes | Kill Spark worker mid-batch → restart → re-run; also replay after checkpoint loss (from W2.D8.2b). Silver contains zero duplicate keys and identical content hash to a clean run; Delta log contains no partial commit (uncommitted files are not referenced) | `pytest tests/integration/test_idempotent_rerun.py -v` |
| W2.D12.3 | 🔺 Tenant isolation test (early priority) | ⏳ Not Started | `tests/integration/test_tenant_isolation.py` | Uses the generator's cross-tenant key collisions. (a) Per-tenant row counts in Silver match the manifest; (b) every Silver join keys on `brand_id` — no row attributes data of brand A to brand B; (c) **negative test**: a deliberately leaky join (without `brand_id`) in a fixture **must fail** the isolation assertion, proving the test detects leaks; (d) tenant-scoped view returns only its own brand | `pytest tests/integration/test_tenant_isolation.py -v` |

**Day 12 Definition of Done**:
- [ ] Integration test passes end-to-end in Docker
- [ ] Re-running the entire pipeline produces identical Silver output (idempotent)
- [ ] Tenant isolation suite passes, and the negative test fails as intended
- [ ] PR `feature/lakehouse-engine` merged (CI integration job added in W5.D30.1)

---

## Week 3: dbt Core Gold Layer & SCD2 History 🟩

### Day 13 — dbt Core Environment & Adapter Capability Spike ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W3.D13.1 | Configure dbt-spark connection | ⏳ Not Started | `dbt_project/profiles.yml` | `dbt debug` passes with Spark Thrift connection; catalog lists Silver schema; connection timeout < 10s | `cd dbt_project && dbt debug` |
| W3.D13.2 | Establish dbt directory taxonomy | ⏳ Not Started | `models/staging/`, `models/intermediate/`, `models/marts/` | Matches ENGINEERING_STANDARDS.md §3.1; `dbt_project.yml` sets materialization defaults per directory | `cd dbt_project && dbt ls` |
| W3.D13.3 | Adapter capability spike | ⏳ Not Started | Results table in ADR-0004 | Verified on the actual versions (dbt-spark + Thrift + Delta + metastore): incremental `merge` strategy, model contracts / constraints enforcement, `persist_docs`, composite `unique_key`. Fallback documented if Thrift is unstable (dbt-spark `session` method) | Minimal test models per feature → `dbt build` |

**Day 13 Depends On**: W2.D11.1 (Silver tables must be populated for dbt to query)

---

### Day 14 — Staging Views & Schema Contracts ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W3.D14.1 | Build staging view wrappers over Silver tables | ⏳ Not Started | `stg_customers.sql`, `stg_orders.sql`, `stg_order_items.sql`, `stg_products.sql`, `stg_clickstream.sql` | 1:1 SELECT with renamed/cleaned columns; `contract: {enforced: true}` where supported (per W3.D13.3); ISO currency standardization; no raw PII columns exposed | `cd dbt_project && dbt run --select staging` |
| W3.D14.2 | YAML contract violation test | ⏳ Not Started | `schema.yml` with enforced contracts | Intentionally altering a column type causes `dbt run` to fail with a contract error (or documented alternative test if adapter lacks support) | Change a column type → verify failure → revert |

---

### Day 15 — SCD Type 2 Customer History (Spark-Owned) ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W3.D15.1 | Build `silver.customers_history` from the CDC log | ⏳ Not Started | `src/streaming/silver_customers_history.py` | Tracked attributes `city`, `country`, `loyalty_tier`; versions ordered by `_cdc_sequence`; `valid_from = _cdc_ts` of the version, `valid_to` = next version's `_cdc_ts` (NULL if current), `is_current`; `D` closes the current version; out-of-order arrivals trigger a deterministic rebuild of the affected keys' history; rules documented in ADR-0004 | `spark-submit src/streaming/silver_customers_history.py` |
| W3.D15.2 | SCD2 scenario tests | ⏳ Not Started | `tests/integration/test_scd2_history.py` | (1) Istanbul → Berlin produces 2 versions (expired Istanbul, current Berlin); (2) the same events delivered out of order produce an **identical** history; (3) two changes with equal `_cdc_ts` are ordered by `_cdc_sequence`; (4) re-running produces no extra versions | `pytest tests/integration/test_scd2_history.py -v` |

**Day 15 Depends On**: W2.D10.1 (sequence-ordered CDC in Silver)

> No dbt snapshots on customers: SCD2 has a single owner (Charter invariant 6).

---

### Day 16 — Conformed Dimensions ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W3.D16.1 | Build `dim_customers` and `dim_customers_history` | ⏳ Not Started | `models/marts/dim_customers.sql`, `models/marts/dim_customers_history.sql` | Read `silver.customers` / `silver.customers_history`; use `email_token` and masked phone from Silver (no hashing in dbt); surrogate key includes `brand_id`; no raw PII in output | `dbt run --select dim_customers dim_customers_history` |
| W3.D16.2 | Build `dim_products` and `dim_date` | ⏳ Not Started | `models/marts/dim_products.sql`, `models/marts/dim_date.sql` | Products include pricing tier (`budget`/`mid`/`premium`/`luxury`); date dimension covers 2024-01-01 to 2027-12-31 via date-spine macro | `dbt run --select dim_products dim_date` |

---

### Day 17 — Fact Tables (Orders & Clickstream) ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W3.D17.1 | Build `fct_orders` and `fct_clickstream` | ⏳ Not Started | `models/marts/fct_orders.sql`, `models/marts/fct_clickstream.sql` | `fct_orders` joins orders + line items + dims on `(brand_id, key)`; revenue calculations correct; `fct_clickstream` windows clicks into sessions | `dbt run --select fct_orders fct_clickstream` |
| W3.D17.2 | dbt test suite with 100% pass rate | ⏳ Not Started | `schema.yml` tests for all marts | `unique` / `not_null` on keys; **composite** `(brand_id, key)` relationship tests (custom generic test); singular tests `assert_positive_revenue.sql` and `assert_no_cross_tenant_joins.sql` | `dbt test --select marts` |

**Day 17 Definition of Done**:
- [ ] All mart models compile and run without errors
- [ ] `dbt test --select marts` → 100% passing
- [ ] No orphan keys in fact tables (composite referential integrity verified)

---

### Day 18 — Documentation, Lineage & Vertical-Slice Milestone ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W3.D18.1 | Generate dbt documentation with lineage graph | ⏳ Not Started | `dbt_project/target/` | Every model and column described in `schema.yml`; lineage shows staging → intermediate → marts | `dbt docs generate && dbt docs serve --port 8081` |
| W3.D18.2 | 🟩 Core milestone: end-to-end vertical slice | ⏳ Not Started | `make e2e` (or script) + PR `feature/dbt-analytics` | One command runs Generator → Kafka → Bronze → Silver → Gold → tests; all 🔺 tests green; README status table updated **with evidence links** | `dbt build && pytest tests/integration/ -v` |

---

## Week 4: Business Marts, Customer 360 & PySpark Benchmarks 🟦

### Day 19 — Customer 360 Denormalized Mart (OBT) ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W4.D19.1 | Build `obt_customer_360` One Big Table | ⏳ Not Started | `models/marts/obt_customer_360.sql` | Combines customer attributes + order aggregations + clickstream engagement; pre-calculates `tenure_days`, `total_lifetime_orders`, `total_lifetime_spend`, `avg_order_value`, `total_page_views`, `total_cart_additions`; grain `(brand_id, customer_id)` | `dbt run --select obt_customer_360` |
| W4.D19.2 | OBT query benchmark | ⏳ Not Started | Section in `docs/benchmarks.md` | Representative queries (top customers by spend, segment distribution) measured per Charter §8 (OBT vs. star-schema joins); p50/p95 reported, no pre-set target | Benchmark script in `scripts/benchmarks/` |

---

### Day 20 — RFM Segmentation & Customer Scoring ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W4.D20.1 | Implement RFM segmentation model | ⏳ Not Started | `models/marts/agg_rfm_segments.sql` | `NTILE(5)` on recency, frequency, monetary **partitioned by `brand_id`**; composite `rfm_score` (e.g. `5-5-4`); segments: Champions, Loyal, At-Risk, Hibernating, Lost | `dbt run --select agg_rfm_segments` |
| W4.D20.2 | Validate segment distributions | ⏳ Not Started | Distribution analysis query | No segment is 0% or 100% per brand; distribution documented (synthetic data — no claim of real-world Pareto behaviour) | `SELECT brand_id, rfm_segment, COUNT(*) FROM agg_rfm_segments GROUP BY 1,2` |

---

### Day 21 — ML Feature Mart: Churn Risk 🟨 Stretch ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W4.D21.1 | Build churn risk feature table | ⏳ Not Started | `models/marts/agg_churn_risk_features.sql` | Features: days_since_last_session, return_rate, frequency_deceleration, recency_days; normalized heuristic `churn_risk_score` in [0, 1] (rule-based, not a trained model); category LOW/MEDIUM/HIGH_RISK | `dbt run --select agg_churn_risk_features` |
| W4.D21.2 | Feature distribution validation | ⏳ Not Started | dbt tests | No NULLs; values bounded [0, 1]; non-degenerate distribution | `dbt test --select agg_churn_risk_features` |

---

### Day 22 — PySpark Profiling & Skew Mitigation ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W4.D22.1 | Simulate extreme tenant data skew | ⏳ Not Started | Skewed dataset (80% `brand_alpha`, fixed seed) | Baseline join measured with AQE disabled and with AQE skew-join enabled (Spark 3.5 default); task-duration distribution captured from Spark UI | Spark UI at `localhost:8181` + benchmark script |
| W4.D22.2 | Implement salted key join | ⏳ Not Started | `src/streaming/optimize_tables.py` (salted join) | Salting compared against **both** baselines; metrics per Charter §8 (max/median task time, shuffle bytes, p50/p95 runtime over 5 runs); conclusion states when salting is worth it vs. AQE — reported even if no improvement | `df.explain(True)` for each variant + benchmark script |

---

### Day 23 — Compaction, Z-Order & Broadcast Joins ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W4.D23.1 | Implement OPTIMIZE + ZORDER BY | ⏳ Not Started | Z-Order in `optimize_tables.py` | `OPTIMIZE silver.orders ZORDER BY (brand_id, order_timestamp)` on Delta OSS 3.2 (feature support verified for this version); before/after file count, files scanned and selective-query runtime recorded per Charter §8 — no pre-set % target | `DESCRIBE DETAIL silver.orders` before/after |
| W4.D23.2 | Implement broadcast hash joins | ⏳ Not Started | Broadcast join patterns | `broadcast(dim_products)` eliminates shuffle exchange; plan shows `BroadcastHashJoin` instead of `SortMergeJoin`; runtime difference measured | `df.explain(True)` |

---

### Day 24 — Benchmark Report Publication ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W4.D24.1 | Author empirical benchmark report | ⏳ Not Started | `docs/benchmarks.md` | Methodology section (dataset, seed, machine spec, Spark config, runs); before/after tables for AQE vs. salting, broadcast join, Z-Order; Spark UI screenshots; explicit caveat that local scale ≠ production scale | Review for completeness |
| W4.D24.2 | Sprint 4 milestone sign-off | ⏳ Not Started | PR `feature/marts-performance` | All dbt marts build; benchmark report complete; optimization code reviewed | `dbt build && pytest tests/` |

---

## Week 5: Production Orchestration, Quality & CI/CD 🟦

### Day 25 — Airflow Environment Configuration ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W5.D25.1 | Configure Airflow connections | ⏳ Not Started | Connections: `spark_default`, `s3_conn` | Webserver starts without warnings; connections pass test; scheduler heartbeat visible; secrets via env vars, not committed | `docker compose logs airflow-webserver --tail=20` → zero ERROR lines |
| W5.D25.2 | Structure DAGs repository | ⏳ Not Started | `dags/` with modular imports | No import errors; custom operators in `dags/operators/` | Airflow UI → DAGs page without import errors |

---

### Day 26 — Parametrised Multi-Tenant Batch DAG ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W5.D26.1 | Single DAG with dynamic task mapping | ⏳ Not Started | `dags/ecommerce_batch_pipeline.py` | One DAG; per-tenant tasks via `.expand(brand_id=<config list>)` (no per-tenant DAG generation); sequence `landing_sensor >> bronze_ingest >> validation_gate >> silver_merge`; retries=2, retry_delay=5min, execution_timeout=30min | Airflow UI → trigger → all mapped tasks green |
| W5.D26.2 | Tenant failure isolation | ⏳ Not Started | Test: fail one brand, others succeed | Injected failure in `brand_beta` mapped task; `brand_alpha` and `brand_gamma` complete; downstream Gold waits / runs per trigger rule as documented | Airflow UI → verify partial success |

---

### Day 27 — Streaming Micro-Batch Orchestration & Alerts ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W5.D27.1 | Streaming micro-batch DAG | ⏳ Not Started | `dags/clickstream_streaming_dag.py` | Airflow triggers Spark jobs with `Trigger.availableNow` on a schedule (e.g. `*/15 * * * *`); Spark owns checkpoints and stream lifecycle; Airflow does not host long-running streams; Kafka consumer-lag check reported | Scheduler triggers DAG; Spark job completes |
| W5.D27.2 | Alert webhook on failure | ⏳ Not Started | Webhook payload | On task failure or DLQ-rate threshold breach: JSON payload (Slack format) with DAG, task, run, error | Deliberate failure → payload in logs |

---

### Day 28 — Quality Gates (dbt tests + DLQ thresholds; GX 🟨 Stretch) ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W5.D28.1 | Quality gate tasks in Airflow | ⏳ Not Started | Gate tasks in DAGs | Post-Silver gate: reconciliation check + DLQ-rate threshold per brand; post-Gold gate: `dbt test`; deliberate corruption halts downstream tasks and fires alert | Airflow DAG → corrupt batch → downstream skipped + alert |
| W5.D28.2 | 🟨 Great Expectations checkpoint (optional) | ⏳ Not Started | `src/quality/expectations/` | Silver suite (value ranges, regex, distribution bounds) run from Airflow; HTML data docs generated; no overlap with dbt tests (documented split) | `great_expectations checkpoint run silver_checkpoint` |

---

### Day 29 — Idempotent Backfill & Failure Recovery ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W5.D29.1 | Historical backfill script | ⏳ Not Started | `scripts/backfill_pipeline.py` | `--start-date` / `--end-date`; seeded generation of 30 days; re-running produces identical Silver/Gold content hashes | `python scripts/backfill_pipeline.py --start-date 2026-09-01 --end-date 2026-09-30` (run twice, compare hashes) |
| W5.D29.2 | Orchestrated recovery under failure | ⏳ Not Started | Documented test results | Kill Spark worker mid-DAG-run → Airflow retry completes without manual intervention; zero duplicate keys (re-uses W2.D12.2 assertions) | `docker kill spark-worker && docker compose up -d spark-worker` → verify |

---

### Day 30 — GitHub Actions CI/CD Pipeline ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W5.D30.1 | Build CI workflow (two jobs) | ⏳ Not Started | `.github/workflows/ci.yml` | **Job 1 (every push/PR)**: `ruff check`, `ruff format --check`, `mypy src/`, `pytest tests/unit/ --cov=src`, `dbt compile`. **Job 2 (PRs to `main` + nightly)**: Docker Compose integration profile running the 🔺 correctness suites. README badge added only after first green run | Push test branch → both jobs green |
| W5.D30.2 | Pre-commit hooks | ⏳ Not Started | `.pre-commit-config.yaml` | ruff lint, ruff format, trailing whitespace, YAML check, secret scan (e.g. `detect-secrets`) | `pre-commit run --all-files` |

**Week 5 Definition of Done**:
- [ ] Airflow DAGs execute without errors
- [ ] Quality gates halt downstream processing on corrupted input
- [ ] Backfill is deterministic (identical hashes on re-run)
- [ ] Both CI jobs green; run linked in evidence
- [ ] PR `feature/orchestration-quality` merged

---

## Week 6: Cloud Deployment Proof & Showcase 🟦 (Databricks 🟨)

### Day 31 — Infrastructure as Code (Terraform on AWS) ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W6.D31.1 | Terraform modules for AWS S3 and IAM | ⏳ Not Started | `infra/terraform/s3_buckets.tf`, `iam.tf`, `variables.tf` | Minimal resource set: buckets `raw-landing`, `lakehouse` (bronze/silver/gold/checkpoints prefixes) with lifecycle rules and SSE; least-privilege IAM role for the Spark job; one **prefix-scoped tenant policy** as isolation demonstration; budget alarm | `terraform init && terraform validate && terraform plan` |
| W6.D31.2 | Terraform validation and linting | ⏳ Not Started | Clean `tflint` / `terraform fmt` | Zero `tflint` warnings; `terraform fmt -check` passes | `terraform fmt -check && tflint` |

---

### Day 32 — Terraform Apply & Spark Job on AWS S3 ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W6.D32.1 | Provision live AWS infrastructure | ⏳ Not Started | Running S3 buckets and IAM roles | `terraform apply` creates all resources; bucket existence and permissions verified | `aws s3 ls` |
| W6.D32.2 | Upload landing data to AWS S3 | ⏳ Not Started | Multi-tenant data in AWS S3 | Generator writes via the AWS storage adapter (config change only); structure matches local | `aws s3 ls s3://raw-landing/landing/ --recursive \| head -20` |
| W6.D32.3 | Run Bronze → Silver job against AWS S3 | ⏳ Not Started | `docs/LOCAL_VS_CLOUD.md` | Same Spark/Delta job code runs from local Spark against AWS S3; row counts reconcile; every config/behaviour difference vs. SeaweedFS recorded; prefix-scoped tenant policy denies cross-tenant read (tested) | `spark-submit --conf <aws profile> src/streaming/bronze_batch_ingest.py ...` |

---

### Day 33 — Databricks Trial & Auto Loader 🟨 Stretch ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W6.D33.1 | Connect Databricks to provisioned S3 | ⏳ Not Started | Databricks workspace with S3 external storage | Storage credentials configured; Databricks lists S3 bucket contents | `dbutils.fs.ls("s3://raw-landing/")` |
| W6.D33.2 | Auto Loader adapter | ⏳ Not Started | `notebooks/databricks_auto_loader.py` | Auto Loader (`cloudFiles`) used as a **separate file-source adapter** feeding the shared Bronze logic; schema evolution handled; differences vs. local file source documented | Run notebook → verify Delta row count |

---

### Day 34 — Databricks Asset Bundles & Teardown 🟨 Stretch ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W6.D34.1 | Deploy Databricks Asset Bundle (DAB) | ⏳ Not Started | `databricks.yml` | `databricks bundle deploy` succeeds; job visible in Jobs UI; pipeline executes end-to-end | `databricks bundle deploy && databricks bundle run` |
| W6.D34.2 | Capture cloud proof and destroy resources | ⏳ Not Started | `docs/cloud_proof/` | Screenshots of job run, Spark execution graph, S3 metrics; **actual spend from AWS/Databricks billing recorded** (not estimated); `terraform destroy` decommissions all AWS resources | `terraform destroy && aws s3 ls` |

> If Days 33–34 are skipped, still run `terraform destroy` after Day 32 and record actual spend.

---

### Day 35 — Portfolio README & Architecture Decision Records ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W6.D35.1 | Finalize GitHub README | ⏳ Not Started | Updated `README.md` | Status table reflects only verified features with evidence links; quickstart verified on a **clean machine** with measured setup time and hardware recorded; no unmeasured performance/cost claims; repository-relative links only | Fresh `git clone` → `docker compose up -d` → `python scripts/smoke_test.py` (timed) |
| W6.D35.2 | Finalize Architecture Decision Records | ⏳ Not Started | 6 ADRs in `docs/adr/` | ADR-0001 SeaweedFS; ADR-0002 Kafka vs. Kinesis + common event envelope; ADR-0003 Append-only Bronze & erasure exception; ADR-0004 Spark/dbt ownership & adapter capabilities; ADR-0005 Tenant isolation & partitioning; ADR-0006 Delivery semantics & replay | Each ADR: Context → Decision → Consequences |

---

### Day 36 — Interview Preparation & Final Release ⏳

| Task ID | Description | Status | Deliverable Artifact | Acceptance Criteria | Verification Command |
|---|---|---|---|---|---|
| W6.D36.1 | System design interview guide | ⏳ Not Started | `docs/INTERVIEW_QA.md` | Covers: delivery semantics per stage, CDC ordering, SCD2 under late data, multi-tenant isolation, AQE vs. salting, Z-Order vs. Liquid Clustering, DLQ replay, local vs. cloud differences | Review for completeness and accuracy |
| W6.D36.2 | Tag official release v1.0.0 | ⏳ Not Started | Git tag `v1.0.0` | `git tag -a v1.0.0`; CI green; README renders correctly on GitHub; all ✅ items have evidence | `git tag -l`; GitHub release page created |

**Week 6 Definition of Done**:
- [ ] Spark job verified on AWS S3; differences documented in `docs/LOCAL_VS_CLOUD.md`
- [ ] AWS resources destroyed; actual spend recorded
- [ ] README renders correctly on GitHub with evidence-backed status
- [ ] 6 ADRs complete
- [ ] Interview prep guide written
- [ ] Release v1.0.0 tagged and published