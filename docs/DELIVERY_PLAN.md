# Delivery Plan: 6-Week "Special Forces" Execution Blueprint

> **Program Schedule**: 6 Weeks × 6 Days/Week × 6 Hours/Day = **216 Hours of Deep Focus**  
> **Methodology**: Agentic Pair-Programming (Antigravity AI + Senior Data Engineer)  
> **Objective**: Build a world-class, multi-tenant real-time CRM lakehouse portfolio that secures Senior Data Engineer offers in the Berlin tech ecosystem.

---

## Program Roadmap at a Glance

| Week | Focus Area | Core Tech Stack | Milestone & Acceptance Gate |
|---|---|---|---|
| **Week 1** | Multi-Tenant Data Generator & Streaming Ingestion | Python 3.11, Pydantic v2, Kafka 3.7 (KRaft), S3 API | Kafka streaming clickstream & CDC events flowing with `brand_id`; dirty data injection calibrated; pytest ≥ 80%. |
| **Week 2** | Lakehouse Engine: Bronze & Silver Processing | PySpark 3.5, Delta Lake OSS 3.2, Structured Streaming | Bronze append-only tables; Silver CDC `MERGE INTO`; quarantine DLQ routing; schema evolution demo. |
| **Week 3** | Analytics Engineering & Historical Modeling | dbt Core, dbt-spark, SCD Type 2 Snapshots, YAML Contracts | Kimball dimensional modeling; SCD2 tracking (Istanbul→Berlin move); strict schema contracts; dbt docs generated. |
| **Week 4** | Customer 360, ML Features & Performance Tuning | Advanced Spark SQL, Window Functions, Z-Order, Broadcast Joins | `obt_customer_360`, RFM segmentation, Churn Risk marts; `docs/benchmarks.md` proving Z-Order & skew optimization. |
| **Week 5** | Production Orchestration, Quality & CI/CD | Apache Airflow 2.9, Docker Compose, Great Expectations, GitHub Actions | Dynamic multi-tenant Airflow DAGs; automated backfill; GX quality checkpoint gates; green GitHub Actions CI. |
| **Week 6** | Cloud Deployment Proof (Terraform + Databricks) & Showcase | Terraform (IaC), AWS S3/IAM, Databricks (14-day trial), Auto Loader | AWS S3 + IAM provisioned via Terraform; Databricks Asset Bundle & Auto Loader executed; Berlin showcase repo finalized. |

---

## WEEK 1: Multi-Tenant Data Generator & Streaming Ingestion

### Daily Breakdown (6 Days × 6 Hours = 36 Hours)

#### Day 1: Project Scaffolding & Configuration Infrastructure (6h)
- **Goal**: Align Python 3.11 environment, Docker networking, and centralized settings.
- **Tasks**:
  - Audit `docker-compose.yml` health checks (Kafka 3.7 KRaft, SeaweedFS S3 gateway, Spark 3.5, PostgreSQL 15).
  - Implement `src/generator/config.py` using `pydantic-settings` with configurable injection rates and multi-tenant parameters.
  - Setup structured logging (`src/utils/logging.py`) with tenant-aware JSON context.
- **Acceptance Test**: `python scripts/smoke_test.py` completes 15/15 checks; `GeneratorConfig` successfully parses CLI and environment variables.

#### Day 2: Customer CDC Generator with Mutation Simulation (6h)
- **Goal**: Build an enterprise customer generator that simulates real transactional database CDC.
- **Tasks**:
  - Implement `src/generator/models/customer.py` supporting natural attributes (`customer_id`, `brand_id`, `loyalty_tier`, etc.).
  - Add CDC mutation engine: emit `I` (Insert), `U` (Update profile/address/tier), and `D` (GDPR Delete).
  - Inject intentional dirty data: 3% corrupt emails, 2% null emails, 2% future-dated signups.
- **Acceptance Test**: Unit test suite `tests/unit/test_customer_model.py` validates schema constraints and dirty data injection percentages within ±0.5% tolerance.

#### Day 3: Orders & Line Items Micro-Batch Generator (6h)
- **Goal**: Generate high-volume financial transactional records across brands.
- **Tasks**:
  - Build `src/generator/models/order.py` and `src/generator/models/order_item.py`.
  - Simulate multi-item shopping baskets with realistic retail pricing and discounts.
  - Inject data anomalies: 3% orphan `customer_id` foreign keys, 4% duplicate orders, 1% negative prices.
- **Acceptance Test**: Verify referential integrity between generated orders and order items; test anomaly detection in unit tests.

#### Day 4: Product Catalog & Schema Evolution Module (6h)
- **Goal**: Implement multi-brand product catalog and dynamic schema drift.
- **Tasks**:
  - Build `src/generator/models/product.py` with seed catalog of 500+ realistic retail SKUs across categories.
  - Implement `src/generator/schema_evolution.py`: after Batch 10, inject `preferred_language: STRING` to simulate upstream schema changes.
  - Write multi-tenant batch runner `src/generator/batch_runner.py` uploading micro-batches to S3 landing paths `landing/{brand_id}/{entity}/`.
- **Acceptance Test**: S3 landing directory shows properly partitioned batches; batches > 10 contain the evolved schema column.

#### Day 5: Kafka Streaming Clickstream Producer (6h)
- **Goal**: Build a resilient, high-throughput Kafka producer for real-time user behavior.
- **Tasks**:
  - Build `src/generator/models/clickstream.py` emitting `page_view`, `search`, `add_to_cart`, `checkout`, `purchase`.
  - Implement `src/generator/kafka_producer.py` using `confluent-kafka` with multi-tenant message keys (`brand_id`).
  - Inject streaming noise: 10% late-arriving events (10–60 min lag) and 4% duplicate event tokens.
- **Acceptance Test**: Stream 5,000 events to Kafka topic `clickstream_events`; verify consumption with `kafka-console-consumer`.

#### Day 6: Unit Test Suite & Sprint 1 Consolidation (6h)
- **Goal**: Achieve ≥ 80% test coverage and ensure reproducible execution.
- **Tasks**:
  - Write comprehensive unit tests in `tests/unit/` covering all generators, models, and producers.
  - Format and lint codebase with `ruff check .` and `ruff format .`.
  - Document Week 1 milestone in `docs/PROGRESS.md`.
- **Acceptance Test**: `pytest tests/unit/ -v --cov=src` reports ≥ 80% coverage; GitHub PR `feature/generator-streaming` approved.

---

## WEEK 2: Lakehouse Engine: Bronze & Silver Processing

### Daily Breakdown (6 Days × 6 Hours = 36 Hours)

#### Day 7: Bronze Batch Ingestion Engine (PySpark + Delta) (6h)
- **Goal**: Ingest raw landing files into immutable, append-only Bronze Delta tables.
- **Tasks**:
  - Build `src/streaming/bronze_batch_ingest.py` to ingest orders, order_items, products, and customer snapshots.
  - Enrich incoming records with metadata: `_ingest_ts`, `_source_file`, `_batch_id`, `_brand_id`.
  - Configure `.option("mergeSchema", "true")` to accommodate schema evolution without pipeline failure.
- **Acceptance Test**: Spark batch job creates Delta tables under `s3a://lakehouse/bronze/` with verified row counts and transaction logs.

#### Day 8: Kafka to Bronze Structured Streaming (6h)
- **Goal**: Stream Kafka clickstream and CDC events into Bronze Delta tables.
- **Tasks**:
  - Build `src/streaming/bronze_stream_ingest.py` using Spark Structured Streaming.
  - Configure `Trigger.availableNow=True` (production cost guard) and checkpoint directories under `s3a://lakehouse/checkpoints/`.
  - Extract tenant metadata and unpack JSON payloads into typed Spark DataFrames.
- **Acceptance Test**: Kafka stream processes 10,000 events into `bronze.raw_clickstream`; checkpoint recovery tested via container restart.

#### Day 9: Dead Letter Queue (DLQ) Quarantine Architecture (6h)
- **Goal**: Ensure zero silent data loss by isolating corrupted and invalid records.
- **Tasks**:
  - Implement `src/quality/dlq_router.py` to evaluate validation rules against Bronze records.
  - Route malformed emails, orphan foreign keys, future dates, and negative prices into `dead_letter.dlq_*` Delta tables.
  - Standardize error envelope schema: `_dlq_id`, `_brand_id`, `_error_category`, `_rejection_reason`, `_original_payload`.
- **Acceptance Test**: Corrupted records generated in Week 1 land in `dead_letter.dlq_orders` and `dead_letter.dlq_customers`; clean records pass to Silver.

#### Day 10: Silver Customer Reconciliation (CDC MERGE INTO) (6h)
- **Goal**: Reconcile customer profile updates and handle GDPR soft deletions.
- **Tasks**:
  - Build `src/streaming/silver_customers.py` implementing Delta `MERGE INTO`.
  - Handle `I` (Insert), `U` (Update profile/tier), and `D` (GDPR soft-delete: `is_deleted = true`, email hashed).
  - Apply deterministic merge conditions utilizing `_cdc_timestamp`.
- **Acceptance Test**: Updating customer city from Istanbul to Berlin updates the active record in `silver.customers` without duplicating primary keys.

#### Day 11: Silver Orders, Items & Clickstream Deduplication (6h)
- **Goal**: Build deduplicated, validated Silver transaction and interaction tables.
- **Tasks**:
  - Build `src/streaming/silver_orders.py` and `src/streaming/silver_order_items.py` with anti-join foreign key checks against `silver.customers`.
  - Build `src/streaming/silver_clickstream.py` with watermarking to deduplicate late-arriving interaction events.
  - Enforce partition strategy: `PARTITIONED BY (brand_id)`.
- **Acceptance Test**: Re-running ingestion pipeline against duplicate source data yields zero duplicate rows in Silver.

#### Day 12: Idempotent Re-Run Testing & Sprint 2 Integration (6h)
- **Goal**: Verify complete pipeline re-entrancy and idempotency under failure conditions.
- **Tasks**:
  - Write `tests/integration/test_bronze_silver.py` verifying Bronze-to-Silver data flow.
  - Perform crash simulation: interrupt Spark job mid-batch and execute retry; verify no partial duplicate records exist.
  - Update progress tracking in `docs/PROGRESS.md`.
- **Acceptance Test**: Full end-to-end integration test passes in local Docker; PR `feature/lakehouse-engine` merged.

---

## WEEK 3: Modern Analytics Engineering: dbt Core & SCD Type 2

### Daily Breakdown (6 Days × 6 Hours = 36 Hours)

#### Day 13: dbt Core Environment & Spark Thrift Integration (6h)
- **Goal**: Connect dbt-core to Spark Delta Lake environment.
- **Tasks**:
  - Configure `dbt_project/profiles.yml` with `dbt-spark` thrift connection targeting Spark master.
  - Validate dbt connection with `dbt debug`.
  - Establish directory taxonomy: `models/staging/`, `models/intermediate/`, `models/marts/`.
- **Acceptance Test**: `dbt debug` passes with valid connection to Spark SQL Thrift server; catalog schema recognized.

#### Day 14: Staging Views & Schema Contracts (6h)
- **Goal**: Build lightweight, 1:1 view wrappers over Silver tables with enforced contracts.
- **Tasks**:
  - Create `stg_customers.sql`, `stg_orders.sql`, `stg_order_items.sql`, `stg_products.sql`, and `stg_clickstream.sql`.
  - Configure `contract: {enforced: true}` in `schema.yml` to prevent unexpected column type alterations.
  - Apply initial field cleaning and standardization (e.g. lowercase emails, ISO currency conforming).
- **Acceptance Test**: `dbt run --select staging` compiles and runs successfully; schema contract violations raise compilation errors.

#### Day 15: Slowly Changing Dimensions (SCD Type 2 Snapshots) (6h)
- **Goal**: Track customer attribute evolution over time without destroying history.
- **Tasks**:
  - Create dbt snapshot `snapshots/snap_customers.sql` configured with `check` strategy on `[city, country, loyalty_tier]`.
  - Build test scenario: update a customer's location from Istanbul to Berlin in the generator and run snapshot.
  - Verify `dbt_valid_from`, `dbt_valid_to`, and active record indicator.
- **Acceptance Test**: Customer relocation produces two records: expired historical row (`dbt_valid_to IS NOT NULL`) and current row (`dbt_valid_to IS NULL`).

#### Day 16: Conformed Dimensions & GDPR Masking (6h)
- **Goal**: Construct enterprise dimensions with strict privacy compliance.
- **Tasks**:
  - Build `models/marts/dim_customers.sql` incorporating SCD2 customer state and GDPR email hashing (`SHA256(email)`).
  - Build `models/marts/dim_products.sql` with pricing tiers (`budget`, `mid`, `premium`, `luxury`).
  - Build `models/marts/dim_date.sql` generated via date-spine macro covering historical and future analysis horizons.
- **Acceptance Test**: Querying `dim_customers` proves no plain-text emails exist; dimensions adhere to Kimball star schema standards.

#### Day 17: Fact Tables (Orders & Clickstream Sessions) (6h)
- **Goal**: Model transactional and behavioral fact tables.
- **Tasks**:
  - Build `models/marts/fct_orders.sql` joining orders, line items, and dimensional surrogates with gross/net revenue calculations.
  - Build `models/marts/fct_clickstream.sql` windowing raw clicks into continuous user sessions.
  - Add dbt generic tests: `unique`, `not_null`, and `relationships` verifying foreign key integrity against dimensions.
- **Acceptance Test**: `dbt test --select marts` completes with 100% passing tests; no orphan keys in fact tables.

#### Day 18: Documentation & Lineage DAG Compilation (6h)
- **Goal**: Generate production-grade dbt documentation and visual DAG lineage.
- **Tasks**:
  - Enrich all models and columns in `schema.yml` with comprehensive markdown descriptions.
  - Compile dbt documentation: `dbt docs generate`.
  - Validate documentation UI via local static server.
- **Acceptance Test**: Lineage graph visually displays end-to-end flow from staging to marts; PR `feature/dbt-analytics` merged.

---

## WEEK 4: Business Marts, Customer 360 & PySpark Benchmarks

### Daily Breakdown (6 Days × 6 Hours = 36 Hours)

#### Day 19: Customer 360 Denormalized Mart (OBT) (6h)
- **Goal**: Create a high-performance One Big Table (OBT) for enterprise BI and executive analytics.
- **Tasks**:
  - Implement `models/marts/obt_customer_360.sql` combining customer attributes, order aggregations, and clickstream engagement.
  - Pre-calculate metrics: lifetime spend, order frequency, average order value, cart abandonment rate.
  - Partition by `brand_id` for multi-tenant analytical isolation.
- **Acceptance Test**: Query execution benchmark: analytical queries execute under 500ms on local Spark engine without runtime joins.

#### Day 20: RFM Segmentation & Customer Scoring (6h)
- **Goal**: Implement algorithmic customer segmentation (Recency, Frequency, Monetary).
- **Tasks**:
  - Build `models/marts/agg_rfm_segments.sql` utilizing SQL `NTILE(5)` window functions.
  - Classify customers into actionable business segments: `Champions`, `Loyal Customers`, `At-Risk`, `Hibernating`, `Lost`.
  - Calculate recency days, monthly order velocity, and monetary quantiles.
- **Acceptance Test**: RFM segment distributions match expected retail benchmarks; segment migration verified across test batches.

#### Day 21: ML Feature Mart: Churn Risk Prediction (6h)
- **Goal**: Generate ML-ready feature tables aligned with industry-standard ML feature store conventions.
- **Tasks**:
  - Build `models/marts/agg_churn_risk_features.sql` calculating customer churn predictors: days since last session, return rate, frequency deceleration.
  - Compute normalized churn risk score (0.000 to 1.000) using deterministic heuristic weighting.
  - Export feature schema matching standard ML inference contracts.
- **Acceptance Test**: Feature table passes data distribution checks; ready for downstream Scikit-Learn / XGBoost ingestion.

#### Day 22: PySpark Performance Profiling & Skew Mitigation (6h)
- **Goal**: Demonstrate deep understanding of distributed Spark execution mechanics.
- **Tasks**:
  - Inject extreme artificial tenant data skew (80% of transactions allocated to `brand_alpha`).
  - Measure execution runtime of standard join vs salted key join in PySpark.
  - Profile Spark UI: inspect shuffle read/write, spilled memory, and straggling tasks.
- **Acceptance Test**: Salted join reduces max task execution time by > 40%; metrics captured for benchmarking report.

#### Day 23: Multi-Dimensional Compaction (Z-Order & Broadcast Joins) (6h)
- **Goal**: Optimize physical Delta storage and query execution paths.
- **Tasks**:
  - Implement `src/streaming/optimize_tables.py` applying `OPTIMIZE` and `ZORDER BY (brand_id, order_timestamp)`.
  - Implement broadcast hash join patterns for dimensional lookups (`broadcast(dim_products)`).
  - Measure file compaction metrics (reduction in small files and S3 read requests).
- **Acceptance Test**: File count in `silver.orders` reduced by > 70%; selective query runtimes slashed by > 50%.

#### Day 24: Benchmark Report Publication (`docs/benchmarks.md`) (6h)
- **Goal**: Synthesize empirical performance gains into a compelling engineering report.
- **Tasks**:
  - Author `docs/benchmarks.md` with concrete execution tables and Spark execution graphs.
  - Document before/after metrics: Shuffle Read MBs, task duration percentiles, small file count, storage savings.
  - Detail architectural rationale: when to use Z-Order vs Partitioning vs Liquid Clustering.
- **Acceptance Test**: Benchmark documentation finalized; PR `feature/marts-performance` merged.

---

## WEEK 5: Production Orchestration, Quality & CI/CD

### Daily Breakdown (6 Days × 6 Hours = 36 Hours)

#### Day 25: Airflow Environment Configuration & Connections (6h)
- **Goal**: Productionize local Airflow 2.9 containerized setup.
- **Tasks**:
  - Configure Airflow connections: `spark_default` (Spark master) and `s3_conn` (SeaweedFS / S3).
  - Verify Airflow webserver and scheduler logs; establish zero-warning startup.
  - Structure `dags/` repository with modular operators and task groups.
- **Acceptance Test**: Airflow Web UI accessible at `localhost:8080`; connections pass diagnostic test pings.

#### Day 26: Dynamic Multi-Tenant Ingestion DAG (6h)
- **Goal**: Build dynamic DAG generation for multi-tenant pipeline scalability.
- **Tasks**:
  - Author `dags/ecommerce_batch_pipeline.py` dynamically creating task branches per tenant (`brand_id`).
  - Sequence task dependencies: `landing_sensor >> bronze_ingest >> dlq_filter >> silver_merge`.
  - Configure task-level retries (`retries=2`, `retry_delay=timedelta(minutes=5)`).
- **Acceptance Test**: DAG dynamically renders isolated branches for all active brands; failure in one brand does not stall other tenant pipelines.

#### Day 27: Streaming Orchestration & Health Sensor DAG (6h)
- **Goal**: Orchestrate streaming micro-batches and continuous health monitoring.
- **Tasks**:
  - Author `dags/clickstream_streaming_dag.py` triggering Spark Structured Streaming micro-batches (`Trigger.availableNow`).
  - Implement custom Kafka consumer lag sensor to detect streaming delays.
  - Add automated alert hook on failure (Slack/Webhook notification payload formatting).
- **Acceptance Test**: Streaming DAG executes successfully on scheduled interval; artificial lag triggers alert sensor.

#### Day 28: Great Expectations Governance Checkpoints (6h)
- **Goal**: Implement enterprise Data Contracts via Great Expectations.
- **Tasks**:
  - Initialize Great Expectations suite in `src/quality/expectations/`.
  - Author expectation suites for Bronze and Silver tables: check non-null PKs, value ranges, string regex patterns.
  - Build Airflow operator executing GX checkpoint post-transformation.
- **Acceptance Test**: Passing data quality checkpoint produces green HTML validation report; deliberate data corruption halts downstream promotion.

#### Day 29: Idempotent Backfill & SLA Failure Recovery (6h)
- **Goal**: Demonstrate pipeline resilience and idempotency under simulated failure conditions.
- **Tasks**:
  - Write runbook and script for historical date backfills (`scripts/backfill_pipeline.py`).
  - Execute 30-day historical simulation; verify deterministic outputs without record duplication.
  - Test task crash recovery: kill worker container mid-run, restart, and resume pipeline execution.
- **Acceptance Test**: Backfill execution completes without manual state intervention; zero duplicate records created in Delta tables.

#### Day 30: GitHub Actions CI/CD Pipeline (6h)
- **Goal**: Automate code quality, linting, and regression testing on every Pull Request.
- **Tasks**:
  - Build `.github/workflows/ci.yml`: execute `ruff check`, `mypy src/`, `pytest tests/unit/ --cov=src`, and `dbt compile`.
  - Setup pre-commit hooks (`.pre-commit-config.yaml`) for local developer enforcement.
  - Verify CI run on test branch.
- **Acceptance Test**: GitHub Actions workflow runs cleanly and turns green; PR `feature/orchestration-quality` merged.

---

## WEEK 6: Cloud Deployment Proof (Terraform + Databricks) & Berlin Showcase

### Daily Breakdown (6 Days × 6 Hours = 36 Hours)

#### Day 31: Infrastructure as Code (Terraform on AWS) (6h)
- **Goal**: Automate AWS cloud infrastructure provisioning with zero manual console clicking.
- **Tasks**:
  - Implement Terraform modules in `infra/terraform/`:
    - S3 buckets (`raw-landing`, `bronze`, `silver`, `gold`, `checkpoints`) with lifecycle rules and encryption.
    - IAM roles and least-privilege policies for Databricks compute and ingestion.
  - Run `terraform init` and `terraform plan` to validate resource definitions.
- **Acceptance Test**: `terraform plan` produces clean execution graph without syntax errors; code passes `tflint`.

#### Day 32: Terraform Apply & Cloud Storage Deployment (6h)
- **Goal**: Provision live AWS infrastructure and upload landing data.
- **Tasks**:
  - Execute `terraform apply` on AWS (using standard AWS credentials).
  - Execute generator configured with AWS S3 target to populate cloud landing zone.
  - Document created resources and capture CLI terminal verification logs.
- **Acceptance Test**: AWS S3 buckets created and populated with multi-tenant partitioned data; permissions verified via AWS CLI.

#### Day 33: Databricks 14-Day Trial & Auto Loader Notebook (6h)
- **Goal**: Prove Databricks-native mastery using cloudFiles (Auto Loader).
- **Tasks**:
  - Connect Databricks workspace (AWS 14-day trial) to provisioned S3 buckets.
  - Deploy Auto Loader notebook `notebooks/databricks_auto_loader.py`: ingest S3 landing files into Databricks Delta Lake Bronze.
  - Validate schema evolution under Databricks Auto Loader engine.
- **Acceptance Test**: Databricks Auto Loader automatically discovers new S3 landing files and merges them into Delta Bronze table.

#### Day 34: Databricks Asset Bundles (DABs) & Cloud Proof Capture (6h)
- **Goal**: Execute cloud job via modern CI/CD and capture unassailable evidence.
- **Tasks**:
  - Configure `databricks.yml` Asset Bundle deploying the ingestion job to Databricks Jobs compute.
  - Execute cloud run; capture high-resolution screenshots of Databricks DAG run, Spark execution graph, and S3 storage metrics.
  - Execute `terraform destroy` to tear down AWS resources, capping cloud spend at < $5.
- **Acceptance Test**: Cloud deployment evidence archived in `docs/cloud_proof/`; AWS resources completely decommissioned ($0 ongoing cost).

#### Day 35: Portfolio Vitrini: README & Architecture Decision Records (6h)
- **Goal**: Transform GitHub repository into an irresistible senior-level showcase.
- **Tasks**:
  - Rewrite root `README.md`: compelling value proposition, high-impact architecture diagrams, 5-minute quickstart, and direct mapping table from CV claims to code files.
  - Finalize Architecture Decision Records (`docs/adr/`):
    - `0001-local-object-storage-seaweedfs.md`
    - `0002-why-kafka-kraft-over-kinesis.md`
    - `0003-append-only-bronze-invariants.md`
    - `0004-dbt-dimensional-modeling-vs-pure-spark.md`
    - `0005-multi-tenant-partitioning-strategy.md`
- **Acceptance Test**: README renders flawlessly on GitHub; repository structure matches top 1% open-source projects.

#### Day 36: System Design Interview Preparation & Final Release Tag (6h)
- **Goal**: Rehearse technical defense of every architectural decision for Berlin interviews.
- **Tasks**:
  - Create `docs/INTERVIEW_QA.md`: comprehensive technical answers to tough interview questions (Shuffle mechanics, Z-Order vs Liquid Clustering, late-arriving stream deduplication, multi-tenant isolation).
  - Tag official release: `git tag -a v1.0.0 -m "Release: Multi-Tenant Real-Time CRM Lakehouse Platform v1.0.0"`.
  - Push final release and verify all badges (CI, Python, Spark, Delta) turn green on GitHub.
- **Acceptance Test**: Repository published and tagged `v1.0.0`; project ready for inclusion in CV and direct submission to Berlin tech recruiters.
