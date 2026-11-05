# Delivery Plan: Six Weeks, Three Demonstrable Milestones

> **Document Version**: 8.1
> **Last Updated**: 2026-11-05 (v8.1: AWS Glue 5.0 replaces EMR Serverless — [ADR-0011](adr/0011-aws-glue-instead-of-emr-serverless.md); bounded Databricks Free Edition track in W6 — [ADR-0012](adr/0012-databricks-free-edition-track.md); tutor learning gate and portfolio notes in every checkpoint)
> **Budget**: 6 weeks × 6 days × ~6 focused hours ≈ 216 h (owner + Claude Code, Sonnet) · cloud spend ≤ **USD 50** total (target < USD 15), AWS Free Tier credits first
> **Relationship to other documents**: scope and invariants → [PROJECT_CHARTER.md](PROJECT_CHARTER.md); task-level acceptance criteria, statuses and evidence → [PROGRESS.md](PROGRESS.md) (task IDs `W<week>.D<day>.<n>`). This plan decides **sequencing, daily goals, checkpoints and milestone gates**.

> [!NOTE]
> **v8.0 re-plan (2026-11-05)** — corrections from the critical review, owner-approved:
> runtime rebased to Spark 3.5.9 / Delta 3.3.3 / Python 3.11 / Java 17 ([ADR-0007](adr/0007-runtime-catalog-and-gold-engine.md)); Hive metastore on Postgres + Spark Thrift Server for dbt-spark, with the dbt capability spike pulled forward to week 2; Airflow 2.9.3 (EOL 2026-04-22) replaced by Airflow 3.x in M2; AWS proof moved from "local Spark → S3" to **managed Spark (AWS Glue 5.0, ADR-0011) + Glue Data Catalog + Terraform + GitHub OIDC** with cost guardrails ([ADR-0008](adr/0008-aws-target-emr-serverless-and-cost-guardrails.md)); component boundaries and package layout ([ADR-0009](adr/0009-component-boundaries-and-contracts.md)); generator simplified (orders carry their line items, static product seed, exact-count defect injection); Great Expectations, churn mart and salting benchmark removed from v1 (Databricks returned in v8.1 as a $0 Free Edition track); a privacy-erasure day and an explicit buffer day added. The PROGRESS day structure (v8) contradicted the milestone sequence (v7) and has been rebuilt to match this plan.

---

## 1. Principles, gates and evidence

1. **Every day ends with a verifiable artefact** (test, command output, CI run, demo step) — not with "files created".
2. **Correctness before breadth**: 🔺 suites gate later work — M2 starts after the restart test passes (W2.D9); M3 starts after the CDC (W3.D15) and tenant-isolation (W3.D18) suites pass.
3. **One primary technology area per milestone**, deep enough to defend in an interview.
4. **Evidence or it did not happen**: ✅ = commit hash + verification output (or CI link). Implemented but unverified = 🔄.
5. **Cut scope, not quality**: each week names what moves to the buffer or the cut list.
6. **Free first, cheap when it buys credibility**: local stack is $0; cloud resources exist only during a cloud session and are destroyed afterwards.

**Checkpoint protocol** (run with the `checkpoint` skill):
| Level | When | What is executed | Who |
|---|---|---|---|
| Day | End of each planned day | Every task's verification command; Evidence line filled; Done-when checked; carry-overs named; portfolio notes; **tutor learning gate** | Coordinator (+ `evidence-auditor` if a 🔺 task is claimed) |
| Week | Day 6 of each week | Day checkpoints + weekly regression set + weekly demo + macro status update | Coordinator + `evidence-auditor` |
| Milestone | Days 12, 24, 36 | Week checkpoint + all milestone acceptance criteria + demo runbook from a stopped stack + independent review + release tag (owner) | Coordinator + `reviewer` + `evidence-auditor` |

## 2. Milestones

| Milestone | Days | Primary area | Release | Gate to next |
|---|---|---|---|---|
| **M1** Reliable streaming ingestion | D1–D12 | Contracts, generator, Kafka, Structured Streaming, Bronze + DLQ, catalog spike | `v0.1.0` | 🔺 restart test green |
| **M2** Data correctness & analytics products | D13–D24 | CDC MERGE, SCD2, dedup, reconciliation, tenant isolation, dbt Gold (Customer 360, RFM), Airflow 3 | `v0.2.0` | 🔺 CDC + tenant isolation green |
| **M3** Cloud productionisation | D25–D36 | Terraform, AWS Glue 5.0, Glue Catalog, Athena, IAM/OIDC, CI/CD, operations, FinOps, privacy erasure, Databricks (Unity Catalog, jobs, dbt, dashboard), release | `v1.0.0` | — |

Branching: one short-lived branch per day or task group (`feat/w1-contracts`, `feat/w2-bronze-stream`, …), merged to `main` after the day checkpoint passes.

---

## 3. Milestone 1 — Weeks 1–2: Reliable Streaming Ingestion 🟩

### Week 1 — Foundations, contracts, generator and producer

#### D1 · Python baseline and quality gates
| | |
|---|---|
| Objective | A reproducible Python dev loop with enforced quality gates before any feature code |
| Work | W1.D1.1 project venv on Python 3.11, fix `pyproject` build backend, `pip install -e .[dev]` · W1.D1.2 move code to `src/lakehouse/{common,generator,contracts}` (ADR-0009), fix the 11 ruff findings, move the coverage gate from `addopts` to the CI command, first tests for config and logging · W1.D1.3 pre-commit (ruff, ruff-format, gitleaks, check-yaml, end-of-file) + import-linter skeleton · W1.D1.4 GitHub Actions job 1 (ruff, mypy, unit tests, import-linter, gitleaks) |
| Deliverable | PR with a green CI job 1 |
| Learn | src-layout packaging, editable installs, pre-commit hooks, secret scanning, import contracts, GitHub Actions basics |
| Verify | `pre-commit run --all-files` · `pytest tests/unit -q` · `lint-imports` · CI run link |
| Depends / blockers | Python 3.11 on host (`winget install Python.Python.3.11` if missing); `gh` CLI optional |
| Done when | CI green on the PR; zero ruff/mypy findings; no `from src.` imports remain |

#### D2 · Local stack rebuild and hardening
| | |
|---|---|
| Objective | The `core` stack runs the target runtime with no secrets in tracked files |
| Work | W1.D2.1 rebuild Spark image per ADR-0007; mount `spark-defaults.conf` (no keys; env credentials provider) · W1.D2.2 compose profiles (`core`, `sql`, `orchestration`, `tools`), `127.0.0.1` ports, `${VAR:?}` secrets, pinned SeaweedFS/aws-cli tags, SeaweedFS S3 identities rendered from env at start; `scripts/bootstrap_env.py` (creates `.env` with random local secrets only if absent) and `scripts/check_env.py` (reports missing key names only) · W1.D2.3 init scripts: topics `customer_cdc` + `clickstream_events` (no Kafka DLQ topic), storage prefixes per Charter, Postgres DBs `airflow_metadata` + `hive_metastore` · W1.D2.4 strict smoke test v2 (fails on missing topics; Delta write/read round trip via s3a inside Spark; per-container memory) |
| Files | `docker/spark/Dockerfile`, `docker/spark/spark-defaults.conf`, `docker-compose.yml`, `docker/seaweedfs/` (template), `scripts/init_kafka.sh`, `scripts/init_storage.sh`, `scripts/init_postgres.sh`, `scripts/smoke_test.py`, new `scripts/bootstrap_env.py` + `scripts/check_env.py`, `tests/unit/scripts/` |
| Deliverable | `docker compose --profile core up -d` healthy; smoke test passes; host spec + memory recorded |
| Learn | Docker image layering, Python/JVM parity, S3A credential providers, KRaft topic config, compose profiles |
| Verify | `python scripts/smoke_test.py` → all checks pass · `gitleaks detect --no-git` on infra files · `docker stats --no-stream` |
| Depends / blockers | One-time image build (~2–3 GB download); old `thelook_*` containers stay stopped (port conflicts) |
| Done when | Smoke test green; old W1.D1.1 evidence gap closed; no credential literals in tracked infra files |

#### D3 · Data contracts and generator configuration
| | |
|---|---|
| Objective | Versioned event contracts that producer and Spark consumers both depend on |
| Work | W1.D3.1 `lakehouse.contracts`: envelope (`event_id`, `brand_id`, `schema_version`, `produced_at`), `CustomerCdcEvent` v1, `ClickstreamEvent` v1, `OrderEvent` v1 (embedded items), `Product` v1; JSON Schema export (committed) · W1.D3.2 `GeneratorConfig` v2: tenants, tenant weights (skew), seed, topics, exact defect counts per DATA_CONTRACTS, no secret defaults · W1.D3.3 contract tests incl. schema snapshot test that fails on a breaking change |
| Deliverable | `contracts` package + schema snapshots + tests |
| Learn | Data contracts, additive vs breaking schema changes, Pydantic v2 validators, JSON Schema |
| Verify | `pytest tests/unit/contracts -q` · deliberately breaking a field fails the snapshot test (negative test) |
| Depends / blockers | D1 |
| Done when | Contracts coverage ≥ 90%; DATA_CONTRACTS and code agree field-by-field |

#### D4 · Customer CDC generator and ground-truth manifest
| | |
|---|---|
| Objective | Deterministic CDC events with exactly known defects |
| Work | W1.D4.1 lifecycle engine (`I` first, `D` terminal), monotonic `_cdc_sequence` per `(brand_id, customer_id)`, `_cdc_ts` · W1.D4.2 exact-count injection (invalid/null email, future signup, out-of-order emission, duplicates, cross-tenant key collisions) + manifest (`event_id → defect, expected outcome ACCEPT/DLQ/NOOP/DUPLICATE`) · W1.D4.3 unit tests |
| Deliverable | `python -m lakehouse.generator cdc --brands 3 --events 1000 --seed 42 --dry-run` writes events + manifest |
| Learn | CDC semantics, LSN-style sequencing, test oracles, seeded randomness |
| Verify | `pytest tests/unit/generator/test_cdc.py -q`; two runs with the same seed are byte-identical (hash) |
| Depends / blockers | D3 |
| Done when | Manifest counts equal configured counts exactly; lifecycle and sequence invariants tested |

#### D5 · Clickstream generator and keyed idempotent producer
| | |
|---|---|
| Objective | Events reach Kafka with the agreed keys, headers and delivery semantics |
| Work | W1.D5.1 clickstream model (sessions, funnel, late offsets, duplicates, null session, malformed payloads) · W1.D5.2 producer: `confluent-kafka`, `enable.idempotence=true`, `acks=all`, key `{brand_id}:{entity_id}`, headers `brand_id`, `schema_version`, `event_type`, delivery callbacks + counters, rate limit, `flush()` on shutdown · W1.D5.3 integration test: produce → consume back |
| Deliverable | `python -m lakehouse.generator produce --events 5000` against the local broker |
| Learn | Key-hash partitioning, idempotent producer vs transactions, headers, consumer lag |
| Verify | `pytest tests/integration/test_producer_roundtrip.py -q` (count, key format, headers, one partition per `(brand_id, customer_id)`, lag 0) |
| Depends / blockers | D2, D4; `confluent-kafka` wheel for Python 3.11 on Windows (else run via `tools` profile) |
| Done when | Round-trip test green; producer metrics logged with `brand_id` |

#### D6 · Week 1 consolidation
| | |
|---|---|
| Objective | Lock in quality and document the event interface |
| Work | W1.D6.1 coverage ≥ 80% on `generator` + `contracts` enforced in CI; mypy clean; import-linter contracts active · W1.D6.2 ADR-0002 (Kafka vs Kinesis comparison, envelope, keys vs headers, JSON + JSON Schema vs registry, topic retention vs Bronze as history) · W1.D6.3 week-1 demo script + runbook section; merge to `main` |
| Deliverable | Week-1 checkpoint report in PROGRESS |
| Verify | Week checkpoint (below) |
| Done when | All week-1 acceptance criteria have evidence |

**Week 1 summary**
- **Expected output**: contracts package, deterministic generator with manifest, keyed idempotent producer, rebuilt local stack, CI job 1.
- **Integration tests**: producer round trip. **Regression set**: `pytest tests/unit -q`, `python scripts/smoke_test.py`, `pre-commit run --all-files`, `lint-imports`.
- **Acceptance**: same seed → byte-identical output · manifest exact · ≥ 5,000 clickstream and ≥ 1,000 CDC events for ≥ 3 brands round-trip · per-key partition affinity asserted · CI job 1 green on `main`.
- **Demo** (≤ 10 min): `docker compose --profile core up -d` → smoke test → generate + produce → console consumer shows keys/headers → manifest summary.
- **Evidence**: CI run link, commit hashes, test outputs, smoke-test output, host spec.

### Week 2 — Bronze, DLQ, restart semantics and catalog

#### D7 · Spark job framework
| | |
|---|---|
| Objective | One way to build, configure, test and submit every Spark job |
| Work | W2.D7.1 `lakehouse.common.spark`: SparkSession factory with environment adapter (`local` = SeaweedFS + metastore, `aws` = S3 + Glue) from settings; job CLI base with run IDs in logs · W2.D7.2 `spark-test` service (tools profile) running pytest inside the Spark image; first Spark tests (StructType derived from contracts) · W2.D7.3 wheel build + `scripts/submit_job.py` (spark-submit in the Spark master container) |
| Deliverable | `python scripts/submit_job.py hello_delta` writes and reads a Delta table |
| Learn | Spark session/config layering, packaging PySpark apps, driver/executor parity |
| Verify | `docker compose --profile tools run --rm spark-test pytest tests/spark -q` |
| Depends / blockers | D2 |
| Done when | Spark tests run in-container; no PySpark on the Windows host required |

#### D8 · Kafka → Bronze Structured Streaming
| | |
|---|---|
| Objective | Every Kafka message lands in Bronze exactly as received, with lineage |
| Work | W2.D8.1 `ingestion.bronze_stream`: both topics, `_raw_value`, key, headers, `_source_ref` (topic/partition/offset), `_ingest_ts`, `_batch_id`, `brand_id` (payload, header fallback), `_parse_error`; `Trigger.availableNow`; one checkpoint per query · W2.D8.2 Spark tests for the parse function (valid, malformed, missing tenant) · W2.D8.3 integration: Bronze rows = produced messages |
| Deliverable | `bronze.raw_customers`, `bronze.raw_clickstream` Delta tables |
| Learn | Kafka source options, `foreachBatch` vs sink, checkpoints, Delta transaction log |
| Verify | `pytest tests/integration/test_bronze_stream.py -q` |
| Depends / blockers | D5, D7 |
| Done when | Count equality incl. parse-error rows; metadata columns non-null |

#### D9 · 🔺 Restart and checkpoint-loss semantics
| | |
|---|---|
| Objective | Prove what happens on restart and on checkpoint loss — the delivery-semantics claim |
| Work | W2.D9.1 test (a) kill mid-stream → restart → each `(topic, partition, offset)` once in Bronze; (b) delete checkpoint → replay → re-reads quantified and documented · W2.D9.2 ADR-0006 draft with the measured outcomes |
| Learn | Offset/commit logs, Delta idempotent writes (`txnAppId`/`txnVersion`), at-least-once vs idempotent outcome |
| Verify | `pytest tests/integration/test_stream_restart.py -q` |
| Depends / blockers | D8; test harness must stop a query deterministically (processingTime trigger + controlled stop) |
| Done when | Both scenarios automated; ADR-0006 states only what the test shows |

#### D10 · Validation gate and DLQ for stream entities
| | |
|---|---|
| Objective | Contract failures are quarantined with a reference to Bronze — nothing is dropped |
| Work | W2.D10.1 `quality.rules` (pure column expressions per contract → `_error_category`) + DLQ writer (`_dlq_id`, `_bronze_ref`, `_retryable`, `_replay_status`) · W2.D10.2 Spark tests per rule incl. negatives · W2.D10.3 integration: DLQ set = manifest's DLQ-expected set (exact); duplicates and late events not in DLQ |
| Learn | Validation as data, error taxonomies, quarantine patterns |
| Verify | `pytest tests/spark/test_rules.py tests/integration/test_dlq_reconciliation.py -q` |
| Depends / blockers | D8 |
| Done when | Exact set equality with the manifest |

#### D11 · Catalog and dbt capability spike (time box 6 h)
| | |
|---|---|
| Objective | Remove the largest rework risk before modelling: tables by name, dbt on Spark |
| Work | W2.D11.1 Postgres-backed Hive metastore; register Bronze/DLQ tables; jobs switch to table names · W2.D11.2 Spark Thrift Server (`sql` profile); `dbt debug`; probe models for incremental `merge`, composite `unique_key`, contracts, `persist_docs` · W2.D11.3 decision recorded (Thrift or `session` fallback) in ADR-0004 spike section |
| Learn | Hive metastore internals, Thrift/JDBC, dbt adapters and their capability limits |
| Verify | `dbt debug` and `dbt build --select spike` output; `SHOW TABLES IN bronze` via Thrift |
| Depends / blockers | D8; Hive 2.3 client + Postgres JDBC + Delta catalog configuration |
| Done when | Capability table filled with observed results; fallback chosen if anything fails |

#### D12 · M1 review, demo and release
| | |
|---|---|
| Objective | Prove M1 end-to-end from a stopped stack |
| Work | W2.D12.1 `docs/runbooks/m1-streaming-demo.md` · W2.D12.2 full regression · W2.D12.3 milestone review (`reviewer` + `evidence-auditor`), README status with evidence links, "can / cannot claim" note, `v0.1.0` tag (owner) |
| Verify | Milestone checkpoint |
| Done when | All M1 acceptance criteria have evidence |

**Week 2 summary**
- **Expected output**: Bronze streaming with lineage, DLQ for stream entities, restart/checkpoint-loss evidence, catalog + dbt decision.
- **Integration tests**: Bronze count, restart (a/b), DLQ reconciliation, Thrift connectivity. **Regression set**: week-1 set + `tests/spark` + all integration tests so far.
- **Acceptance** = M1 acceptance below.

### M1 milestone review and demo
- **Acceptance**: unit suite offline green, coverage ≥ 80% on `generator` + `contracts` · deterministic generator · Bronze rows = produced messages incl. `_parse_error` · 🔺 restart: offsets unique after restart; checkpoint-loss re-reads documented · DLQ = manifest DLQ set exactly · tables resolvable by name in the metastore; dbt capability decision recorded · CI job 1 green on `main` · ADR-0002, ADR-0006 (draft) written.
- **Demo** (`m1-streaming-demo.md`, ≤ 15 min): stack up → smoke → produce → Bronze `availableNow` → kill/restart → offset-uniqueness query → DLQ vs manifest → `SHOW TABLES` via Thrift.
- **Evidence**: release tag, CI link, test outputs, demo transcript, reviewer verdict.
- **Claimable**: composite keys without hot tenants; at-least-once producer + checkpointed Bronze with tested restart; no silent loss. **Not claimable**: exactly-once end-to-end, Silver correctness, cloud.

---

## 4. Milestone 2 — Weeks 3–4: Data Correctness and Analytics Products 🟩

### Week 3 — Silver correctness

#### D13 · Batch landing and Bronze batch ingest
| | |
|---|---|
| Objective | The second ingestion pattern (files) with the same Bronze guarantees |
| Work | W3.D13.1 generator: orders with embedded items, static product seed per brand, landing writer (boto3, retries, manifest next to the batch), additive schema change `sales_channel` from batch 10 · W3.D13.2 `ingestion.bronze_batch`: incremental file source (`availableNow`), `_source_ref` = file + line, `mergeSchema` · W3.D13.3 unit tests (moto) + integration count test |
| Learn | Incremental file ingestion (Auto Loader concept on OSS), schema evolution in Delta |
| Verify | `pytest tests/unit/generator/test_landing.py tests/integration/test_bronze_batch.py -q` |
| Depends / blockers | D7, D11 |
| Done when | Bronze rows = landed rows; batch 10+ rows carry `sales_channel`, earlier rows null |

#### D14 · Silver customers: sequence-ordered CDC MERGE
| | |
|---|---|
| Objective | Correct current state under duplicates and reordering, with pseudonymised PII |
| Work | W3.D14.1 `silver.silver_customers` (`foreachBatch`: dedup to max sequence per key, MERGE guarded by `_cdc_sequence`, tombstones, HMAC `email_token`, `phone_masked`, `stale_cdc_noop` metric) · W3.D14.2 Spark tests on tiny frames |
| Learn | Delta MERGE semantics, multiple-match errors, keyed HMAC vs hashing |
| Verify | `pytest tests/spark/test_silver_customers.py -q` |
| Depends / blockers | D10, D11; `PII_HMAC_KEY` from `.env` |
| Done when | Merge logic unit-tested for all action types |

#### D15 · 🔺 CDC correctness suite and PII checks
| | |
|---|---|
| Objective | The central correctness claim, proven |
| Work | W3.D15.1 six scenarios (in-order, stale `U`, late `U` after `D`, duplicate, multi-event batch, cross-brand same `customer_id`) · W3.D15.2 no raw email/phone in Silver; key from env; different key → different token · W3.D15.3 ADR-0004 final (Spark/dbt ownership + spike results) |
| Verify | `pytest tests/integration/test_cdc_correctness.py -q` |
| Done when | All six pass; PII test passes; ADR-0004 accepted |

#### D16 · SCD2 history (Spark-owned)
| | |
|---|---|
| Objective | History identical regardless of arrival order |
| Work | W3.D16.1 `silver.silver_customers_history`: deterministic rebuild of affected keys from the Bronze CDC log ordered by `_cdc_sequence` · W3.D16.2 four scenario tests (Istanbul → Berlin, shuffled = identical hash, equal `_cdc_ts` ordered by sequence, re-run adds nothing) |
| Learn | SCD2 under late data, rebuild-vs-patch trade-off |
| Verify | `pytest tests/integration/test_scd2_history.py -q` |
| Done when | Shuffled and in-order deliveries produce identical history hashes |

#### D17 · Orders, items, clickstream Silver and DLQ replay
| | |
|---|---|
| Objective | Dedup, referential integrity and recoverable quarantine |
| Work | W3.D17.1 `silver_orders` + `silver_order_items` (explode, dedup, composite FK to customers, orphans → DLQ `_retryable`) · W3.D17.2 `silver_clickstream` (in-batch dedup + bounded lookback MERGE, `late_beyond_window` metric) · W3.D17.3 `quality.replay` CLI + test (late parent recovered; replaying twice adds nothing) |
| Verify | `pytest tests/integration/test_silver_orders.py tests/integration/test_dlq_replay.py -q` |
| Done when | Zero duplicate business keys; replay idempotent |

#### D18 · Reconciliation, idempotent re-run, 🔺 tenant isolation
| | |
|---|---|
| Objective | Every record accounted for, re-runs are no-ops, tenants cannot leak |
| Work | W3.D18.1 `quality.reconcile`: `bronze = silver_accepted + dlq + duplicates_removed + stale_cdc_noop + late_beyond_window` vs manifest → `ops.pipeline_metrics` · W3.D18.2 crash mid-batch + full re-run → identical Silver content hash · W3.D18.3 tenant isolation suite + negative leaky-join fixture + tenant-scoped views · W3.D18.4 ADR-0003, ADR-0005 |
| Verify | `pytest tests/integration/test_reconciliation.py tests/integration/test_idempotent_rerun.py tests/integration/test_tenant_isolation.py -q` |
| Done when | All green and the negative test fails as intended |

**Week 3 summary**
- **Expected output**: complete, idempotent, tenant-isolated Silver with SCD2, DLQ replay and reconciliation metrics.
- **Integration tests**: CDC suite, SCD2, orders/replay, reconciliation, idempotent re-run, tenant isolation. **Regression set**: all M1 suites + the above.
- **Acceptance**: 🔺 CDC and tenant-isolation suites green · reconciliation equation holds per batch · content hash identical on re-run · no raw PII in Silver.
- **Demo**: inject the six CDC scenarios → query Silver/SCD2 → run the leaky fixture and show the failing assertion → reconciliation report.

### Week 4 — Gold and orchestration

#### D19 · dbt foundations
| | |
|---|---|
| Objective | A tested staging layer and tenant-safe test primitives |
| Work | W4.D19.1 `dbt_project.yml`, profiles from env, sources on Silver, staging models · W4.D19.2 generic test `relationships_by_brand`, singular `assert_no_cross_tenant_joins`, `dbt parse` in CI job 1 |
| Learn | dbt project structure, sources, generic vs singular tests |
| Verify | `dbt build --select staging` · CI shows `dbt parse` |
| Depends / blockers | D11 decision, Silver tables |
| Done when | Staging builds with 100% tests passing |

#### D20 · Dimensions and facts
| | |
|---|---|
| Work | W4.D20.1 `dim_customers`, `dim_customers_history`, `dim_products`, `dim_date` · W4.D20.2 `fct_orders`, `fct_order_items`, `fct_sessions`; composite relationship tests; contracts where supported |
| Verify | `dbt build --select marts` |
| Done when | No orphan keys; all tests green |

#### D21 · Customer 360 and RFM
| | |
|---|---|
| Work | W4.D21.1 `obt_customer_360` (grain `(brand_id, customer_id)`, `as_of_date` var) · W4.D21.2 `agg_rfm_segments` (NTILE per brand) + dbt unit tests + distribution check · W4.D21.3 `dbt docs generate`; tenant-scoped Gold views |
| Learn | OBT vs star schema, reproducible metrics, dbt unit tests |
| Verify | `dbt build --select +agg_rfm_segments` · segment distribution query |
| Done when | No segment at 0%/100% per brand; same `as_of_date` → identical output |

#### D22 · Airflow 3 pipeline
| | |
|---|---|
| Objective | One parametrised, re-runnable pipeline |
| Work | W4.D22.1 Airflow 3.x image (Python 3.11, Java 17, Spark client) in the `orchestration` profile · W4.D22.2 DAG `lakehouse_pipeline`: per-brand landing check (`.expand`) → bronze → silver jobs → reconcile gate → `dbt build`; retries, timeouts, `max_active_runs=1` · W4.D22.3 ADR-0010 (orchestration ↔ compute interface: SparkSubmit locally, EMR Serverless operator on AWS) |
| Learn | Airflow 3 task SDK, dynamic task mapping, idempotent tasks |
| Verify | `airflow dags test lakehouse_pipeline <date>` · UI run green |
| Depends / blockers | Memory: `core` + `orchestration` together on 7.6 GB Docker — stop the `sql` profile while Airflow runs |
| Done when | A full run completes; a second run for the same interval changes nothing |

#### D23 · Failure isolation, quality gates, alerting, backfill
| | |
|---|---|
| Work | W4.D23.1 injected failure for one brand; other brands complete; documented trigger rules · W4.D23.2 DLQ-rate threshold per brand halts downstream · W4.D23.3 failure/threshold callback → JSON webhook payload (local sink; Slack optional via secret) · W4.D23.4 backfill of a past interval is idempotent |
| Verify | `pytest tests/integration/test_dag_failure_isolation.py -q` + payload in logs |
| Done when | Gate halts on corruption; alert payload captured |

#### D24 · M2 review, demo and release
| | |
|---|---|
| Work | W4.D24.1 `scripts/run_e2e.py` (one command: generate → produce → bronze → silver → reconcile → dbt) · W4.D24.2 full regression · W4.D24.3 runbook `m2-correctness-demo.md`, milestone review, `v0.2.0` (owner) |
| Done when | All M2 acceptance criteria have evidence |

**Week 4 summary**
- **Expected output**: tested Gold marts incl. Customer 360 and RFM; Airflow 3 pipeline with gates and alerts; one-command e2e.
- **Integration tests**: dbt build, DAG run, failure isolation, backfill idempotency. **Regression set**: all suites + `run_e2e.py`.

### M2 milestone review and demo
- **Acceptance**: 🔺 CDC (6 scenarios) and tenant isolation (incl. negative) green · SCD2 order-independent · reconciliation per batch matches manifest · re-run hash identical · `dbt build` 100% tests incl. composite relationships and cross-tenant test · RFM non-degenerate per brand · no raw PII in Silver/Gold · DAG run + failure isolation + gate + alert evidenced · ADR-0003/0004/0005/0010 written.
- **Demo** (`m2-correctness-demo.md`, ≤ 20 min): `run_e2e.py` → CDC scenarios → SCD2 history → leaky-join negative test → dbt docs lineage → Customer 360/RFM per brand → Airflow run with one failing brand.
- **Claimable**: sequence-ordered CDC and order-independent SCD2, proven by tests; tested tenant isolation; reproducible marts. **Not claimable**: legal GDPR compliance, performance at scale, cloud.

---

## 5. Milestone 3 — Weeks 5–6: Cloud Productionisation on AWS + Databricks 🟦

Cloud days follow one pattern: `terraform apply` (owner-approved) → run → verify → record spend → `terraform destroy` of compute/data (state bucket kept). The AWS account is on the **Free plan** (USD 100 credits, plan ends ≈ 2027-03-22): charges cannot occur, credits are consumed and tracked (ADR-0011).

### Week 5 — AWS foundation and the same jobs on AWS Glue 5.0

#### D25 · AWS account readiness and Terraform bootstrap
| | |
|---|---|
| Objective | A safe, budget-tracked AWS account and a working IaC toolchain |
| Work | W5.D25.1 (owner + coordinator) root MFA; owner CLI access with short-lived credentials (`aws login` if available, else IAM user + MFA + role assumption, keys only in the OS credential store); install AWS CLI v2, Terraform, tflint, trivy · W5.D25.2 `infra/terraform/bootstrap` (state bucket: versioning, SSE, public-access block, native lockfile) · W5.D25.3 budgets 5/20/40 USD (tracks credit consumption) · W5.D25.4 CI: `terraform fmt -check`, `validate`, `tflint`, `trivy config` |
| Learn | Terraform state and locking, AWS account plans and credentials, Budgets, IaC scanning |
| Verify | `terraform plan` clean · CI IaC job green · budgets visible (owner) |
| Depends / blockers | IAM Identity Center is unavailable on this account type — handled by the CLI-access decision above |
| Done when | Bootstrap applied; budgets active; no long-lived keys in files |

#### D26 · Terraform core modules
| | |
|---|---|
| Work | W5.D26.1 storage (lakehouse bucket incl. `artifacts/` prefix, lifecycle, SSE-S3, public-access block) and catalog (Glue databases) · W5.D26.2 identity: Glue job role (prefix-scoped), tenant demo policy (`brand_alpha` only), GitHub OIDC provider + role restricted to this repo/`main`/environment · W5.D26.3 compute: Glue 5.0 job definitions (`G.1X`, 2 workers, timeout, max concurrency 1, `--datalake-formats delta`) · `reviewer` (code, `security`) |
| Learn | Least-privilege IAM, OIDC trust policies, Glue job model, S3 lifecycle economics |
| Verify | `terraform plan` · `trivy config infra/terraform` · reviewer verdict |
| Done when | Plan reviewed and approved; zero high/critical IaC findings |

#### D27 · First cloud run on Glue
| | |
|---|---|
| Work | W5.D27.1 wheel build + upload to `artifacts/`; job arguments wire the `aws` environment adapter · W5.D27.2 `terraform apply` (owner) · W5.D27.3 generator writes landing data via the AWS adapter; `bronze_batch` as a Glue job; tables in Glue Data Catalog; Athena query; spend recorded; teardown |
| Learn | Glue job parameters, Glue catalog as Hive-compatible metastore, Athena on Delta |
| Verify | `python scripts/cloud/run_job.py bronze_batch` → `SUCCEEDED`; row counts = local run for the same seed |
| Done when | Same code, configuration-only difference; counts reconcile |

#### D28 · Silver on Glue, tenant policy test, local-vs-cloud report
| | |
|---|---|
| Work | W5.D28.1 Silver jobs + reconcile on Glue · W5.D28.2 tenant demo policy: `brand_alpha` role denied on `brand_beta/` (CLI evidence) · W5.D28.3 `docs/LOCAL_VS_CLOUD.md` (catalog, credentials, Delta build, S3 semantics, logs, Glue specifics) · teardown |
| Verify | Reconcile output on AWS = local; AccessDenied evidence |
| Done when | Differences documented with evidence; spend recorded |

#### D29 · CI/CD
| | |
|---|---|
| Work | W5.D29.1 CI job 2: Docker `core` profile integration suites on PRs to `main` + manual dispatch · W5.D29.2 `deploy-and-run` workflow (manual, environment `aws-dev` with required reviewer): OIDC → build wheel → upload → start Glue job → wait → assert reconcile · README badge after the first green run |
| Learn | OIDC in GitHub Actions, environments and approvals, artefact promotion |
| Verify | Links to green CI job 2 and deploy run |
| Done when | No AWS secrets stored in GitHub; deploy gated by approval |

#### D30 · Operations, FinOps and cloud orchestration
| | |
|---|---|
| Work | W5.D30.1 `ops` metrics report (local + cloud): counts, DLQ rate, late/stale, freshness, lag · W5.D30.2 runbooks: replay, checkpoint recovery, cloud teardown, incident triage · W5.D30.3 cost report (spend per cloud day, cost per job run) + verified teardown · W5.D30.4 cloud variant of the Airflow DAG triggers the Glue jobs (`GlueJobOperator`) once (ADR-0010) |
| Verify | Runbook dry-run; Glue job runs listed; no running resources after teardown |
| Done when | Spend to date recorded; teardown verified |

**Week 5 summary**
- **Expected output**: Terraform-provisioned AWS foundation, the same jobs on Glue 5.0, CI/CD with OIDC, runbooks, measured spend.
- **Integration tests**: CI job 2, cloud reconcile run, tenant prefix denial. **Regression set**: all local suites + `terraform validate` + IaC scan.
- **Demo**: `deploy-and-run` → Glue job run → Athena query → reconcile report → teardown.

### Week 6 — Privacy, Databricks, reproducibility and release

#### D31 · Privacy erasure procedure
| | |
|---|---|
| Work | W6.D31.1 `ops.erasure` CLI: rewrite a subject's records in Bronze (audited exception), Silver, Gold; `ops.erasure_log` · W6.D31.2 test: subject absent from current table versions; time-travel and `VACUUM` retention caveats documented; ADR-0003 update |
| Verify | `pytest tests/integration/test_erasure.py -q` |
| Done when | Procedure tested; no claim that `VACUUM` alone equals erasure |

#### D32 · Databricks I — Unity Catalog and the same jobs (ADR-0012)
| | |
|---|---|
| Objective | Hands-on Unity Catalog governance and the same Bronze → Silver code on Databricks serverless |
| Work | W6.D32.1 (owner, by hand in the UI) Free Edition workspace; catalog `lakehouse_dev`, schemas `bronze/silver/gold`, volume `landing`, a tenant-scoped group with grants; explore lineage in Catalog Explorer · W6.D32.2 the same grants as SQL in the bundle (reproducible) · W6.D32.3 upload a generator sample into the volume; Databricks Asset Bundle deploys the `lakehouse` wheel as a serverless job; Bronze → Silver with `availableNow` |
| Learn | Unity Catalog object model (metastore → catalog → schema → table/volume), privileges and lineage, serverless jobs, Asset Bundles |
| Verify | `databricks bundle validate && databricks bundle run` → success; row counts = local for the same seed |
| Depends / blockers | Free Edition quotas and feature gaps (documented, not worked around) |
| Done when | UI work and its code equivalent both recorded; counts reconcile |

#### D33 · Databricks II — Gold, dashboard and table layout
| | |
|---|---|
| Work | W6.D33.1 dbt-databricks target: same Gold models on the SQL warehouse, tests green · W6.D33.2 one AI/BI dashboard on Customer 360 / RFM · W6.D33.3 table layout note: local `OPTIMIZE` file counts before/after (measured per Charter §8) and Liquid Clustering vs Z-Order on Databricks (qualitative + file statistics) · `docs/LOCAL_VS_CLOUD.md` third column |
| Learn | dbt adapters and portability, Databricks SQL warehouses, Liquid Clustering, small-file problem |
| Verify | `dbt build --target databricks` · dashboard screenshot · note in `docs/benchmarks.md` |
| Done when | Gold builds on two engines from one codebase; differences documented |

#### D34 · Clean-machine reproducibility and README
| | |
|---|---|
| Work | W6.D34.1 fresh clone in a new folder → bootstrap → stack → e2e (timed); fix gaps · W6.D34.2 README rewrite from `docs/PORTFOLIO_NOTES.md`: verified features only, evidence links, implemented-architecture diagram, quickstart, engineering workflow |
| Done when | Quickstart timed and reproducible; every README claim links evidence |

#### D35 · ADRs, portfolio narrative, final regression and cloud cycle
| | |
|---|---|
| Work | W6.D35.1 ADR-0002…0012 final; `docs/PORTFOLIO_NOTES.md` interview narrative final (design story, trade-offs, failure-mode walkthroughs, CV evidence bullets) · W6.D35.2 all local suites + CI · W6.D35.3 one end-to-end AWS cycle (apply → deploy-and-run → verify → destroy), spend recorded |
| Done when | Everything green; resources destroyed; total spend recorded |

#### D36 · Buffer, M3 review and release
| | |
|---|---|
| Work | Carry-overs first · M3 milestone review · `v1.0.0` tag (owner) · public-repo readiness (full-history gitleaks; LICENSE file; GitHub secret scanning/push protection once public) · retrospective |
| Done when | M3 acceptance has evidence |

**Week 6 summary**
- **Expected output**: tested erasure, the same pipeline and Gold models on Databricks, reproducible quickstart, complete ADRs, portfolio narrative, `v1.0.0`.
- **Regression set**: everything + timed clean-machine run + final AWS cycle.

### M3 milestone review and demo
- **Acceptance**: Terraform plan clean, `tflint`/`trivy` clean in CI · same job code on local Spark, AWS Glue 5.0 and Databricks serverless with configuration-only differences; counts reconcile · least-privilege + tenant prefix denial evidenced · CI job 1 + job 2 + deploy workflow green, OIDC only, gitleaks in pre-commit and CI · alerts and runbooks evidenced · erasure tested · Unity Catalog grants reproducible from code · spend recorded and AWS resources destroyed · README claims all linked to evidence · clean-machine quickstart timed.
- **Demo** (`m3-cloud-demo.md`, ≤ 25 min): approve deploy → Glue run → Athena query → tenant denial → cost report → teardown proof → Databricks job + UC lineage + dashboard.
- **Claimable**: "provisioned AWS with Terraform, deployed through GitHub OIDC, ran the same Spark/Delta jobs locally, on AWS Glue 5.0 and on Databricks serverless, built Gold with dbt on two engines, governed with Unity Catalog, measured cost". **Not claimable**: production readiness, managed Kafka, production Databricks experience.

---

## 6. Job-market evidence (Berlin, verified 2026-11-05)

Method: postings retrieved from the employers' own ATS APIs (Greenhouse, SmartRecruiters, Ashby) on 2026-11-05 — the APIs list only **published (active)** postings. The previous v7 sample (11 ads, mostly expired) is superseded.

| # | Employer — role | Status 2026-11-05 · first published | Required (R) / preferred (P) |
|---|---|---|---|
| 1 | [GetYourGuide — Senior Data Engineer, Growth](https://job-boards.greenhouse.io/getyourguide/jobs/8243008) | Active · 2026-10-01 | R: SQL, Python, dbt, Delta/Snowflake, Postgres, Spark familiarity, BI, data quality, AI-first. P: Java/Scala, CRM/marketing data, semantic layer |
| 2 | [Wooga — Senior Data Engineer](https://job-boards.greenhouse.io/wooga/jobs/8305649) | Active · updated 2026-10-22 | R: Python, SQL, Scala/Java/Go, cloud data platform, dbt, Airflow, batch + streaming, Docker, CI/CD, unit tests, Terraform, agentic AI tooling. P: Snowflake |
| 3 | [HelloFresh — Senior Data Platform Engineer](https://careers.hellofresh.com/global/en/job/8128254?gh_jid=8128254) | Active · 2026-09-04 | R: AWS, Kafka (deep), SQS, Postgres/NoSQL, Terraform, Kubernetes, data quality on streams, governance/cost guardrails, on-call. P: Flink, Kafka Connect, data lakes |
| 4 | [Trade Republic — Data Platform Engineer](https://traderepublic.com/en-de/about?jobId=6318622003&gh_jid=6318622003) | Active (evergreen) · 2025-01-10, updated 2026-10-28 | Stack: Python, AWS, Spark + Kyuubi, dbt, Airflow, Iceberg, Kubernetes, SLOs |
| 5 | [Grover — Sr. Data Engineer](https://job-boards.greenhouse.io/grover/jobs/8034518003) | Active · 2026-10-16 | R: Python, SQL, Snowflake/BigQuery/Redshift, Airflow, AWS, Terraform, CI/CD, data quality, FinOps. P: dbt, Kubernetes, Kafka |
| 6 | [A11 — Senior Data Platform Engineer (for Andercore)](https://job-boards.greenhouse.io/projectaservicesgmbhcokg/jobs/8855064002) | Active · 2026-10-12 | R: SQL, Python, Spark, lakehouse, IaC, CI, testing, CRM/ERP modelling, AI coding tools. P: Databricks, Kafka/Kinesis, Salesforce |
| 7 | [A11 — Senior Data Engineer](https://job-boards.greenhouse.io/projectaservicesgmbhcokg/jobs/8800470002) | Active · 2026-10-29 | R: SQL, Python, dbt, DWH modelling, German + English. P: ML/AI |
| 8 | [Delivery Hero — Data Engineer II, Global Discovery](https://jobs.smartrecruiters.com/DeliveryHero/744000159819620-data-engineer-ii-global-discovery-consumer-) | Active · 2026-11-05 (mid-level, stack signal) | R: Python, SQL, BigQuery, Airflow, dbt, DQ monitoring, unit/integration tests, coding agents. P: Kafka/Flink |
| 9 | [SumUp — Data Platform Engineer](https://sumup.com/careers/positions/8364212002?gh_jid=8364212002) | Active · 2026-07-23 (mid-level) | R: Python/Go, SQL, GCP, Terraform, Kubernetes, dbt/Snowflake/Trino/Airflow, CI/CD, observability. P: Kafka/Flink |
| 10 | [Solaris — Senior Bank Analytics Engineer](https://job-boards.greenhouse.io/solarisbank/jobs/8675884002) | Active · 2026-10-07 (AE track) | R: SQL, Python, advanced modelling, Git, CI/CD, testing, DQ framework, banking. P: Snowflake, dbt, Airflow |
| 11 | [Enpal — Senior Analytics Engineer](https://jobs.ashbyhq.com/enpal/46731c1e-c229-40e1-ba11-9185cb3f2a00) | Listed · 2026-09-14 | Description not retrievable — counted for presence only |

> [!WARNING]
> n = 10 analysable postings from employers with public ATS APIs; LinkedIn/StepStone-only employers (e.g. Zalando) are under-represented. Treat counts as directional, not representative.

### Coverage map (n = 10)
| Skill | Demand | Project coverage | Evidence (planned location) |
|---|---|---|---|
| Python | 10/10 | ✅ Covered | `src/lakehouse/`, `tests/` |
| SQL / data modelling | 9/10 · 5/10 | ✅ Covered | dbt marts, SCD2, Spark MERGE |
| dbt | 8/10 (6 R) | ✅ Covered (dbt-spark, tests, unit tests, contracts) | `dbt_project/` |
| Data quality / observability | 8/10 | ✅ Covered | `quality/`, `ops.pipeline_metrics`, alerts, runbooks |
| AI-assisted engineering | 7/10 | 🟡 Partial — documented Claude Code workflow with independent review gates (workflow, not an AI product) | `CLAUDE.md`, `.claude/`, README "Engineering workflow" |
| Airflow | 6/10 | ✅ Covered (Airflow 3, mapping, gates) | `dags/` |
| CI/CD + testing | 6/10 | ✅ Covered | `.github/workflows/` |
| Kafka / streaming | 6/10 (2 R) | ✅ Covered locally; 🟡 no managed Kafka / Flink | `ingestion/`, restart tests |
| Cloud warehouse (Snowflake/BigQuery/Redshift) | 6/10 | 🟡 Partial — Databricks SQL warehouse + dbt-databricks, Athena; no Snowflake/BigQuery | W6.D33 |
| Databricks / Unity Catalog | 1/10 here (Andercore P); Zalando and Aroundhome in the earlier sample | ✅ Bounded — Free Edition: UC by hand and as code, serverless jobs, dbt, dashboard | W6.D32–D33 |
| Terraform / IaC | 5/10 | ✅ Covered | `infra/terraform/` |
| Kubernetes | 4/10 | ❌ Gap (Docker, Glue, Databricks serverless) | — |
| AWS | 3/10 explicit | ✅ Covered (S3, Glue 5.0 jobs, Glue Catalog, IAM, OIDC, Athena, Budgets) — aligned with the AWS Data Engineer Associate syllabus | M3 evidence |
| Spark | 3/10 explicit | ✅ Covered | Spark jobs, tests, benchmark |
| Lakehouse formats | 3/10 | ✅ Delta; 🟡 Iceberg discussed in ADR only | ADR-0007 |
| FinOps / cost | 3/10 | ✅ Covered (budgets, measured cost per run) | cost report |
| PII / governance | 3/10 | ✅ Covered (HMAC, tombstones, erasure, prefix IAM) | ADR-0003, tests |
| JVM language | 3/10 | ❌ Gap | — |

### Answers
1. **Competitive?** Yes for platform- and pipeline-oriented Senior DE roles (HelloFresh, Trade Republic, Wooga, A11/Andercore-type) **if completed with evidence** — the correctness tests, IaC and cloud proof are above typical portfolio level. Partially for warehouse-centric roles (Grover, SumUp, Delivery Hero) because of the missing Snowflake/BigQuery experience. The project supports interviews; it does not replace professional experience on the CV.
2. **Still missing**: Snowflake/BigQuery specifically, Kubernetes, a JVM language, Iceberg hands-on, Flink. The warehouse gap is partly closed by Databricks SQL + dbt-databricks (W6.D33).
3. **Low-value investments removed**: Great Expectations (0/10, duplicates dbt tests + validation gate), churn heuristic mart (no real signal on synthetic data), salting benchmark (AQE covers skew; local single-node numbers prove little), separate `order_items` landing entity (no new concept), Kinesis. Databricks was removed in v8.0 for time/cost and **returned in v8.1** because Free Edition costs $0.
4. **Changes made**: AWS Glue managed Spark + OIDC CI/CD + FinOps (ADR-0008/0011); bounded Databricks track (ADR-0012); stronger data-quality/observability; Airflow 3; documented AI-assisted workflow as evidence; erasure procedure scheduled; buffer day.
5. **Credible to a hiring manager?** Yes, provided claims stay evidence-linked: the design follows recognisable production patterns (idempotent MERGE, sequence-ordered CDC, single-owner SCD2, contracts, least-privilege IAM, ephemeral cloud) and states its limits (synthetic data, local Kafka, small scale).

---

## 7. Cost plan (cloud)
| Item | Expected | Guardrail |
|---|---|---|
| Glue 5.0 job runs (≈ 20–30 small runs, G.1X × 2) | ≈ USD 1–5 (DPU-hours; rate per region — confirm on the pricing page) | timeout, max concurrency 1, runs only on cloud days |
| S3 storage + requests (few GB) | < USD 1 | lifecycle 14 days on landing/checkpoints |
| Databricks Free Edition | USD 0 | daily quotas; no paid tier |
| Glue Data Catalog, Athena | < USD 1 | small data, partitioned queries |
| CloudWatch / Budgets / IAM / OIDC | ~USD 0 | log retention ≤ 14 days |
| **Total** | **< USD 10 (estimate), drawn from the USD 100 credits; Free plan → no out-of-pocket charges** | Budgets at 5/20/40; hard ceiling 50 |
GitHub Actions: private-repo free minutes; integration job only on PRs to `main` and manual runs.

## 8. Cut list (in this order if a week slips)
1. W6.D33.3 table-layout note → post-v1.
2. W6.D33.1–2 dbt-databricks + dashboard → post-v1 (keep D32: Unity Catalog + jobs).
3. W4.D23.4 backfill → post-v1 (re-run idempotency already proven in W3.D18.2).
4. W5.D29.2 deploy workflow → manual `run_job.py` with the owner's short-lived credentials (CI job 2 stays).
5. Never cut: 🔺 suites, reconciliation, IaC + Glue run, secret scanning, evidence.

## 9. Out of scope for v1
Production Databricks features beyond Free Edition, Kinesis implementation (ADR comparison only), managed Kafka (MSK), Kubernetes, Flink, Great Expectations, churn mart, Reverse ETL, live dashboards, MWAA, multi-cloud, real-vendor TCO comparisons, Schema Registry.
