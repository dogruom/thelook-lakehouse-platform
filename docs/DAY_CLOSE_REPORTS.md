# Day Close Reports

> **Document Version**: 1.0
> **Last Updated**: 2026-11-13
> **Purpose**: one readable report per planned day (newest first), written by the `checkpoint` skill.
> Raw evidence lives in [PROGRESS.md](PROGRESS.md), the per-session file trace in
> [SESSION_LOG.md](SESSION_LOG.md), the reasons in [DECISION_LOG.md](DECISION_LOG.md).
> Days W1.D1–W1.D3 closed before this log existed; their evidence is in PROGRESS and SESSION_LOG.

---

## W4.D21 · Customer 360 and RFM — closed 2026-11-13

**Executive summary.** Clickstream sessions now belong to real customers: the generator draws identified sessions from the same-brand CDC customer pool, and the shared clickstream data was rebuilt with the owner's approval through an audited reset job. On top of that, Gold has a Customer 360 table and per-brand RFM segments that are reproducible for a fixed `as_of_date`, plus one view per tenant.

- **Delivered**: W4.D21.0 ✅ (clickstream ↔ customers, rebuild) · W4.D21.1 ✅ (`obt_customer_360`) · W4.D21.2 ✅ (`agg_rfm_segments`, dbt unit tests) · W4.D21.3 ✅ (docs complete, `gold_tenant` views)
- **Evidence**:
  - Commits `3e49b32`, `6828076`, `f3dd0b2`, `de4dc63`. `dbt build --select staging marts` → PASS=146 WARN=0 ERROR=0; after the contract rename `dbt build --select obt_customer_360+` → PASS=29; after the review fixes PASS=30 (4 unit tests, non-degenerate test).
  - Rebuild: Bronze 5200 = Silver 4925 + DLQ 50 + duplicates 200 + parse 25; 0 orphan identified sessions (1706/913/279).
  - RFM: 5 segments in every brand, 5.9–41.2 %; identical checksum on two builds with the same `as_of_date` (also after the review fix), different with another date.
  - `check_dbt_docs.py` → 15 models, 0 gaps; unit 1661 passed / 1 skipped, coverage ≈ 80.32 % (after the review fixes); mypy clean, lint-imports 2 kept.
- **Reviews / audit**: code review APPROVE WITH NITS, no must-fix; both should-fix applied (truncated `recency_hours` via portable `whole_hours_between` — dbt-spark `datediff` rounds up; OBT unit test for the day cut, cross-brand id, cancelled revenue, no-order customer, with a negative check) and nits 1, 3, 4, 5 (brand regex `{1,40}\Z`, tie-break wording, negative `tenure_days` documented, reset metrics-delete test); nit 2 documented (views of removed brands are not dropped).
- **Decisions**: D-097 (generator pools, audited rebuild, `as_of_date`, valid orders, hours recency, RFM rules, unit-test override, tenant views, contracts v8.1).
- **Found during the day**:
  - The shared CDC was produced by an older generator version: 6 accepted inserts of the current seed run were absent from Silver; fixed additively with `produce --only cdc` (no deletion).
  - dbt-core 1.12.6 unit tests fail on the Thrift Server (`run_query` on a DDL returns no result) → spark override of the `unit` materialization (V-14).
  - dbt-spark post-hooks cannot use `run_query` for DDL either → `adapter.execute(fetch=False)`.
  - DATA_CONTRACTS §5.2 listed RFM columns inside the OBT although RFM is built from the OBT (cycle) → contract v8.1.
  - `reconcile` reports a `raw_orders` batch-sum mismatch (231 vs 240, 18 vs 9; totals equal) — orders untouched today (V-28 → W4.D23.2).
- **Infra / config touched**: Kafka topic `clickstream_events` recreated (same settings); `dbt_project.yml` vars `as_of_date`, `tenant_brands`, post-hook on `marts/analytics`.
- **Not verified / limitations**: customer attributes in the OBT are the current Silver state, not the state at `as_of_date`; RFM on 2 days of synthetic history (recency in hours); portable SQL of the new models untested on Athena (W5.D28.4); tenant views are not access control.
- **Carry-overs**: V-28 orders reconcile mismatch → W4.D23.2.
- **Risks**: coverage headroom ≈ 0.32 pt (V-24).
- **Learning queued**: OBT vs star schema, reproducible metrics (`as_of_date`), NTILE and ties, dbt unit tests vs data tests.
- **Next day**: W4.D22 — Airflow 3 image (owner approves the build), `lakehouse_pipeline` DAG, ADR-0010.
- **Merge status**: merged 2026-11-13 as `dbb1b8e` (PR #18; CI 6/6 green, review APPROVE WITH NITS with fixes, audit PASS after 2 fixes, D-032).

---

## W4.D20 · Dimensions and facts — closed 2026-11-13

**Executive summary.** The Gold layer exists: four dimensions and three facts in `gold_marts`, built by dbt from staging, with brand-aware surrogate keys and tenant-aware foreign-key tests on the order, item, product and customer-history links (the session link is at warn, V-26). A singular test proves per brand that no order, item or session is lost between staging and Gold. `silver.products` now exists as a Spark job, so `dim_products` reads Silver like everything else. The SQL is designed to run on Athena too (owner decision D-093); that branch is untested until W5.D28.4. One real data defect surfaced: the generator gives clickstream sessions random customer ids, so sessions do not link to customers yet; that test is a visible warning and is fixed first in W4.D21.

- **Delivered**: W4.D20.0 ✅ (`silver.products` + `stg_products`) · W4.D20.1 ✅ (dimensions) · W4.D20.2 ✅ (facts)
- **Evidence**:
  - Commit `ff84b4b`. `dbt build --select staging marts` → PASS=117 WARN=1 ERROR=0 (Thrift session UTC).
  - `silver_products` on the shared catalog: 120 → 120 accepted, inserted 120; reruns inserted 0 / updated 0.
  - Spark `test_silver_products.py` 53 passed, `test_tables.py` 11 passed; unit 1646 passed / 1 skipped, coverage 80.41 %; ruff/format/mypy clean.
  - Per-brand counts and net sums: PROGRESS W4.D20 Evidence.
- **Reviews / audit**: code review APPROVE WITH NITS; 4 should-fix applied (numeric line-number tie-break for products, dim_date calendar test, error-level cross-tenant check on sessions while the FK test warns, UTC pinned on the Thrift Server) plus the nits (real `check_equation` test, null `customer_sk` on orphans, empty-staging guard in the reconcile test).
- **Decisions**: D-093 Gold also on Athena (W5.D28.4) · D-094 `silver.products` · D-095 Gold conventions · D-096 clickstream customer-id defect.
- **Found during the day**:
  - Generator clickstream: identified sessions get fresh UUIDs, so 3879/3879 reference no customer (D-096, V-26).
  - The products tie-break on `_source_ref` was lexical (`#L9` after `#L10`) and all 120 rows share one `_ingest_ts`; fixed with numeric line order.
  - dbt folder names are not selectors on their own (`--select dims` matches nothing); tags `dims`/`facts` added, PROGRESS verify updated.
  - The Thrift Server cannot return `min()/max()` of a DATE column served from Delta statistics (V-27).
  - The Thrift Server ran in UTC only by JVM default; now pinned (`spark.sql.session.timeZone=UTC`).
  - `sales_channel` is NULL on all 225 orders by design (present from landing batch 10).
- **Infra / config touched**: `docker-compose.yml` (Thrift Server `spark.sql.session.timeZone=UTC`), `pyproject.toml` (import-linter ignore for `silver_products → ops.metrics`, same as the other Silver jobs).
- **Not verified / limitations**: the `default__` (Trino) branch of the portable macros is untested until W5.D28.4; the empty-staging branch of the reconcile test is not exercised; contracts only on two pilot models.
- **Carry-overs**: W4.D21.0 clickstream ↔ customers fix and data rebuild (planned; owner approves the rebuild).
- **Risks**: coverage headroom 0.41 pt; a full Gold build takes ≈ 10 min.
- **Learning queued**: star schema (dimensions vs facts, grain), surrogate vs natural keys, dbt contracts, portable SQL / adapter dispatch.
- **Next day**: W4.D21 — clickstream fix, `obt_customer_360`, `agg_rfm_segments`.
- **Merge status**: pending (PR, CI, merge recorded after the merge).

---

## W4.D19 · dbt foundations — closed 2026-11-13

**Executive summary.** dbt now reads Silver through one staging view per table and has tests that
understand tenants. The key new test, `relationships_by_brand`, fails when an order points at a
customer who exists only in another brand. dbt's built-in `relationships` test misses that case,
and a small leaky fixture proves the difference (built-in: 1 failure, ours: 2). Customers, history
and clickstream are verified on real Silver data. Orders and order items are not yet: their Silver
tables are empty locally, and loading them needs one owner step (V-25).
- **Update (same day, after the owner landed orders, V-25)**: `silver_orders` 249 read = 225 accepted + 9 DLQ + 9 duplicates + 6 FK orphans, 488 items; full `dbt build --select staging` → PASS=42 ERROR=0; 1 order references a customer id that exists in two brands, so the orders leg is exercised. W4.D19.1 → ✅. `v0.1.0` tagged on the M1 merge `ac239b3` (D-092); `silver.products` decided (D-091).
- **Delivered**: W4.D19.1 ✅ (data-verified after V-25) · W4.D19.2 ✅ (tenant-safe tests, `dbt parse` in CI).
- **Evidence**:
  - Commit `7397428`; CI [run 43427736811](https://github.com/dogruom/thelook-lakehouse-platform/actions/runs/43427736811) green (`dbt parse` in both fixture states + gating check, unit, gitleaks).
  - Staging build on the shared catalog (897 customers, 2604 history rows, 12805 clickstream rows, 3 brands): PASS=25 ERROR=0, orders-dependent tests excluded.
  - `tests/integration/test_dbt_tenant_tests.py` 5 passed, 3 consecutive runs.
  - Unit 1634 passed / 1 skipped, coverage 80.91 %; ruff/format/mypy clean.
- **Reviews / audit**: code review APPROVE WITH NITS, no must-fix. All three should-fix items were applied before the commit: contract tests (exactly one current history version with `valid_to IS NULL ⇔ is_current`, tombstones without PII), the singular test's limits stated, and fixture parsing in CI. The nits were applied too: the macro `label` is now required and the `dbt ls` test is stronger. `evidence-auditor` was not routed (2 tasks, no 🔺).
- **Decisions**: D-088 staging conventions · D-089 tenant-safe test primitives, fixtures as views · D-090 V-15 closed, V-24 re-targeted, V-25, `silver.products` gap.
- **Found during the day (not in the plan)**:
  - dbt ignores SQL tests in `tests/fixtures/` without any warning, because that folder is reserved for unit-test fixtures. The fixture test now lives in `tests/tenant_fixtures/`.
  - dbt-spark seeds are Delta managed tables. Dropping one leaves its S3 location behind, so the next run fails with `LOCATION_ALREADY_EXISTS`. The fixtures are now `VALUES` views, and a retry loop that hid this was removed.
  - The pre-commit hook caught invalid YAML in the new CI step (`: ` inside a plain scalar) and a gitleaks false positive on a macro signature. The fixes were a block scalar and renamed parameters; no ignore entry was added.
  - The shared catalog had no Silver schema. The pipeline was re-run without a namespace. The orders landing upload failed: the host has no S3 credentials, and the guard hook blocks the bind-mount upload for agents.
  - The lineage diagram shows `silver.products`, but the table does not exist.
- **Not verified / limitations**:
  - `stg_orders` and `stg_order_items`, plus the orders legs of the tenant tests, have no data-backed run yet (V-25).
  - The clickstream leg of `assert_no_cross_tenant_joins` is vacuous on today's data. Three customer ids are shared across brands, but no clickstream row references them.
  - V-15 was closed on one observation, not a load test.
- **Carry-overs**: none (the W4.D19.1 data verification was done the same day after V-25).
- **Risks**: `silver.products` must be built before `dim_products` (D-091); V-24 coverage headroom.
- **Learning queued**: dbt sources/staging, generic vs singular tests, tenant-safe referential tests, var-gated negative fixtures (TASKS Learning queue).
- **Next**: W4.D20: `silver_products` (D-091), then dimensions and facts.
- **Merge status**: PR [#16](https://github.com/dogruom/thelook-lakehouse-platform/pull/16) merged as `90d4f4f` (merge commit) on 2026-11-13 under D-032: PR CI green, code review APPROVE WITH NITS, `evidence-auditor` gate PASS (after a 2-line doc fix), `main` CI [run 43441915188](https://github.com/dogruom/thelook-lakehouse-platform/actions/runs/43441915188) success.

---

## W3.D14–D18 · Silver correctness — Week 3 closed 2026-11-13

**Executive summary.** Silver now holds data that can be trusted:
- Customer changes are applied in source order, even when they arrive late or twice, and customer history is rebuilt correctly.
- Orders whose customer is not there yet wait in a retryable DLQ, and a replay recovers them.
- Every batch balances: Bronze in = accepted + DLQ + duplicates + no-ops + too late (the customers data-level check is weak, V-23).

Two proofs ran on the real stack. First, a job killed in the middle of a batch and then re-run produced exactly the same Silver content as a clean run. Second, a join that forgets `brand_id` was detected.
- **Delivered**: W3.D14.1–D14.2 ✅ · W3.D15.1–D15.3 ✅ · W3.D16.1–D16.2 ✅ · W3.D17.1–D17.3 ✅ · W3.D18.1–D18.4 ✅ (details and evidence in [PROGRESS Week 3](PROGRESS.md)).
- **Evidence**:
  - Commits: `505143c`, `bbba657`, `51ca27b`, `8f495d5`, `04d5f20`, `c6ab4b9`.
  - Unit 1634 passed / 1 skipped, coverage 80.91 %; Spark full suite 514 passed (+ `test_isolation.py` 6 after the fix).
  - Integration W3 set: CDC 23, SCD2 11, orders 13, replay 9, reconciliation 9, tenant isolation 7, idempotent re-run 5 (0 skipped; the checkpoint proves the kill landed mid-batch).
  - M1 regression integration 50 passed / 1 skipped (broker chaos opt-in).
  - ruff/format/mypy/import-linter clean.
- **Reviews / audit**: code review APPROVE WITH NITS, no must-fix; should-fix 2–3 applied (`04d5f20`), should-fix 1 recorded as V-23. `evidence-auditor` (Haiku): gate PASS, 5 MISMATCH, all docs — a W3.D15.2 verify command naming a non-existent `test_pii.py` integration file, the Spark count missing the 6 post-fix isolation tests, a portfolio line generalising one crash point to "crashes", the balance claim without the V-23 caveat, and a stale PROGRESS header date; all fixed. Auditor re-runs: unit 1597 passed / 1 skipped, ruff/format/mypy clean.
- **Decisions**: D-073 … D-087.
- **Found during the close (not in the plan)**:
  - `isolation_check` crashed with an ambiguous self-join on the first stack run. The Spark unit tests had used two separate frames, which hid it. Fixed with a regression test.
  - The test world's 1 % collision rate produced no cross-brand collisions, so the leaky-join check was vacuous. The test now uses 10 % (D-087).
  - The S3 listing helper used an option that `aws s3 ls` rejects, so checkpoints looked empty.
  - PR CI failed the unit coverage gate (78.71 % < 80 %): Spark-container tests do not count towards host coverage. 37 host unit tests for the pure-Python parts brought it to 80.91 %; threshold unchanged (V-24 thin headroom).
  - The re-run comparison included `_bronze_batch_id`, which differs per namespace, so it is now excluded (D-087).
- **Not verified / limitations**: V-23 customers data-level reconciliation is weak; the M1 regression wall time is not representative (host idle); the demo was not run separately; the tutor gate is pending.
- **Carry-overs**: none (V-15 metastore concurrency → W4.D19 by plan).
- **Learning queued**: CDC ordering, SCD2 rebuild, retryable DLQ/replay, reconciliation equation, crash-safety proof, tenant isolation (TASKS Learning queue).
- **Next**: W4.D19 dbt foundations.
- **Merge status**: PR [#15](https://github.com/dogruom/thelook-lakehouse-platform/pull/15) merged as `23cdb97` (merge commit) on 2026-11-13 under D-032: PR CI green after the coverage fix (`769ab4f`), `main` CI [run 43326199577](https://github.com/dogruom/thelook-lakehouse-platform/actions/runs/43326199577) success.

---

## W3.D13 · Batch landing and Bronze batch ingest — closed 2026-11-11

**Executive summary.** The platform now has its second way in: besides events from Kafka, order
and product files dropped into a landing folder reach Bronze with the same guarantees — the
integration test shows one Bronze row per landed line, each pointing back to its file and line; a
rerun adds nothing (the checkpoint remembers processed files and each write is idempotent), and
each test run can be isolated in its own folder. The order data carries realistic
defects with a manifest that says, line by line, what should happen to each one later.
- **Delivered**: W3.D13.1 ✅ orders/products generator, S3 landing writer, `land` CLI · W3.D13.2 ✅ `bronze_batch` · W3.D13.3 ✅ integration count test.
- **Evidence**: `be2ae2b`; unit 1293 passed / 1 skipped, coverage 83 %; Spark batch tests 14 passed, full Spark 307 passed; integration `test_bronze_batch.py` 7 passed (rerun on the final tree, 173 s) — rows per file = manifest line count, `sales_channel` NULL before batch 10, rerun adds nothing; `land --dry-run` byte-identical across runs; ruff/mypy/import-linter clean.
- **Reviews / audit**: code review APPROVE WITH NITS — should-fix: docstring overclaimed `mergeSchema` as the cause of `sales_channel` (it is the contract projection), run-level defect budget untested at fractional rates; nits: vacuous cross-tenant test, missing wrong-type / empty-file / rewound-checkpoint tests; all applied, contract wording aligned (v7.8). `evidence-auditor`: 7 MISMATCH, all docs — exactly-once-sounding summary sentence, writer retry wording, Spark/integration counts from before the review fixes (both suites rerun on the final tree), "landing write" vs Bronze write in DATA_CONTRACTS, missing close hash, merge status pending by convention; fixed. Auditor re-runs: unit 1293/1 skipped, 83 %, `land --dry-run` byte-identical (78 files).
- **Decisions**: D-069 late-parent defect deferred to W3.D17, namespaced landing · D-070 orders generator rules · D-071 `bronze_batch` design.
- **Found during the day (not in the plan)**: a namespaced `bronze_batch` run read every landing file in the bucket (found by the test-author before the integration test was written; fixed, D-069); Spark `from_json` keeps a wrong-typed field as NULL without a parse error (V-20, input for the W3.D17 gate).
- **Not verified / limitations**: V-20 wrong-typed lines need the orders gate (W3.D17) · V-21 landing keys must stay immutable · V-22 boto3 writer only against moto, not SeaweedFS (W5.D27).
- **Carry-overs**: none.
- **Learning queued**: incremental file ingestion, line lineage, path vs payload brand, schema evolution, PERMISSIVE parsing (TASKS Learning queue).
- **Next**: W3.D14 Silver customers (with D14.0 late CDC generator, Q-003).
- **Merge status**: PR [#14](https://github.com/dogruom/thelook-lakehouse-platform/pull/14) merged as `b37a5a6` (merge commit) on 2026-11-11 under D-032: PR CI green, `main` CI [run 42938974449](https://github.com/dogruom/thelook-lakehouse-platform/actions/runs/42938974449) success.

---

## W2.D12 · M1 review, demo and release — closed 2026-11-11

**Executive summary.** Milestone 1 (reliable streaming ingestion) is technically complete. The demo
runs from a stopped stack in about 16 minutes and is written down step by step; the milestone
review found gaps in the tenant checks on the Kafka key and header, a flaky smoke start and
unpinned image downloads, and all of them were fixed and re-tested on the full suite. What is left
needs the owner: the `v0.1.0` tag, the learning gate and a few local clean-ups.
- **Delivered**: W2.D12.1 ✅ demo runbook · W2.D12.2 ✅ full regression · W2.D12.3 🔄 review, README status and can/cannot-claim note done; `v0.1.0` tag owner-only (V-18).
- **Evidence**: demo recorded on `aa7f9e9` (16 min 11 s, numbers in the [runbook](runbooks/m1-streaming-demo.md)); `0ba2cc2` review fixes; regression on the rebuilt pinned image: unit 1222 passed / 1 skipped, Spark 291 passed, integration 35 passed with broker chaos (1 h 24 min), lint/types/boundaries clean; smoke 24/24 after an 8 s readiness wait. Runbook `2aaa8d0`. Post-review fixes `782ec39`: unit 1222 passed / 1 skipped, Spark 293 passed, purge-glob test 9 passed.
- **Reviews / audit**: milestone review (reviewer, code + data + security) → findings applied (D-062–D-066). Post-fix code review: APPROVE WITH NITS — purge glob too wide (`*/it_*`), non-UTF-8 Kafka key passed unchecked, unused import; all applied (D-068), the two optional robustness nits (container without healthcheck, JAR hashes taken from the earlier image) recorded below. `evidence-auditor` (milestone): 4 MISMATCH, all docs — TASKS D12.3 status vs owner tag, runbook not yet committed (now `2aaa8d0`), README Spark count 291 → 293, README "delivered" → "technically complete" while the tag is pending; all fixed. Offline re-runs by the auditor: unit 1222/1 skipped, purge-glob 9 passed, ruff/mypy/import-linter clean.
- **Decisions**: D-062 DLQ envelope as built · D-063/D-064 transport rules · D-065 shared brand key · D-066 smoke wait, `it_*` purge, image pins, CI concurrency · D-067 Hive schema deferred · D-068 non-UTF-8 key, narrowed purge.
- **Not verified / limitations**: JAR SHA-256 pins were computed from the previously built image, not checked against Maven Central independently (agent network blocked); `stack_ready` assumes every core container has a healthcheck (true today). Owner actions V-09, V-10, V-11, V-13 (deferred), V-17, V-18, V-19 in [VERIFICATION_REGISTER.md](VERIFICATION_REGISTER.md).
- **Carry-overs**: none for M1 code. Q-003 (Silver `_parse_error` exclusion) belongs to W3.D14; D-030 late CDC to W3.D14.0.
- **Learning**: M1 learning gate started (round 1 asked); owner continues it later (V-11).
- **Next**: W3.D13 batch landing + Bronze batch.
- **Merge status**: PR [#13](https://github.com/dogruom/thelook-lakehouse-platform/pull/13) merged as `ac239b3` (merge commit) on 2026-11-11 under D-032: PR CI green (lint/types/boundaries/unit, gitleaks), `main` CI [run 42931477510](https://github.com/dogruom/thelook-lakehouse-platform/actions/runs/42931477510) success.

---

## W2.D11 · Catalog and dbt capability spike — closed 2026-11-10

**Executive summary.** The tables written by the Spark jobs now have names (`bronze.raw_customers`,
`dead_letter.dlq_customers`, …) in a catalog stored in PostgreSQL, and the gate and reports read
them by name. dbt connects to Spark through the Thrift Server and every feature the Gold layer
is planned to use was tried on small probe tables: incremental merge, a composite tenant key, enforced
contracts and documentation comments. Two dbt-spark gaps were found and worked around; the
decision is to keep the Thrift Server, no fallback needed.
- **Delivered**: W2.D11.1 ✅ metastore + `register_tables` + reads by name · W2.D11.2 ✅ Thrift Server + dbt probes · W2.D11.3 ✅ ADR-0004 spike table and decision.
- **Evidence**: commit `f2f07f9`; `register_tables` → 3 registered + 1 exists, rerun all exists; `dlq_gate` by name → 3060 / 15600 rows, DLQ 210 / 150, inserted 0; Thrift `SHOW TABLES IN bronze` / `dead_letter` → 4 tables, `count(*)` 3060; `dbt debug` OK; `dbt build --select spike` PASS=8 in round 1 and round 2; negatives fail as designed (wrong expectation FAIL 3, contract mismatch, NULL insert rejected). Full suite: unit 1195 → 1203 after fixes, Spark 269, integration 34 passed / 1 skipped (71 min), lint/types/boundaries clean.
- **Reviews / audit**: code review APPROVE WITH NITS — 2 major (registration failure after a committed ingest hid the counters; D-061 overclaimed "all capabilities") and 5 nits, all applied. `evidence-auditor`: 4 MISMATCH (stale M1 macro count 29 → 34/37, dbt verify command, ADR-0004 status wording, "every capability Gold needs" overclaim), all fixed in the follow-up docs commit; stack-run evidence classed as not re-verifiable offline, internally consistent.
- **Decisions**: D-060 catalog wiring and naming rules · D-061 dbt toolchain, Thrift kept, contract style, macro override.
- **Found by the spike (not in the plan)**: DataNucleus FK auto-creation self-deadlocks on PostgreSQL at the first `CREATE TABLE` (lock wait seen in `pg_stat_activity` + thread dump) → FK-less schema; a container restarted with an older rendered `hive-site.xml` reproduced it once (all Hive clients must restart after a config change); OSS Delta rejects `SET NOT NULL` after CTAS; dbt-spark's column-comment macro fails over PyHive.
- **Not verified / limitations**: V-13 FK-less metastore · V-14 dbt-spark workarounds to re-test on upgrades · V-15 job and dbt on the metastore at the same time (W3) · V-16 Thrift Server without auth (local only) · Hive DDL behaviour on Glue/Databricks (W5/W6).
- **Carry-overs**: none.
- **Risks**: Thrift Server holds 1 of the 2 worker cores — stop it before job runs and integration tests (`docker compose --profile sql stop spark-thrift`).
- **Learning queued**: Hive metastore internals, external Delta tables, Thrift/JDBC, dbt adapter capability limits (TASKS Learning queue).
- **Next**: W2.D12 (M1 review, demo and release).
- **Merge status**: PR [#12](https://github.com/dogruom/thelook-lakehouse-platform/pull/12) merged as `aa7f9e9` (merge commit) under D-032; `main` CI [run 42722466999](https://github.com/dogruom/thelook-lakehouse-platform/actions/runs/42722466999) success.

## W2.D10 🔺 · Validation gate and DLQ — closed 2026-11-09

**Executive summary.** Bronze rows that break the data contract now go to dead-letter tables, one
row per rejected message, with a fixed rule code, an error category and a link back to the Bronze
row. Unparseable messages stay in Bronze, as designed. The rejected set matches the generator's
expected set exactly, and a second run adds nothing.
- **Delivered**: W2.D10.1 ✅ rules + `dlq_gate` + `dlq_report` · W2.D10.2 ✅ per-rule Spark tests · W2.D10.3 ✅ DLQ reconciliation integration test.
- **Evidence**: commits `5f64ad4`, `c91e03f`; default run: CDC 3060 read / 210 DLQ, clickstream 15600 read / 75 parse errors (Bronze only) / 150 DLQ; rerun inserts 0; Spark 178 (`test_rules.py`), 258 total; integration 14 (DLQ module), day-close full run on `5f64ad4` 34 passed, 1 skipped (broker chaos opt-in, D-059); unit 1146 at `5f64ad4`, 1154 after `c91e03f`, coverage 79 %; ruff, format, mypy, lint-imports clean. Details: PROGRESS D10 Evidence.
- **Reviews / audit**: design review before implementation; code review must-fix: vacuous reconciliation check (MERGE metric compared with itself) → independent post-write checks with a negative unit test; `duplicates_removed` reserved for Silver (renamed `rejected_duplicates`, `bronze_duplicate_rows`); canonical `produced_at` in the signup rule; `event_id` masked in report logs. Re-check APPROVE WITH NITS; nits fixed in `c91e03f` (no concurrency guarantee in the docstring, masking and constant-parity tests). Carried over: mapping of gate log terms to `ops.pipeline_metrics` terms (W3.D18).
- **Decisions**: D-056 rules and codes · D-057 gate design, checks, tenant attribution, masking · D-058 Spark-vs-Pydantic divergences, frozen fixtures · D-059 test cadence; Q-001 queued.
- **Not verified / limitations**: Spark rules differ from Pydantic in documented cases (V-12, e.g. a JSON number in a string field passes); the gate rescans all of Bronze per run (O(n), watermark later); one gate at a time per namespace (V-05); tables are path-based until W2.D11 (V-02). V-08 did not recur.
- **Risks**: none new.
- **Learning queued**: validation as data, error taxonomies, quarantine patterns, insert-only MERGE, null-safe join.
- **Next**: W2.D11 (catalog and dbt capability spike).
- **Merge status**: PR to `main` · code review re-check APPROVE WITH NITS, no must-fix · `evidence-auditor` no MISMATCH, 2 commit-tie wording fixes applied · merged under D-032 once CI is green.

## W2.D9 🔺 · Restart and checkpoint loss — closed 2026-11-09

**Executive summary.** Bronze survives crashes before and after the write, a killed driver, an
exception in the batch callback and a broker outage: after a restart every Kafka offset is in Bronze
once per table, proven per partition against the Kafka watermarks. The tests found one real
defect: when only part of a checkpoint was lost, the job silently skipped new data. That case now
fails loudly and leaves Bronze unchanged. Every earlier "not verified" line is now either closed or
listed with an owner day in [VERIFICATION_REGISTER.md](VERIFICATION_REGISTER.md).
- **Delivered**: W2.D9.1 ✅ restart and loss suite S1–S9 · W2.D9.2 ✅ ADR-0006 draft · submit guards (capacity, duplicate driver, timeout kill, stale `--skip-build`) · verification register.
- **Evidence**: commit `c23e322`; integration **21 passed** in 62 min with `RUN_BROKER_CHAOS=1`, no skip; S9 before the fix: 102 of 204 CDC and 208 of 416 click rows (silent loss), after: exit 1, "checkpoint rewound", rows unchanged; S7 exit 1 after 77 s, rerun exact; unit 1136 passed (1137 after the nit fix); ruff, format, mypy clean. Details: PROGRESS D9 Evidence.
- **Reviews / audit**: design review before implementation; code review found 5 must-fix (vacuous S4, silent `kill_driver`, prune of an in-use zip, `close()` orphaning drivers, guards test order), all fixed; re-check APPROVE WITH NITS (all 5 confirmed fixed in code); nits fixed: inaccurate prune comment, a job ending exactly at the deadline reported 125 instead of its own code (new unit test). Carried over: exit-code overlap with spark-submit's own codes not documented, maintenance jobs (OPTIMIZE) not named in the single-writer note, leftover `.tmp` zips not pruned. `evidence-auditor`: no MISMATCH; 4 wording fixes applied (offset wording, S7 figure, V-06).
- **Decisions**: D-049 fault-injection job · D-050 checkpoint-rewind guard · D-051 content-addressed zip and submit pre-flight · D-052 stop grace period · D-053 empty-topic tables (corrects D8) · D-054 input allowlists · D-055 verification register.
- **Not verified / limitations**: all in the register (V-01…V-11). Main ones: Delta commit safety on S3/Glue (W5), concurrent writers (known limit, single writer), D-030 late CDC (W3.D14.0), one unexplained flaky failure of the D8 count test (V-08, did not recur in the 21-pass run).
- **Risks**: Bronze now refuses to run after a partial checkpoint loss; recovery is a documented manual step (new checkpoint path, then Silver dedup), see ADR-0006 operating rules.
- **Learning queued**: checkpoint anatomy (`offsets/`, `commits/`, `metadata`), Delta idempotent writes and their blind spot, `failOnDataLoss`, fault injection, delivery vocabulary.
- **Next**: W2.D10 (validation gate and DLQ).
- **Merge status**: [PR #10](https://github.com/dogruom/thelook-lakehouse-platform/pull/10) · CI green on the code commits (runs 42665055877, 42665043767; lint/types/boundaries/unit tests + coverage + gitleaks; CI runs no Spark or integration tests) · code review re-check APPROVE WITH NITS, no must-fix · `evidence-auditor` no MISMATCH, 4 wording fixes applied · merged under D-032 once this commit's CI is green.

## W2.D8 · Kafka → Bronze streaming — closed 2026-11-09

**Executive summary.** Kafka messages now land in Bronze Delta tables, one row per message, including
the ones that cannot be parsed. Counts match the generated messages and manifest per topic, parse-error
code and tenant, and a second run from the same checkpoint adds nothing. What happens after a crash
in the middle of a run is not proven yet; that is the D9 gate.
- **Delivered**: W2.D8.1 ✅ `bronze_stream` + `bronze_report` · W2.D8.2 ✅ parse tests · W2.D8.3 ✅ count integration test incl. rerun.
- **Evidence**: commits `b67f155`, `78f3d60`, `0792283`; Spark 71 passed; integration 7 passed, no skip; unit 1066 passed, coverage 88 %; default run: 3060 and 15600 rows, 75 `malformed_json`, `distinct_source_refs` equal to rows, metadata null counts 0. Details: PROGRESS D8 Evidence.
- **Reviews / audit**: design review APPROVE WITH CHANGES, 9 items applied, one replaced by measurement: the suggested `cast` round trip does not detect invalid UTF-8 in Spark 3.5, so `decode`/`encode` is used. Code review (data + security) APPROVE WITH NITS, no must-fix; rerun test, `_await_all` tests, clear missing-table error and the docstring fix applied. `evidence-auditor`: re-ran unit 1066, Spark 71, ruff, mypy; one MISMATCH (W2.D8.2 Verify command could not run on the host), fixed. Integration, job runs, coverage, ruff format and lint-imports were run by the coordinator, not re-run in the audit.
- **Decisions**: D-047 (headers persisted, ADR-0002 and DATA_CONTRACTS amended; app id = query id; stable parse codes; `event_date` from Kafka time as fallback; no `mergeSchema`), D-048 (exception text in logs, a D7 defect found through the negative run).
- **Not verified / limitations**: replay after a real mid-stream failure (D9). `_await_all` failure path with fake queries only. Trailing garbage after a JSON object is not flagged. Empty first run has no integration case; a missing table is covered by a manual negative run. Tables are path-based until W2.D11.
- **Carry-overs (none blocking)**: D10 gate should compare payload `brand_id` with key and header, and reject NULL `brand_id` · `rows_by_brand` keys for parse-error rows come from an unvalidated header (unbounded in logs) · integration runs leave `bronze/it_*` and `checkpoints/bronze_stream/it_*` in SeaweedFS · D7 `layer_uri` character nits still open · guard hook false positive: `--rm` followed later by a flag containing `r` (`-rs`, `-Pattern`) reads as recursive deletion.
- **Risks**: a job waits without a timeout when no worker is up; `seaweedfs` and `spark-worker` were found exited once after `up -d`, cause not diagnosed; Kafka and SeaweedFS exit 137 on stop; `submit_job.py` is not safe to run concurrently (two exit-2 failures while two agents built `dist/`; each passed on the immediate retry, consistent with this cause but not proven).
- **Learning queued**: Kafka source options, `foreachBatch` vs sink, checkpoints, Delta transaction log and `txnAppId`. Tutor gate deferred to the Learning queue (D-046).
- **Next**: W2.D9 🔺 (restart and checkpoint loss).
- **Merge status**: [PR #9](https://github.com/dogruom/thelook-lakehouse-platform/pull/9) · CI green (runs 42634087351, 42634101850: lint/types/boundaries/unit tests + coverage + gitleaks; CI runs no Spark or integration tests) · code review APPROVE WITH NITS, no must-fix · `evidence-auditor` one MISMATCH, fixed · merged under D-032 once this commit's CI is green.

## W2.D7 · Spark job framework — closed 2026-11-09

**Executive summary.** Spark jobs now have one way to start: a session built from settings, a run ID
on every log line, and code packaged and submitted to the standalone cluster from the host. A small
job writes and reads a Delta table on object storage, and a negative run shows the shipped package,
not the source mount, is what runs. Bronze (D8) can build on this.
- **Delivered**: W2.D7.1 ✅ session factory + job framework · W2.D7.2 ✅ `spark-test` in the image · W2.D7.3 ✅ wheel + `submit_job.py`.
- **Evidence**: commits `e82f91b`, `5836709`, `80a6e63`, `44b5ea7`; Spark tests 22 passed; `hello_delta --expect-packaged --rows 7` exit 0 (7 written, 7 read, Delta version 0); without `--py-files` exit 1; unit 1045 passed, coverage 93 %; smoke 24/24; integration 6 passed. Details: [PROGRESS D7](PROGRESS.md).
- **Reviews / audit**: no design review (the interface was already fixed by the tests from the previous session). Code review (data + security) APPROVE WITH NITS, no must-fix; 6 nits applied in `44b5ea7`. `evidence-auditor`: see Merge status.
- **Decisions**: D-044 (job resolution, packaging, `spark-test` without `core`), D-045 (money and timestamps stay strings until Silver; the "string→DECIMAL unverified" risk is now measured in Spark tests; the Silver check itself comes in W3).
- **Incident (2026-11-09).** `.env` holds a stale `COMPOSE_PROJECT_NAME`, a problem already noted in the week-1 runbook. `docker compose --profile core up -d` ran without the override, so compose used the legacy `thelook_lakehouse` project and tried to recreate its containers under the new names. Result: the stopped legacy container `thelook_spark_master` was **removed**, and four containers that never started were left behind (`68ad88a828c4_crm_postgres`, `9e6c2e87c9e5_crm_seaweedfs`, `82fe4b6afa84_crm_kafka`, `9a792f08c345_crm_spark_master`). Legacy volumes, images and the other six legacy containers still exist; `crm_*` containers and volumes were not affected. Anything stored only in that container's writable layer is lost; this could not be checked afterwards. Nothing was cleaned up, because `docker rm` needs the owner.
- **Not verified / limitations**: `aws` profile never run on AWS (W5). `--expect-packaged` proves location on one executor, not freshness with `--skip-build`. `stale_modules` catches deleted modules, not edited ones. Delta on SeaweedFS exercised for one write and read only; no concurrent writers.
- **Carry-overs (review nits, none blocking)**: `layer_uri` control characters, `?`, `#` (before Bronze paths use it) · reject `user:pass@` in `s3_endpoint` · `setLocalProperty` / `setLogLevel` on Databricks serverless (W6) · `resolve_job` `TypeError` has no structured log · content hash in the zip · `docker/spark/requirements-spark.txt` is stale (pins Spark 3.5.1 / Delta 3.2.0) and unused by the Dockerfile · harmless py4j "Logging error" line at pytest exit.
- **Risks**: `.env` project name until the owner fixes it. Kafka and SeaweedFS exited 137 on the last stop (`stop_grace_period` check still open).
- **Learning queued**: driver vs executor code shipping, config layering, run IDs, `from_json` leniency. Lab: `.claude/learning/labs/W2_LAB.md`. Tutor gate deferred to the Learning queue (needs the owner).
- **Next**: W2.D8 (Kafka → Bronze).
- **Merge status**: [PR #8](https://github.com/dogruom/thelook-lakehouse-platform/pull/8) · code review APPROVE WITH NITS, all applied or carried over · `evidence-auditor` no MISMATCH (3 wording fixes applied) · D-032 merge once CI is green; CI runs and the merge commit are on the PR page.

## W1.D6 · Week 1 consolidation and week checkpoint — closed 2026-11-06

**Executive summary.** Week 1 is closed: contracts, a deterministic generator for CDC and
clickstream with exact defect manifests, a keyed idempotent producer and a hardened local stack,
all behind CI quality gates. The event interface is now an accepted ADR, and a runbook lets a
reviewer reproduce the demo.
- **Delivered**: W1.D6.2 ✅ ADR-0002 · W1.D6.3 ✅ runbook + week checkpoint · W1.D6.1 🔄 per-package coverage gate (green locally; CI link after push).
- **Evidence**: commit `12ac263`; coverage generator 95 %, contracts 96 %; regression set green (see PROGRESS "Week 1 checkpoint").
- **Reviews / audit**: ADR reviewed in the D5 code review (one wording fix); week `evidence-auditor`: no MISMATCH (re-ran generator 392 passed, clickstream hashes identical, round trip 3 passed); gate provisional until the CI link on `main`; two wording fixes applied.
- **Decisions**: D-037 (per-package coverage), D-038 (ADR-0002).
- **Not verified**: tutor week-gate answers pending from the owner (re-teaching material delivered: W1 guide + 3 artifacts).
- **Week-gate follow-ups (closed the same day)**: Java-compatible partitioner (D-039, live probe 157/200 → 0/200), broker-outage and CLI end-to-end integration tests, accounting invariant (D-040), demo timed from a stopped stack (105 s), and a wrong stop command found and fixed (`docker compose stop` skipped profiled services).
- **Carry-overs**: D6.1 CI link; D-030 CDC lateness before SCD2 tests.
- **Next**: W2.D7 (Spark job framework).
- **Merge status**: [PR #5](https://github.com/dogruom/thelook-lakehouse-platform/pull/5) · CI green (runs 42218324701, 42218357517: lint/types/boundaries/unit tests + per-package coverage + gitleaks) · merged under D-032 as `3da43d1`; W1.D6.1 → ✅. Week-gate follow-ups: [PR #6](https://github.com/dogruom/thelook-lakehouse-platform/pull/6) · CI green (runs 42226475387, 42226486809) · review APPROVE WITH NITS (applied) · audit no MISMATCH (one caveat added) · merged as `0c283c5`.

## W1.D5 · Clickstream generator and keyed idempotent producer — closed 2026-11-06

**Executive summary.** Generated events now reach Kafka with the agreed keys and headers, and the
round trip proves that every key stays on one partition (per-customer / per-session ordering) and
that the producer wrote no extra records. Clickstream has its own exact defect manifest, so Bronze
and Silver can be reconciled against it in Week 2.
- **Delivered**: W1.D5.1 ✅ clickstream model · W1.D5.2 ✅ producer + `produce` CLI · W1.D5.3 ✅ round-trip test.
- **Evidence**: commit `03d02b4`; unit 797 passed; `produce --events 5000` → 6220/6220 delivered, exit 0; round trip 3 passed against the live broker; clickstream dry-run byte-identical (seed 42).
- **Reviews**: code review (data + security) APPROVE WITH NITS — 3 MAJOR applied (CLI ValueError, flush > delivery timeout, contract doc table + manifest §2.5.1). No design review before implementation (interface fixed in the task cards) — process deviation, noted.
- **Decisions**: D-034 (clickstream constants), D-035 (malformed manifest identity), D-036 (producer settings).
- **Not verified / limitations**: "lag 0" measured for the test's own consumer group only; producer behaviour under a broker outage not tested; delivery is at-least-once (re-running `produce` re-sends the same `event_id`s).
- **Risks closed**: `confluent-kafka` 2.4.0 wheel works on Windows.
- **Learning queued**: key-hash partitioning, idempotent producer vs transactions, headers vs key, consumer lag, delivery callbacks.
- **Merge status**: together with W1.D6 (same branch).

## W1.D4 · Customer CDC generator and ground-truth manifest — closed 2026-11-06

**Executive summary.** The project can now generate deterministic, multi-tenant customer CDC events
with an exactly known set of defects, and a manifest that states the correct outcome for every event.
This is the measuring stick for the Silver MERGE in Week 2: pipeline output will be compared with it
by equality, not by eyeballing.

| Task | Status | Evidence |
|---|---|---|
| W1.D4.1 CDC lifecycle engine | ✅ | `848d9c8`; `pytest tests/unit/generator/test_cdc.py -q -k lifecycle` → 2 passed |
| W1.D4.2 Exact-count defects + manifest | ✅ | `848d9c8`; `-k manifest` → 2 passed; full `test_cdc.py` + `test_manifest.py` → 161 passed |
| W1.D4.3 Determinism | ✅ | `848d9c8`; `-k determinism` → 6 passed (incl. subprocess runs with different `PYTHONHASHSEED`); CLI seed 42 twice → identical hashes |
| W1.D2 carry-over: compose policy test | ✅ | `f72c2aa`; `pytest tests/unit/infra -q` → 48 passed |

- **Example run** (`--brands 3 --events 1000 --seed 42`): accepted 880 + DLQ 70 + duplicates 20 + stale 50 + late 0 = emitted 1020 = bronze.
- **Gates**: unit 709 passed · `lakehouse.generator` coverage 98 % · ruff / format / mypy (21 files) clean · import-linter 2 kept.
- **Reviews and audit**: design review CHANGES REQUIRED → 6 must-fix applied (D-022…D-027) · code review APPROVE WITH NITS → 1 MAJOR doc fix (D-029) + nits applied · `evidence-auditor`: all tasks OK, no MISMATCH.
- **Decisions**: D-021…D-033 ([DECISION_LOG](DECISION_LOG.md)) — key ones: gate order DLQ → dedup → MERGE (D-022); outcome precedence (D-023, D-029); batch-aware `classify_outcomes` (D-024); exact counts or error (D-026); fictional phones only (D-031); standing merge approval (D-032).
- **Not verified / known limitations**: `late = 0` by construction — CDC lateness and delays between intermediate sequences are not generated (D-030, extend before SCD2 tests); byte-identity verified only with the pinned Faker on Python 3.11; Spark parsing of money strings still unverified (W2).
- **Carry-overs**: none.
- **Risks**: `confluent-kafka` wheel on Windows untested (D5); 7.6 GB Docker memory for the D5 integration test.
- **Learning queued (batched, W1 gate)**: CDC lifecycle + sequencing, test oracle / reconciliation, deterministic generation; 6 questions; glossary + 4 entries.
- **Next day**: W1.D5 — clickstream generator, keyed idempotent producer, produce → consume round trip (not started, by owner instruction).
- **Merge status**: [PR #4](https://github.com/dogruom/thelook-lakehouse-platform/pull/4) · CI green (runs 42154153601, 42154161110: lint/types/boundaries/unit tests + gitleaks) · merged under D-032 as `2bd833d`.
