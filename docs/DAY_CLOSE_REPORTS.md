# Day Close Reports

> **Document Version**: 1.0
> **Last Updated**: 2026-11-10
> **Purpose**: one readable report per planned day (newest first), written by the `checkpoint` skill.
> Raw evidence lives in [PROGRESS.md](PROGRESS.md), the per-session file trace in
> [SESSION_LOG.md](SESSION_LOG.md), the reasons in [DECISION_LOG.md](DECISION_LOG.md).
> Days W1.D1–W1.D3 closed before this log existed; their evidence is in PROGRESS and SESSION_LOG.

---

## W2.D11 · Catalog and dbt capability spike — closed 2026-11-10

**Executive summary.** The tables written by the Spark jobs now have names (`bronze.raw_customers`,
`dead_letter.dlq_customers`, …) in a catalog stored in PostgreSQL, and the gate and reports read
them by name. dbt connects to Spark through the Thrift Server and every feature the Gold layer
needs was tried on real Delta tables: incremental merge, a composite tenant key, enforced
contracts and documentation comments. Two dbt-spark gaps were found and worked around; the
decision is to keep the Thrift Server, no fallback needed.
- **Delivered**: W2.D11.1 ✅ metastore + `register_tables` + reads by name · W2.D11.2 ✅ Thrift Server + dbt probes · W2.D11.3 ✅ ADR-0004 spike table and decision.
- **Evidence**: commit `f2f07f9`; `register_tables` → 3 registered + 1 exists, rerun all exists; `dlq_gate` by name → 3060 / 15600 rows, DLQ 210 / 150, inserted 0; Thrift `SHOW TABLES IN bronze` / `dead_letter` → 4 tables, `count(*)` 3060; `dbt debug` OK; `dbt build --select spike` PASS=8 in round 1 and round 2; negatives fail as designed (wrong expectation FAIL 3, contract mismatch, NULL insert rejected). Full suite: unit 1195 → 1203 after fixes, Spark 269, integration 34 passed / 1 skipped (71 min), lint/types/boundaries clean.
- **Reviews / audit**: code review APPROVE WITH NITS — 2 major (registration failure after a committed ingest hid the counters; D-061 overclaimed "all capabilities") and 5 nits, all applied. `evidence-auditor`: see merge status.
- **Decisions**: D-060 catalog wiring and naming rules · D-061 dbt toolchain, Thrift kept, contract style, macro override.
- **Found by the spike (not in the plan)**: DataNucleus FK auto-creation self-deadlocks on PostgreSQL at the first `CREATE TABLE` (lock wait seen in `pg_stat_activity` + thread dump) → FK-less schema; a container restarted with an older rendered `hive-site.xml` reproduced it once (all Hive clients must restart after a config change); OSS Delta rejects `SET NOT NULL` after CTAS; dbt-spark's column-comment macro fails over PyHive.
- **Not verified / limitations**: V-13 FK-less metastore · V-14 dbt-spark workarounds to re-test on upgrades · V-15 job and dbt on the metastore at the same time (W3) · V-16 Thrift Server without auth (local only) · Hive DDL behaviour on Glue/Databricks (W5/W6).
- **Carry-overs**: none.
- **Risks**: Thrift Server holds 1 of the 2 worker cores — stop it before job runs and integration tests (`docker compose --profile sql stop spark-thrift`).
- **Learning queued**: Hive metastore internals, external Delta tables, Thrift/JDBC, dbt adapter capability limits (TASKS Learning queue).
- **Next**: W2.D12 (M1 review, demo and release).
- **Merge status**: _pending — filled after PR / CI_.

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
