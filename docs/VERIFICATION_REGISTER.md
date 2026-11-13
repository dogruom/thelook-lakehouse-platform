# Verification register

> **Version**: 1.9 · **Last updated**: 2026-11-13 (W4.D19) · Decision: D-055

One list of every statement that a day report marked "not verified". Each row has a class and an
owner day, so nothing stays as an unowned caveat. Update this file whenever a day report names a
new unverified item, and move rows to *Closed* only with evidence.

Classes: **Closed** (verified, evidence linked) · **Planned** (has a task and a day) ·
**Known limit** (accepted, documented, no task) · **Owner action** (needs the owner, not code).

## Open

| # | Item | Class | Owner day / task | Risk if ignored |
|---|---|---|---|---|
| V-03 | Delta commit safety on AWS S3 / Glue (local SeaweedFS has no put-if-absent; single writer assumed) | Planned | W5 (Glue run) | Concurrent writers on S3 could corrupt the log; ADR-0006 results must not be carried over untested |
| V-04 | AWS profile / short-lived credentials on the owner's machine | Planned | W5.D1 | First cloud day blocked |
| V-05 | Two writers on one Bronze table at the same time | Known limit | ADR-0006 §single writer; `submit_job.py` refuses an identical running driver (best effort) | A second scheduler could double-write; no lock exists locally |
| V-06 | Spark worker dies in the middle of a batch (task retry path) | Known limit | Not covered: S3 kills the driver, not a worker | Low: Spark retries the task, the batch commit is unchanged |
| V-07 | Trailing content after a JSON object is not flagged as a parse error (D-047) | Known limit | Revisit if a producer ever emits it | A malformed record could pass Bronze parsing; D10 rules still check the contract |
| V-08 | `test_bronze_rows_equal_produced_messages…` failed once (`raw_customers 1 == 0`) and passed on rerun | Watch | Did not recur in the W2.D9 (21 passed) and W2.D10 day-close (34 passed) runs; keep watching, investigate on a second failure | A flaky test hides real regressions |
| V-11 | Tutor learning gates W1 and W2.D7–D11 (M1 learning gate; round 1 asked 2026-11-11, answers pending) | Owner action | Learning queue in [TASKS.md](TASKS.md); owner returns to it after starting W3.D13 | Owner cannot explain the work in interviews |
| V-12 | Spark rules accept a JSON number in a string field (`first_name: 5`) and differ from Pydantic on rare timestamp/integer forms (D-058, `rules.KNOWN_DIVERGENCES`) | Known limit | Revisit if a producer outside the Pydantic contracts is added | A non-conforming producer could pass a few wrong-typed values into Silver |
| V-13 | Local Hive metastore schema has no foreign keys (DataNucleus auto-create without constraints, ADR-0004 W1) | Known limit | Deferred (D-067): owner downloads the official Hive 2.3 PostgreSQL schema script only if wanted; prepared patch kept outside the repo | Orphan metadata rows possible after a crash mid-DDL; local single-user catalog only |
| V-14 | dbt-spark gaps carried in the project: `spark__alter_column_comment` override, column-level `not_null` unusable on Delta (ADR-0004 W2/W3) | Known limit | Re-test on every dbt-spark upgrade (`dbt build --select spike`) | Override may drift from upstream; a removed workaround would break `persist_docs` |
| V-16 | Spark Thrift Server has no authentication (published on 127.0.0.1 only; reachable from every container on the compose network) | Known limit | Local only; cloud targets use IAM/Glue or Databricks SQL auth (W5/W6) | A compromised local container could read/write the catalog and S3 data |
| V-21 | Overwriting an already ingested landing key with different bytes is not re-read (the file-source checkpoint tracks paths) | Known limit | Landing keys are immutable by convention; the generator is deterministic (same bytes on rerun) | A corrected file under the same key would be ignored silently |
| V-22 | `write_batch` / `write_products` against SeaweedFS (only moto-tested; the integration test lands via `*_local` + aws CLI because the host has no S3 credentials) | Planned | W5.D27 (generator writes through the AWS adapter) | An S3-API difference in the boto3 path would surface only in the cloud run |
| V-23 | `reconcile` cannot derive customers' `silver_accepted`, `duplicates_removed` and `stale_cdc_noop` from data (Silver keeps one row per key); the customers reconcile row only checks `dlq <= bronze` at data level (W3 code review should-fix 1) | Known limit | Covered instead by the CDC suite: per-batch counters equal the crafted events exactly and the final rows equal the manifest (`test_cdc_correctness.py`); a data-level check (Silver rows per brand = distinct valid Bronze keys) is a candidate for W4.D23 quality gates | A customers counter bug that keeps each batch row balanced would pass `reconcile` |
| V-24 | Unit coverage headroom is thin (80.91 % vs the 80 % CI gate): Spark-only code is tested in the Spark container, which does not feed host coverage | Planned | Next Spark-heavy change (W4.D19 changed no Python under `src/`, D-090): combine Spark-container coverage or add host tests for pure-Python parts | The next Spark-heavy change can fail the CI coverage gate without a real test gap |
| V-26 | Identified clickstream sessions reference no customer (generator draws random ids); `fct_sessions.customer_id` relationship test at `warn` (D-096) | Planned | W4.D21.0 | Customer 360 session metrics would be empty; a tenant FK test stays non-blocking |
| V-27 | Thrift Server / Delta: a bare `min()`/`max()` on a DATE column (answered from Delta file statistics) fails with `ClassCastException LocalDate → java.sql.Date`; workaround: aggregate via string (`assert_dim_date_calendar`) | Known limit | Local Thrift Server only; re-check on Athena (W5.D28.4) and Databricks | A new test or query using date min/max fails at runtime (visible, not silent) |
| V-19 | One `/security-review` run over M1 (owner-triggered command) | Owner action | Owner runs `/security-review` once on `main` after the merge | No independent security pass besides the reviewer agent |

## Closed (2026-11-13, W4.D19)

| Item (earlier wording) | Result | Evidence |
|---|---|---|
| V-17 Guard hook flags `--rm` / flag-like words as `rm` | Owner applied the left-boundary regex patch | `ec96735`; `guard_commands.py --self-test` → 132/132 passed |
| V-10 Dangling anonymous volume `5b2af2ca…` | Owner removed it | `docker volume ls -q -f dangling=true` → empty |
| V-09 Stale `COMPOSE_PROJECT_NAME` in the local env file | Owner set it to `crm_lakehouse` (file not read by the agent) | Owner confirmation 2026-11-13 |
| V-15 A running Spark job and dbt using the metastore at the same time | `register_tables` (Spark, Hive metastore) and `dbt build` of 23 staging tests (Thrift Server) overlapped 15:13:29–15:14:51; both succeeded, no lock wait or schema error. One observation, not a load test (D-090) | [DAY_CLOSE_REPORTS W4.D19](DAY_CLOSE_REPORTS.md) |
| V-25 Shared-catalog orders/order_items empty; staging orders not data-verified | Owner landed orders 2026-11-13; `silver_orders` 249 read = 225 accepted + 9 DLQ + 9 duplicates + 6 orphans; `dbt build --select staging` PASS=42 ERROR=0 | PROGRESS W4.D19.1 Evidence |
| V-18 Release tag `v0.1.0` for M1 | Annotated tag on the M1 merge commit `ac239b3` (PR #13), pushed 2026-11-13 with the owner's go (D-092) | `git ls-remote --tags origin v0.1.0` → `refs/tags/v0.1.0` |

## Closed (2026-11-12, W3)

| Item (earlier wording) | Result | Evidence |
|---|---|---|
| V-01 Generator does not produce intermediate-late CDC or `late_beyond_window` events (D-030) | Out-of-order events anchored to a later unmoved event: intermediate-late and late-after-delete, manifest `late_after_sequence` and `out_of_order_intermediate` (D-073) | `505143c`; `tests/unit/generator/test_cdc.py` (anchor, late split); integration `test_cdc_correctness.py` 23 passed incl. late update after delete, `test_scd2_history.py` 11 passed |
| V-20 Wrong-typed landing order fields reach Bronze with `_parse_error` NULL | Orders gate rules flag NULL-or-wrong-type contract fields → DLQ (D-077) | `bbba657`; Spark `tests/spark/test_rules_orders.py` in the 514-passed suite (2026-11-12) |
| Q-003 Silver must drop `_parse_error` rows before any `brand_id` join | Parse and contract exclusion inline in `silver_customers` before dedup and MERGE (D-074) | `505143c`; Spark `test_silver_customers.py` in the 514-passed suite |

## Closed (2026-11-11, W2.D12)

| Item (earlier wording) | Result | Evidence |
|---|---|---|
| Smoke test failed once right after `up -d` (2026-11-10, 23/24, rerun 24/24) | Cause: checks ran before init jobs finished; `smoke_test.py` waits up to 180 s for healthy services and init jobs exited 0 (D-066) | `0ba2cc2`; `tests/unit/scripts/test_smoke_wait.py` 4 passed; smoke after `up -d`: waited 8 s, 24/24 |
| Integration runs leave `it_*` Bronze/DLQ/checkpoint data in the bucket | Session-end purge of every `it_*` namespace (`KEEP_IT_DATA=1` keeps it) (D-066) | `0ba2cc2`; integration 35 passed, purge without warnings (2026-11-11) |
| NULL Kafka key / missing `brand_id` header passed the gate (M1 review) | Rules `kafka_key_missing`, `brand_header_missing` (D-064) | `0ba2cc2`; Spark container 291 passed |
| Spark image base and connector JARs not pinned by digest/hash | Base pinned by index digest; 9 JARs verified with `sha256sum -c` (D-066) | `0ba2cc2`; image rebuilt, full regression green |

## Closed (2026-11-10, W2.D11)

| Item (earlier wording) | Result | Evidence |
|---|---|---|
| V-02 Bronze tables are addressed by path, not by catalog name | Bronze/DLQ registered as `bronze.raw_*` / `dead_letter.dlq_*` in the Postgres-backed Hive metastore; gate and reports read by name, registered location verified; paths remain the write contract (D-060) | `f2f07f9`; `register_tables` run, `SHOW TABLES IN bronze` via Thrift, PROGRESS W2.D11 evidence |

## Closed (2026-11-09, W2.D9)

| Item (earlier wording) | Result | Evidence |
|---|---|---|
| Kafka and SeaweedFS exit 137 on stop | Cause found: SeaweedFS needs 11.1 s, Docker default 10 s. `stop_grace_period: 60s`; two stop cycles with data, no 137 | D-052 |
| Job waits forever when no worker is up | Pre-flight reads master JSON (`workers[].state`, `coresfree`, `memoryfree`) → exit 3 at once | `tests/integration/test_submit_guards.py` (live), D-051 |
| `submit_job.py` not safe to run concurrently | Content-addressed zips (`dist/lakehouse_app-<sha12>.zip`), in-use zips never pruned, identical running driver → exit 4 | `test_submit_guards.py`, `tests/unit/scripts/test_submit_stop_prune.py` |
| `--skip-build` could ship stale code | Refuses when no zip matches the source hash → exit 1 | `test_submit_guards.py` (live) |
| Timeout left the driver running in the container | Driver found by a submit marker and killed; exit 124 (125 if the stop is not confirmed) | `test_submit_guards.py` (live) |
| Replay after a mid-stream failure (D8 limit) | Crash after/before write, SIGKILL, exception in callback: every offset once per table | ADR-0006 S1–S4 |
| `_await_all` failure path tested with fakes only | Real exception in a callback fails the job, restart completes | ADR-0006 S4 |
| Broker failing mid-run untested | Broker stopped mid-run: job exits 1 in 77 s, rerun lands every offset once | ADR-0006 S7 (`RUN_BROKER_CHAOS=1`) |
| "No table on empty topic" (D8 doc) | Wrong: Delta creates an empty table. Docs and tests corrected | D-053, ADR-0006 S8 |
| Partial checkpoint loss (not in any report) | **Found real silent data loss** (102/204 CDC rows). Now refused with `CheckpointRewoundError`, rows unchanged | D-050, ADR-0006 S9, `tests/spark/test_bronze_rewound.py` |
| `layer_uri` / endpoint validation nits | Allowlist, userinfo rejected without echo | D-054 |
| `docker/spark/requirements-spark.txt` unused | Removed | D-051 |
