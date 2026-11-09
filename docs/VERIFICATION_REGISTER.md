# Verification register

> **Version**: 1.1 · **Last updated**: 2026-11-09 (W2.D10) · Decision: D-055

One list of every statement that a day report marked "not verified". Each row has a class and an
owner day, so nothing stays as an unowned caveat. Update this file whenever a day report names a
new unverified item, and move rows to *Closed* only with evidence.

Classes: **Closed** (verified, evidence linked) · **Planned** (has a task and a day) ·
**Known limit** (accepted, documented, no task) · **Owner action** (needs the owner, not code).

## Open

| # | Item | Class | Owner day / task | Risk if ignored |
|---|---|---|---|---|
| V-01 | Generator does not produce intermediate-late CDC or `late_beyond_window` events (D-030) | Planned | W3.D14.0 (before SCD2 tests) | SCD2 late-arrival tests would pass vacuously |
| V-02 | Bronze tables are addressed by path, not by catalog name | Planned | W2.D11 (catalog spike) | Jobs on Glue/Databricks would need path config |
| V-03 | Delta commit safety on AWS S3 / Glue (local SeaweedFS has no put-if-absent; single writer assumed) | Planned | W5 (Glue run) | Concurrent writers on S3 could corrupt the log; ADR-0006 results must not be carried over untested |
| V-04 | AWS profile / short-lived credentials on the owner's machine | Planned | W5.D1 | First cloud day blocked |
| V-05 | Two writers on one Bronze table at the same time | Known limit | ADR-0006 §single writer; `submit_job.py` refuses an identical running driver (best effort) | A second scheduler could double-write; no lock exists locally |
| V-06 | Spark worker dies in the middle of a batch (task retry path) | Known limit | Not covered: S3 kills the driver, not a worker | Low: Spark retries the task, the batch commit is unchanged |
| V-07 | Trailing content after a JSON object is not flagged as a parse error (D-047) | Known limit | Revisit if a producer ever emits it | A malformed record could pass Bronze parsing; D10 rules still check the contract |
| V-08 | `test_bronze_rows_equal_produced_messages…` failed once (`raw_customers 1 == 0`) and passed on rerun | Planned | Watch in W2.D10 integration runs; investigate on a second failure | A flaky test hides real regressions |
| V-09 | Owner's local environment file holds a stale `COMPOSE_PROJECT_NAME` | Owner action | Owner edits the value (do not print the file) | Plain `docker compose` targets the wrong project |
| V-10 | Legacy containers of the earlier incident and anonymous volume `5b2af2ca…` | Owner action | Owner decides keep or remove | Disk use only |
| V-11 | Tutor learning gates W1, W2.D7, W2.D8, W2.D9 | Owner action | Learning queue in [TASKS.md](TASKS.md) | Owner cannot explain the work in interviews |
| V-12 | Spark rules accept a JSON number in a string field (`first_name: 5`) and differ from Pydantic on rare timestamp/integer forms (D-058, `rules.KNOWN_DIVERGENCES`) | Known limit | Revisit if a producer outside the Pydantic contracts is added | A non-conforming producer could pass a few wrong-typed values into Silver |

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
