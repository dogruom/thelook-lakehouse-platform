# Runbook: Milestone 1 streaming demo (about 15 minutes)

Shows, from a stopped stack: smoke test → produce to Kafka → Kafka→Bronze `availableNow` → restart
from the checkpoint → per-partition offset uniqueness → DLQ gate (twice, idempotent) → tables by
name in the Hive metastore → `SHOW TABLES` through the Spark Thrift Server.
Background: [ADR-0002](../adr/0002-event-interface-kafka-envelope.md) (event interface),
[ADR-0006](../adr/0006-delivery-semantics-restart-and-checkpoint-loss.md) (restart and checkpoint loss),
[ADR-0004](../adr/0004-spark-dbt-ownership-and-adapter-capabilities.md) (catalog and dbt),
[DATA_FLOW.md](../DATA_FLOW.md) (where each step lives in the code).

The demo works on the **default (non-namespaced) tables**, which keep data from earlier runs, so
absolute counts grow with every demo. Check the **invariants**, not the totals. The exact
"Bronze = produced" and "DLQ = manifest" comparisons run on throw-away topics in the integration
tests (step 9).

## Prerequisites
- Everything in [week1-demo.md](week1-demo.md) "Prerequisites" (venv, Docker memory, local env file,
  `COMPOSE_PROJECT_NAME=crm_lakehouse`).
- PowerShell from the repo root. In Git Bash also set `MSYS_NO_PATHCONV=1`, otherwise
  `/opt/spark/...` paths in `docker exec` are rewritten to Windows paths.

## Steps

1. **Start the core stack** — `docker compose --profile core up -d`
   Expected: all `crm_*` containers healthy (Kafka has a ~60 s start period).

2. **Smoke test** — `python scripts/smoke_test.py`
   Expected: `RESULT: all 24 checks passed`. If it reports a failure right after step 1, wait
   30 s and rerun once (see Troubleshooting).

3. **Produce a seeded batch**
   ```powershell
   python -m lakehouse.generator produce --brands 3 --events 5000 --cdc-events 1000 --seed 1210
   ```
   Expected: `delivered[topic=clickstream_events]=5200`, `delivered[topic=customer_cdc]=1020`, 0 failed.

4. **Kafka → Bronze** — `python scripts/submit_job.py --timeout-seconds 900 bronze_stream`
   Expected: one `bronze_stream.query_done` per table with `rows_seen` = what step 3 delivered
   (clickstream includes injected `malformed_json` rows, counted as `parse_errors`), then `job.succeeded`.

5. **Restart from the checkpoint** — produce a small second batch and run the job twice:
   ```powershell
   python -m lakehouse.generator produce --brands 3 --events 1000 --cdc-events 200 --seed 1211
   python scripts/submit_job.py --skip-build --timeout-seconds 900 bronze_stream   # reads only the new offsets
   python scripts/submit_job.py --skip-build --timeout-seconds 900 bronze_stream   # nothing new: 0 rows, 0 batches
   ```
   Expected: same `query_id` per table on both runs (same checkpoint lineage); run 1 `rows_seen`
   = the new messages; run 2 `rows_seen = 0`.

6. **Offset uniqueness** — `python scripts/submit_job.py --skip-build bronze_report`
   Expected per table: `rows == distinct_source_refs`, `distinct_lineages == 1`, and in
   `offsets_by_partition` every partition has `rows == distinct == max - min + 1` (no gap, no duplicate).
   A crash in the middle of a batch is shown by the integration tests S1–S4/S7 (step 9), not live.

7. **DLQ gate, twice** — `python scripts/submit_job.py --skip-build dlq_gate` (run it two times),
   then `python scripts/submit_job.py --skip-build dlq_report`
   Expected: run 1 `dlq_inserted` > 0 for new rejects and `dlq_missing = 0`; run 2 `dlq_inserted = 0`
   (insert-only MERGE on `_dlq_id`); report `rows == distinct_dlq_ids`; codes are stable rule codes.

8. **Tables by name** — register, start the Thrift Server, query:
   ```powershell
   python scripts/submit_job.py --skip-build register_tables
   docker compose --profile core --profile sql up -d
   docker exec crm_spark_thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true -e "SHOW TABLES IN bronze; SHOW TABLES IN dead_letter; SELECT count(*) FROM bronze.raw_customers;"
   ```
   Expected: `bronze.raw_clickstream`, `bronze.raw_customers`, `dead_letter.dlq_clickstream`,
   `dead_letter.dlq_customers`; the count equals `rows` from step 6. The Thrift Server needs a few
   seconds after `up -d`; retry the query if the connection is refused.

9. **Exact reconciliation (tests)** — stop the Thrift Server first (it holds 1 of 2 worker cores):
   ```powershell
   docker compose --profile sql stop spark-thrift
   pytest tests/integration/test_bronze_stream.py tests/integration/test_dlq_reconciliation.py -q
   ```
   Expected: all pass — Bronze rows = produced messages including parse errors; DLQ
   `(brand_id, _bronze_ref)` set = manifest DLQ set exactly. About 25 minutes; outside the 15-minute demo.

10. **Stop (never `down -v`)** — `docker compose --profile core --profile sql stop`

## Recorded run (2026-11-10, from a stopped stack, commit `aa7f9e9`)
| Step | Result | Time |
|---|---|---|
| 1 | containers started, Kafka healthy | 29 s |
| 2 | first run 23/24 (the failing check was not captured); rerun **24/24** | 98 s + 95 s |
| 3 | delivered clickstream 5200, customer_cdc 1020, 0 failed | 3 s |
| 4 | raw_customers `rows_seen` 1020 (0 parse errors), raw_clickstream 5200 (25 `malformed_json`) | 60 s |
| 5 | second produce 1040 + 204; run 1 seen 204 / 1040 (query ids `53a773ec…`, `a8bf18eb…`); run 2 seen 0 / 0, same ids | ~2 × 50 s |
| 6 | raw_customers 4284 rows = 4284 refs, 1 lineage; raw_clickstream 21840 = 21840, 1 lineage, 105 `malformed_json`; all partitions contiguous and unique | 57 s |
| 7 | run 1 inserted 84 (customers) / 60 (clickstream), `dlq_missing` 0; run 2 inserted 0 / 0; DLQ 294 = 294 ids (`email_invalid` 126, `email_null_or_wrong_type` 84, `signup_after_produced` 84), 210 = 210 ids (`session_id_null_or_wrong_type` 210) | ~2 min |
| 8 | 4 tables listed; `count(*)` raw_customers 4284, dlq_customers 294 | ~40 s |
| **Total steps 1–8** | 21:20:47 → 21:36:58 | **16 min 11 s** (about 14.5 min without the smoke rerun) |

Consistency checks on the recorded numbers: 84 new customer DLQ rows = 70 (1020 CDC events, seed
1210) + 14 (204, seed 1211), the generator's exact defect counts for those batch sizes; Bronze
customers grew 3060 → 4284 = +1020 +204.

## Troubleshooting
- **Smoke test failed once right after `up -d`** (2026-11-10, 23/24): the checks ran before the
  init jobs had finished. Fixed in W2.D12 (D-066): `smoke_test.py` now waits up to 180 s for healthy
  services and finished init jobs and prints `stack ready after N s`. A failure now is real.
- **`beeline` not found** in Git Bash: set `MSYS_NO_PATHCONV=1` (path rewriting).
- **Jobs wait for resources**: the Thrift Server occupies 1 of the 2 worker cores; stop it before
  running jobs or integration tests.
- Everything else: [week1-demo.md](week1-demo.md) "Troubleshooting".
