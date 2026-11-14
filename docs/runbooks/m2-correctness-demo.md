# Runbook: Milestone 2 correctness demo (about 20 minutes + optional test runs)

Shows: one-command pipeline → sequence-ordered CDC scenarios → order-independent SCD2 history →
leaky-join negative test → dbt lineage → Customer 360 / RFM per brand → Airflow run with one
failing brand. Background: [ADR-0004](../adr/0004-spark-dbt-ownership-and-adapter-capabilities.md)
(Spark/dbt ownership), [ADR-0005](../adr/0005-multi-tenant-isolation-and-partitioning.md) (tenant
isolation), [ADR-0010](../adr/0010-orchestration-compute-interface.md) (orchestration ↔
compute), [DATA_FLOW.md](../DATA_FLOW.md) (where each step lives in the code).

What this demo claims: sequence-ordered CDC and order-independent SCD2 proven by tests; tested
tenant isolation incl. a negative test; reproducible marts (same input → same checksum).
What it does not claim: legal GDPR compliance, performance at scale, anything on cloud.

## Prerequisites
- [week1-demo.md](week1-demo.md) "Prerequisites" (venv, Docker memory, local `.env`,
  `COMPOSE_PROJECT_NAME=crm_lakehouse`), plus `.venv-dbt` (`requirements/dbt.txt`).
- Images `crm/spark:3.5.9` and `crm/airflow:3.0.6` built.
- `.env` contains `AIRFLOW__API_AUTH__JWT_SECRET` (`python scripts/check_env.py`).
- Memory: `core` + `sql` + `orchestration` do not fit next to each other on a 7.6 GB Docker VM
  with the Airflow API server running; stop `airflow-api-server` and `airflow-dag-processor`
  for steps 7–8 (`airflow dags test` runs in the scheduler container only).

## Steps

1. **Start** — `docker compose --profile core --profile sql up -d`; `python scripts/smoke_test.py`
   Expected: all checks passed.

2. **One-command pipeline** — `python scripts/run_e2e.py` (the `land` step needs the local S3
   keys in the shell, see [OWNER_ACTIONS.md A3](../OWNER_ACTIONS.md); otherwise `--skip-land`)
   Expected: every step `ok`: produce → land → bronze_stream → bronze_batch → dlq_gate → silver
   jobs → reconcile → dbt_build. About 25–30 minutes; for a live demo show a recorded run and
   re-run with `--skip-generate` (processing only).

3. **CDC scenarios (🔺)** — `pytest tests/integration/test_cdc_correctness.py -q`
   Shows: in-order replace, stale update ignored, late update after delete keeps the tombstone,
   duplicate counted, multi-event batch applies the maximum `_cdc_sequence`, same `customer_id`
   in two brands stays two rows; Silver holds no raw email/phone and the token is HMAC, not a
   plain hash.

4. **SCD2 history** — `pytest tests/integration/test_scd2_history.py -q`
   Shows: the same events in different arrival orders produce the same history rows.

5. **Leaky-join negative test** — `pytest tests/integration/test_dbt_tenant_tests.py -q`
   Shows: on a deliberately leaky fixture, plain `relationships` misses the cross-tenant match
   while `relationships_by_brand` and `assert_no_cross_tenant_joins` fail as intended.

6. **Lineage and marts per brand**
   ```powershell
   cd dbt_project
   ..\.venv-dbt\Scripts\dbt docs generate --profiles-dir profiles
   ..\.venv-dbt\Scripts\dbt docs serve --profiles-dir profiles --port 8085   # lineage graph; Ctrl+C to stop
   cd ..
   docker exec crm_spark_thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true -e "SELECT brand_id, segment, count(*) FROM gold_marts.agg_rfm_segments GROUP BY brand_id, segment ORDER BY 1, 2; SELECT brand_id, count(*) FROM gold_tenant.obt_customer_360__brand_beta GROUP BY brand_id;"
   ```
   Expected: five segments per brand, none 0 % or 100 %; the `__brand_beta` view returns only
   `brand_beta`.

7. **Airflow run with one failing brand** — stop the `sql` profile first if memory is short, then
   `pytest tests/integration/test_dag_failure_isolation.py -q`
   Shows: `landing_check` for `brand_beta` fails (injected with the `fail_brand` param), the other
   brands succeed, `summarize_landing` (`all_done`) collects the healthy brands, the shared chain
   (`bronze_stream` …) is not blocked by the failed brand, `run_verdict` (`one_failed`) turns the
   run red, and the alert payload `ALERT_PAYLOAD {"kind":"failure",...}` is in the task log.

8. **Backfill is idempotent (optional, ≈ 60 min)** — needs `core` + `sql` + `orchestration`:
   `pytest tests/integration/test_backfill_idempotent.py -q`
   Shows: two `airflow dags test` runs of the same past date end `success` and the Silver
   `content_hash` and row counts are identical.

9. **Stop (never `down -v`)** —
   `docker compose --profile core --profile sql --profile orchestration stop`

## Troubleshooting
- Spark jobs time out with no executor: the master can drop the worker after a Docker hiccup
  while the container stays healthy — `docker compose restart spark-worker`, then look for
  "Registering worker" in `docker logs crm_spark_master`.
- `dbt build` cannot connect: the Thrift Server needs a few seconds after `up -d`.

## Recorded run
_To be filled from the M2 checkpoint run (commit, date, outputs)._
