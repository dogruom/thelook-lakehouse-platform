# Runbook: DLQ replay (orders)

## When to use
`dead_letter.dlq_orders` holds retryable rows (`_retryable = true`, `_replay_status` `PENDING` or
`FAILED_AGAIN`): orders whose customer or product had not arrived when `silver_orders` ran (foreign-key
orphans). Typical trigger: `dlq_report` shows `PENDING` rows and the missing customers/products have
since been loaded. Only `orders` has retryable rows; contract failures (bad values, missing
fields) are not replayable by design, fix them at the producer.

Background: [replay.py](../../src/lakehouse/quality/jobs/replay.py),
[ADR-0006](../adr/0006-delivery-semantics-restart-and-checkpoint-loss.md).

## Preconditions
- The parents exist in Silver: run `silver_customers` and `silver_products` (or `bronze_batch` for
  `raw_products`) first; otherwise the replay re-rejects the same rows (`FAILED_AGAIN`, count + 1).
- `silver_orders` ran at least once (it creates `silver.orders`, `silver.order_items`, `dlq_orders`);
  the job fails with a clear message when a table is missing.
- Single writer: no other `silver_orders` or `replay` run is active (S3A has no put-if-absent;
  `scripts/submit_job.py` refuses an identical running driver, best effort).
- Local: stack up (`docker compose --profile core up -d`), PowerShell at the repo root.

## Steps (local)
1. **Before-picture** (`dlq_report` only knows `dlq_customers` and `dlq_clickstream`, so read the
   orders DLQ through the Thrift Server; needs `register_tables` once and the `sql` profile)
   ```powershell
   python scripts/submit_job.py --skip-build register_tables
   docker compose --profile core --profile sql up -d
   docker exec crm_spark_thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true -e "SELECT brand_id, _replay_status, _retryable, count(*) AS n FROM dead_letter.dlq_orders GROUP BY 1,2,3 ORDER BY 1,2"
   ```
   Note the `PENDING` / `FAILED_AGAIN` counts per brand. Stop the Thrift Server afterwards (it holds
   a worker core): `docker compose --profile sql stop`.
2. **One tenant first** (the job has no dry-run flag; `--brand` limits the blast radius)
   ```powershell
   python scripts/submit_job.py --skip-build replay --table orders --brand brand_alpha
   ```
3. **All tenants** (after the tenant run looked right):
   ```powershell
   python scripts/submit_job.py --skip-build replay --table orders
   ```
   Options: `--max-replays N` (default 3; rows that reached N attempts are left alone),
   `--namespace <segment>` (tests only).
4. **Recompute the books**
   ```powershell
   python scripts/submit_job.py --skip-build reconcile
   python scripts/submit_job.py --skip-build metrics_report --output-uri s3a://lakehouse/ops/reports
   ```

## Verification
- Log `replay.done` carries `candidates`, `bronze_found`, `recovered`, `still_orphan`, `orders_inserted`,
  `items_inserted`, `replayed`, `failed_again`, `dlq_updated`. Recovered rows move `PENDING` ->
  `REPLAYED`, the rest -> `FAILED_AGAIN`, `_replay_count + 1` for all attempted rows.
- Run step 2 or 3 a **second time**: nothing recovered, Silver and DLQ counts unchanged. This is an
  idempotent outcome (insert-only MERGE), not an exactly-once claim.
- `reconcile` exits 0. Its orders check compares the pooled `silver_accepted + dlq`, which is stable
  under replay: a row moving from DLQ to Silver does not change the sum.
- `metrics_report`: `raw_orders` `dlq_rate` falls by the recovered share; `reconcile` column `OK`.

## Cloud (AWS)
`replay` is **not** one of the ten Glue jobs provisioned in W5 (`bronze_batch`, `dlq_gate`, five
Silver jobs, `reconcile`, `register_tables`, `metrics_report`). To run it on Glue the job must first be
added to the `glue_jobs` variable of `infra/terraform/envs/dev` (an owner-approved Terraform change),
then `python scripts/cloud/run_job.py replay --arg=--table --arg=orders`. Until then replay is
a local-only operation.

## Rollback / do not
- There is no undo for recovered orders: they are real Silver rows with valid foreign keys. If a
  replay was wrong, the cause is wrong parent data: fix the parents and re-run the Silver jobs; do not
  delete Silver rows by hand.
- Never edit `_replay_status` / `_replay_count` by hand or with ad-hoc SQL; `set_replay_outcome` is the
  only writer.
- A crash between the Silver write and the DLQ status update is safe: rows stay `PENDING`, the rerun
  recovers the same orders (the merge adds nothing) and then fixes the status.
- Do not raise `--max-replays` to push rows through a permanent cause (missing parent that never
  arrives); investigate with `dlq_report --list-refs` (`_bronze_ref` locates the Bronze row).
