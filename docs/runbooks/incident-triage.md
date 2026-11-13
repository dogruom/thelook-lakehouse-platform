# Runbook: incident triage

## When to use
Something in the pipeline looks wrong: `reconcile` exits non-zero, the DLQ grows, one tenant looks
different from the others, or a stream does not finish. Start with the one-page picture, then jump to
the matching section.

## Preconditions
- PowerShell at the repo root; local stack up for local checks (`docker compose --profile core up -d`).
  On AWS: owner credentials and `scripts/cloud/run_job.py` (needs owner approval; it starts paid runs).
- Read-only first. Nothing below changes data until a section says so.

## Step 0: the one-page picture
```powershell
python scripts/submit_job.py --skip-build metrics_report --output-uri s3a://lakehouse/ops/reports
```
On AWS: `python scripts/cloud/run_job.py metrics_report --arg=--output-uri --arg=s3://crm-lakehouse-dev-<account_id>/ops/reports`
(`--arg=` with `=`, one list element each, PowerShell-safe; `--args-json` is the alternative).
The markdown is in the log (`metrics_report.markdown`) and under `ops/reports/<run_id>/report.md` and
`report.json`. Per `(source_table, brand_id)` it shows bronze, silver_accepted, dlq, dlq_rate,
duplicates, stale CDC, late, parse errors, last reconcile status, freshness and ingest lag. `n/a` means
"not measurable", never 0. Lag caveat: Kafka consumer offsets are not available (and never on Glue); the
report only shows `max(_ingest_ts) - max(_kafka_ts)` for Kafka-sourced Bronze tables. On AWS the
Kafka-sourced Bronze tables are seeded copies (D-101), so their freshness is old by design.

## Red reconcile
`python scripts/submit_job.py --skip-build reconcile` exits 1 and logs one `reconcile.mismatch` per failed
check (counts and codes only):
| `check` | Meaning | Look at |
|---|---|---|
| `batch_row_unbalanced` | A recorded batch row does not satisfy `bronze = silver_accepted + dlq + duplicates_removed + stale_cdc_noop + late_beyond_window` | `reasons`, `batch_job`, `batch_id`: a Silver job bug or a crashed batch |
| `data_remainder_negative` | Per `(source_table, brand_id)`: more Silver + DLQ rows than distinct Bronze refs | `source_table`, `brand_id`: orphaned Silver rows, DLQ rows from a deleted Bronze, or a wrong namespace |
| `batch_sum_vs_data` | Sum of the batch terms differs from the data-derived total for `term` | `term`, `data`, `batch_rows_sum` |
Known legitimate causes (D-079, D-083): a clickstream re-read across micro-batches, a crash
re-execution that split a batch's terms differently, a Silver checkpoint loss (new batch rows), or a replay
(handled by pooled `silver_accepted + dlq`). The data-derived totals are the authority.
Steps:
1. `metrics_report`: find the `MISMATCH` rows (reconcile column) and the affected table/brand.
2. Compare Bronze with the source: `python scripts/submit_job.py --skip-build bronze_report` (`rows` vs
   `distinct_source_refs`, `distinct_lineages`).
3. Rerun `reconcile` once after the cause is fixed (it MERGEs on `run_id`, `source_table`, `brand_id`, so
   reruns do not pile up rows).
Do not: edit `ops.pipeline_metrics` by hand, delete mismatching batch rows to turn the check green, or
re-run `bronze_stream` to "fix" it.

## DLQ spike
1. `metrics_report`: which table and brand has a high `dlq_rate`.
2. `python scripts/submit_job.py --skip-build dlq_report --list-refs` (customers and clickstream): counts by
   rule code, category, brand and replay status; `rows == distinct_dlq_ids` means no message was
   dead-lettered twice. `_bronze_ref` (`topic/partition/offset`) locates the Bronze row. Only counts and
   codes are logged, never payload values.
3. Classify: a retryable foreign-key orphan on orders (parent not yet arrived) -> wait for the parents,
   then [replay.md](replay.md). A contract violation (bad value, missing field) -> fix the producer
   / generator; replay cannot help. A new rule code right after a deploy -> suspect the rule change
   (check `docs/DECISION_LOG.md`).
4. Parse errors are not DLQ: they stay in Bronze with `_parse_error` (`bronze_report` `parse_errors`).
Do not: delete DLQ rows, flip `_retryable`, or loosen a rule to make the count fall.

## One tenant fails or looks different
1. `metrics_report`: compare that brand's rows with the others (same table).
2. Brand keys `__null__` or `__invalid__` in `bronze_report` `rows_by_brand` or `dlq_report` `by_brand`
   mean the `brand_id` header or value was missing or outside the tenant-id contract: a producer or
   header problem, not a tenant outage. Kafka key must be `{brand_id}:{entity_id}` and `brand_id` also a
   header.
3. Isolation: `python scripts/submit_job.py --skip-build isolation_check --expected-json '{"customers": {"brand_alpha": 100}}'`
   (counts per brand vs expected, plus the leaky-join detector); scoped views come from
   `register_tenant_views`.
4. AWS: tenant role `crm-lakehouse-dev-tenant-brand-alpha` can read `landing/brand_alpha/*` only; an
   access-denied outside that prefix is the intended behaviour, not an incident.
Do not: run a join without `brand_id`, or "fix" a tenant by copying another tenant's data.

## Stuck stream (local)
1. Is a driver running and where: `docker exec crm_spark_master ps -eo pid,args`; Spark master UI
   `http://localhost:8181` (free cores: the Thrift Server holds one, stop it with
   `docker compose --profile sql stop`).
2. `scripts/submit_job.py` exit codes: 3 no worker capacity, 4 identical driver already running, 124
   timeout (driver stop confirmed), 125 timeout with an unconfirmed stop: check `ps` before any new run.
3. Is Kafka healthy: `docker compose ps`; topic end offsets vs Bronze:
   `docker exec crm_kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic customer_cdc`
   against `bronze_report` `offsets_by_partition` (max offset + 1 = end offset when caught up).
4. Checkpoint symptoms (`CheckpointRewoundError`, `failOnDataLoss`) -> [checkpoint-recovery.md](checkpoint-recovery.md).
Do not start a second driver on the same job, kill with `docker rm`, or delete checkpoints.

## Failed Glue run (AWS)
1. `run_job.py` prints state and Glue's `ErrorMessage`; exit code is non-zero unless `SUCCEEDED`.
2. Logs: CloudWatch groups `/aws-glue/jobs/error` and `/aws-glue/jobs/output`, run id as stream prefix.
3. Common causes: wrong artifact (`--artifact-sha`), `register_tables` not run before `dlq_gate`,
   empty or missing Bronze copy (`seed_bronze.py` dry run), single-writer conflict (max concurrency is 1).
4. Rerun the one job, not the sequence, once fixed; record it with `--record`. Max retries are 0 by
   design: a rerun is a decision, not an automatic retry.

## Verification
After any fix: the affected job exits 0, `reconcile` exits 0, and `metrics_report` shows the key as
`OK` with a plausible `dlq_rate` and freshness. Write down cause, fix and evidence in the day-close
report (decision-level changes also go to `docs/DECISION_LOG.md`).

## Rollback / do not
- Bronze is append-only; Silver and DLQ states change only through their jobs. No manual edits of Delta
  files, `_delta_log`, checkpoints or `ops.pipeline_metrics`.
- No `docker compose down -v`, prune or volume removal; use `docker compose --profile <name> stop`.
- Never paste secrets, `.env` content or payload values into tickets, logs or chat.
