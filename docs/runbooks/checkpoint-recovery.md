# Runbook: checkpoint recovery (Kafka -> Bronze and Silver streams)

## When to use
A streaming job fails or behaves oddly after its checkpoint changed: the checkpoint directory is
missing or partly missing, `bronze_stream` exits non-zero with `CheckpointRewoundError` or a
`failOnDataLoss` message, or `bronze_report` shows `distinct_lineages > 1`.
Authority: [ADR-0006](../adr/0006-delivery-semantics-restart-and-checkpoint-loss.md) (scenarios S5, S6,
S9; evidence in `tests/integration/test_stream_restart.py`). Wording: offsets are committed **once per
checkpoint lineage**; we do not claim exactly-once delivery.

## Preconditions
- Know which checkpoint is affected. Layout (`s3a://lakehouse/` locally, `s3://<data bucket>/` on AWS):
  - `checkpoints/bronze_stream/[<ns>/]<table>` (`raw_customers`, `raw_clickstream`)
  - `checkpoints/silver_<entity>/[<ns>/]<table>` (for example `silver_orders/orders`)
- No driver for that job is running: `docker exec crm_spark_master ps -eo pid,args` (a second writer on
  one table can overwrite commits; S3A has no put-if-absent).
- Take the before-picture:
  ```powershell
  python scripts/submit_job.py --skip-build bronze_report
  ```
  Save `rows`, `distinct_source_refs`, `distinct_lineages`, `offsets_by_partition`.

## Decide by symptom
| Symptom | Meaning | Action |
|---|---|---|
| Whole checkpoint directory gone (S5) | Job would start a new lineage and re-read the topic from earliest | Rerun is safe for completeness; duplicates are expected (steps A) |
| `CheckpointRewoundError` (partial loss, S9: `offsets/` and `commits/` gone, `metadata` kept) | Query id survived, batch ids restarted at 0; the job refuses to continue so no batch is skipped silently | Restore the checkpoint from backup, or move to a new lineage knowingly (steps B) |
| `failOnDataLoss` / "records deleted past the checkpointed offset" (S6) | Kafka retention removed data the checkpoint still points at | Data is gone from Kafka; recovery is a manual owner decision, do not set `failOnDataLoss=false` casually |
| Job stalls, checkpoint intact | Not a checkpoint problem | See [incident-triage.md](incident-triage.md), "Stuck stream" |

## Steps A: whole checkpoint lost (duplicates, nothing lost)
1. Do **not** recreate or copy checkpoint files by hand.
2. Rerun the stream; it starts a new lineage and re-reads from the earliest offset:
   ```powershell
   python scripts/submit_job.py --skip-build --timeout-seconds 900 bronze_stream
   ```
3. Check Bronze:
   ```powershell
   python scripts/submit_job.py --skip-build bronze_report
   ```
   Expected (measured in S5): `rows` is a multiple of the messages, `distinct_source_refs` equals the
   messages, `distinct_lineages` is 2. Duplicates share `_source_ref`; nothing is lost.
4. Run the downstream jobs so Silver absorbs the duplicates (`dlq_gate`, then the Silver jobs, then
   `reconcile`). Silver deduplicates deterministically and merges idempotently; the re-read rows are
   counted, not hidden, in `reread_rows` / `duplicates_removed` of `ops.pipeline_metrics`.
5. Compare: `python scripts/submit_job.py --skip-build metrics_report` and
   `python scripts/submit_job.py --skip-build content_hash --table customers` before and after; the Silver
   content must not change because of the re-read.

## Steps B: partial loss or a deliberate new lineage
1. Preferred: restore the lost checkpoint files from a copy of the same checkpoint (same query id).
2. If no copy exists, start a new lineage on purpose. It behaves like Steps A (full re-read, duplicates):
   ```powershell
   python scripts/submit_job.py --skip-build bronze_stream --checkpoint-namespace recovery_20261114
   ```
   Use a fresh, dated value; never reuse a namespace. The old checkpoint stays untouched for inspection.
3. Verify as in Steps A, step 3 and 4.

## Silver checkpoints
Losing a Silver checkpoint makes that Silver job re-read Bronze from the start. The MERGE is
deterministic, so the Silver state converges, but the job writes new batch rows to
`ops.pipeline_metrics` (new query id), so `reconcile` check (c) may report a mismatch against the
data totals. This is expected, not demonstrated on a live run; the data-derived totals are the authority
(D-083). Prefer the audited reset job for a clean rebuild of the clickstream lineage
(`reset_clickstream`, dry run first); other tables have no reset job.

## Verification
- `bronze_report`: per partition `rows >= distinct` and `distinct == max - min + 1` (no gap).
- `distinct_source_refs` equals the produced message count (use a throw-away topic for an exact check).
- `reconcile` exit code and `metrics_report` `reconcile` column (see incident-triage for mismatches).

## Rollback / do not
- Never delete a checkpoint directory casually to "unstick" a job: that is a lineage change (S5).
- Never share one checkpoint between two queries or two jobs, and never copy a checkpoint between
  environments or tables.
- Never run two drivers on the same job; `scripts/submit_job.py` refuses an identical one, best effort.
- Do not delete Bronze duplicates: Bronze is append-only. Audited erasure is the only exception.
- Do not claim exactly-once. Say: at-least-once delivery, offset committed once per checkpoint lineage,
  idempotent outcome in Silver.
- On AWS the Kafka-sourced Bronze tables are seeded copies (D-101); there is no streaming checkpoint
  to recover there. Recovery is `seed_bronze.py --replace` (dry run first) from the local table.
