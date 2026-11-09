# ADR-0006: Delivery semantics — restart, replay and checkpoint loss

- **Status:** Draft (W2.D9). Bronze stage only; Silver semantics are added in W3 (D14, D18).
- **Date:** 2026-11-09

## Context
The delivery-semantics claim of the platform must rest on measurements, not on the documentation of
Spark or Delta. The Kafka → Bronze job runs one `Trigger.availableNow` query per topic, with a
`foreachBatch` sink that appends to Delta with `txnAppId` = streaming query id and `txnVersion` =
micro-batch id. The job can die at any point, the checkpoint can be lost, and the broker can be
unavailable or may have removed data.

## Decision

### Wording, per stage
| Stage | Semantics we claim | Evidence |
|---|---|---|
| Producer → Kafka | At-least-once; a re-run of `produce` re-sends the same `event_id`s | W1.D5 |
| Kafka → Bronze | Offset committed once per checkpoint lineage; the sink is idempotent per batch | S1–S4 below |
| Bronze → Silver | Not decided here (idempotent MERGE, W3) | W3.D18 |

We do not claim exactly-once delivery. The result proven below is narrower: after a restart on an
intact checkpoint, each `(topic, partition, offset)` is present in Bronze once.

### Measured behaviour
Source: [tests/integration/test_stream_restart.py](../../tests/integration/test_stream_restart.py).
Faults come from the job `bronze_stream_fault`, which runs the real job code and is inert unless
invoked by name. Each scenario compares Bronze with the produced messages and with the topic
watermarks per partition (`min`, `max`, `rows` and `distinct` offset), not with job output.

| # | Failure | Result |
|---|---|---|
| S1 | Driver dies after the Delta commit and before the offset commit | Restart re-runs that batch (same query id, same batch id); Delta skips it; each offset once |
| S2 | Driver dies before the Delta write | Restart re-runs that batch and writes it; each offset once |
| S3 | `SIGKILL` of driver processes from outside, mid-run | Non-zero exit; restart gives each offset once |
| S4 | Exception in the batch callback | Exit 1; restart resumes at that batch; each offset once |
| S5 | Whole checkpoint lost | New query id, full re-read: `rows = 2 × messages`, `distinct_source_refs = messages`, 2 lineages. Bronze holds duplicates; nothing is lost |
| S6 | Records deleted past the checkpointed offset | Non-zero exit, `failOnDataLoss` in the error, Bronze unchanged; no silent skip |
| S7 | Broker stopped mid-run (opt-in, `RUN_BROKER_CHAOS=1`) | Recorded final run: the job ended by itself with exit 1 after 77 s, no hang; rerun after recovery gives each offset once |
| S8 | Empty topics on the first run | One zero-row batch; Delta creates the table with its schema and no rows; a second run plans nothing; later data arrives once |
| S9 | Partial checkpoint loss: `offsets/` and `commits/` gone, `metadata` kept | See below |

### Skipped replays are verified (S9)
Delta skips an already recorded `(txnAppId, txnVersion)` silently, whatever the batch contains. With
a partial checkpoint loss the query id survives while the batch ids restart at 0, so a batch with
new rows is skipped as if it were a replay. Measured before the fix: exit 0, 102 of 204 CDC rows and
208 of 416 clickstream rows in Bronze, offsets committed past the missing rows. This is permanent
silent data loss, which the charter forbids.

`write_bronze_batch` therefore compares the table version before and after the write. When the
version did not change, Delta skipped the batch, and the `_source_ref` set of the batch must equal
the set already stored for that `_batch_id`. If it differs, the job raises `CheckpointRewoundError`
before the offset commit. A true replay passes, and normal writes pay for no table scan.

### Operating rules
- **Whole checkpoint lost (S5):** rerun is safe for completeness. Duplicates in Bronze are expected
  and share `_source_ref`; downstream must not assume `_source_ref` is unique across lineages.
- **Partial checkpoint loss (S9):** the job refuses to continue. Restore the checkpoint, or move to a
  new checkpoint path knowingly, which is the S5 situation.
- **Retention gap (S6):** data is already gone from Kafka. Any recovery is a manual decision.
- **Single writer:** S3A has no put-if-absent and no DynamoDB LogStore is configured, so two drivers
  on one table can overwrite each other's commit. `scripts/submit_job.py` refuses an identical
  running driver and kills a timed-out driver inside the container. This guard is best effort, not a
  lock: different argument spellings pass it. Concurrent writers were not measured.

## Consequences
- The D9 gate of Milestone 1 has automated evidence for restart, replay and checkpoint loss.
- The day card proposed a `processingTime` trigger with a controlled stop. An injected crash under
  `availableNow` was used instead, because it hits the window between the Delta commit and the offset
  commit deterministically (D-049).
- The S7 result is one observation on one local broker, not a guarantee of a time bound.
- Everything here is local: Spark standalone on SeaweedFS. It says nothing about Glue, Databricks or
  AWS S3. The S3 commit behaviour must be checked again on AWS in W5
  ([LOCAL_VS_CLOUD.md](../LOCAL_VS_CLOUD.md)).
- Known limits: a worker failure mid-batch and concurrent writers are not tested.
