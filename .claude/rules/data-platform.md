---
paths:
  - "src/**"
  - "dbt_project/**"
  - "dags/**"
  - "tests/**"
  - "docker/**"
  - "docker-compose.yml"
  - "infra/**"
---

# Data platform rules (apply Charter v7 §3 invariants)

## Keys, tenancy, ordering
- Every business key, MERGE condition, join and dbt relationship test includes `brand_id`.
- Kafka message key: `{brand_id}:{customer_id}` (CDC) or `{brand_id}:{session_id}` (clickstream).
  `brand_id` is also sent as a header for routing/filtering; never rely on headers for partitioning.
- CDC: apply only when `source._cdc_sequence > target._cdc_sequence`; `_cdc_ts` is informational.
  `D` produces a tombstone with PII nulled. Out-of-order and duplicate CDC must be idempotent.

## Layers and ownership
- Bronze: append-only; metadata `_ingest_ts`, `_source_ref` (topic/partition/offset or file),
  `_batch_id`, `brand_id`; unparseable payloads stored raw with `_parse_error`.
- DLQ (`dead_letter.dlq_*`): contract/integrity failures with `_dlq_id`, `_bronze_ref`,
  `_error_category`, `_rejection_reason`, `_retryable`, `_replay_status`, `_replay_count`.
- Silver (Spark): `foreachBatch` + deterministic `MERGE`; re-running a batch yields the same state.
- Gold (dbt): reads Silver only, never writes it; no raw PII columns.

## Delivery semantics vocabulary
Use exactly: "at-least-once" (producer, Kafka), "offset committed once per checkpoint lineage"
(Kafka → Bronze), "idempotent outcome" (Bronze → Silver). Never "exactly-once" without a test proving it.

## Streaming specifics
- Checkpoints live under `s3a://lakehouse/checkpoints/<job>/`; never share a checkpoint between queries.
- Prefer `Trigger.availableNow` for scheduled runs; Airflow triggers runs, Spark owns stream state.
- Every correctness claim needs a test: restart/replay, checkpoint loss, CDC ordering, tenant isolation
  (including a negative test that a leaky join is detected), DLQ replay without duplicates.

## Environment honesty
- Storage access only through the S3 API (`S3_ENDPOINT`, path-style). Record any SeaweedFS vs AWS S3
  difference in `docs/LOCAL_VS_CLOUD.md` (create when the first difference is observed).
- Pin container image versions; no `:latest` for new services.
