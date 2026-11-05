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

## Runtime, catalog and environments (ADR-0007, ADR-0008)
- Spark 3.5.9 + Delta 3.3.3 + Python 3.11 + Java 17 locally; AWS Glue 5.0 (Spark 3.5.4,
  Python 3.11, Delta 3.3.0) on AWS; Databricks serverless in W6 (ADR-0011/0012). Do not use Spark 4-only APIs.
- Tables are registered in a Hive Metastore (Postgres-backed) locally and in the AWS Glue Data
  Catalog on AWS; jobs address tables by name (`bronze.raw_customers`), paths come from config.
- dbt-spark connects through the Spark Thrift Server (`sql` profile).
- Storage access only through the S3 API (`S3_ENDPOINT`, path-style locally). Record any SeaweedFS
  vs AWS S3 / Glue / Databricks difference in `docs/LOCAL_VS_CLOUD.md` (create when the first one is observed).

## Reconciliation vocabulary
Per batch, counted in events: `bronze = silver_accepted + dlq + duplicates_removed +
stale_cdc_noop + late_beyond_window`. Every term is a metric in `ops.pipeline_metrics`.
