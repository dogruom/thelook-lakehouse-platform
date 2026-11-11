# Data Flow — Follow One Event Through the Code

> **Last Updated**: 2026-11-11 (through W3.D13)
> **Purpose**: where each step of the pipeline lives, what goes in and out, and how to see the data.
> Target design: [ARCHITECTURE.md](ARCHITECTURE.md). Field-level rules: [DATA_CONTRACTS.md](DATA_CONTRACTS.md).
> **Rule**: updated at every day close. A step is ✅ only if its code exists and ran; 📋 steps are planned, and their paths may change.

**To understand a file**: open the file listed for a step and read its module docstring. To ask an AI assistant about it, paste the file together with the step's row from this table.

```mermaid
flowchart LR
  G[Generator] --> K[Kafka topics]
  G --> L[Landing files]
  K --> B[Bronze]
  L --> B
  B --> Q{Quality gate}
  Q -->|valid| S[Silver]
  Q -->|invalid| D[DLQ]
  S --> GO[Gold with dbt]
```

## Steps

| # | Step | Status | Code | In → out | See the data |
|---|---|---|---|---|---|
| 1 | Define what a valid event is | ✅ | [models.py](../src/lakehouse/contracts/models.py), [schemas/](../src/lakehouse/contracts/schemas/) | Pydantic models → JSON Schema files | `schemas/*.json` |
| 2 | Generate CDC events | ✅ | [cdc.py](../src/lakehouse/generator/cdc.py) `generate_cdc`, [config.py](../src/lakehouse/generator/config.py) | seed + config → events with planned defects | `python -m lakehouse.generator cdc --help` |
| 3 | Generate clickstream events | ✅ | [clickstream.py](../src/lakehouse/generator/clickstream.py) `generate_clickstream` | seed + config → events with planned defects | `python -m lakehouse.generator clickstream --help` |
| 4 | Record the expected outcome of every event | ✅ | [manifest.py](../src/lakehouse/generator/manifest.py) `classify_outcomes`, `expected_final_rows` | events → expected accepted, DLQ, duplicate, stale counts | [W1 lab notebook](../.claude/learning/labs/W1_lab.ipynb) (local) |
| 5 | Build Kafka key and headers | ✅ | [kafka.py](../src/lakehouse/contracts/kafka.py) `cdc_key`, `clickstream_key`, `cdc_headers`, `clickstream_headers`; [messages.py](../src/lakehouse/generator/messages.py) | event → key `{brand_id}:{entity_id}`, header `brand_id` | [week1-demo.md](runbooks/week1-demo.md) |
| 6 | Send to Kafka | ✅ | [producer.py](../src/lakehouse/generator/producer.py) `produce_messages`; CLI [\_\_main\_\_.py](../src/lakehouse/generator/__main__.py) | messages → topics `customer_cdc`, `clickstream_events` | [week1-demo.md](runbooks/week1-demo.md) |
| 7 | Start any Spark job | ✅ | [spark.py](../src/lakehouse/common/spark.py), [jobs.py](../src/lakehouse/common/jobs.py), [submit_job.py](../scripts/submit_job.py) | job name → run on the cluster, logs with `run_id` | Spark UI `127.0.0.1:8181` |
| 8 | Parse contract JSON in Spark | ✅ schema only | [spark_schema.py](../src/lakehouse/contracts/spark_schema.py) | contract → `StructType` | [test_spark_contract_schemas.py](../tests/spark/test_spark_contract_schemas.py) |
| 9 | Kafka → Bronze | ✅ | [bronze.py](../src/lakehouse/ingestion/bronze.py) `to_bronze_rows`, `write_bronze_batch`; [bronze_stream.py](../src/lakehouse/ingestion/jobs/bronze_stream.py) | message → one append-only row with raw value, lineage and `_parse_error`; tables at `s3a://lakehouse/bronze/raw_customers`, `raw_clickstream` (by path until the metastore, W2.D11). A rewound checkpoint fails the job (`CheckpointRewoundError`, D-050); restart behaviour in [ADR-0006](adr/0006-delivery-semantics-restart-and-checkpoint-loss.md) | `python scripts/submit_job.py bronze_report` (counts per table, parse-error code and tenant, `offsets_by_partition`, `distinct_lineages`; no raw values); [test_stream_restart.py](../tests/integration/test_stream_restart.py); [bronze_report.py](../src/lakehouse/ops/jobs/bronze_report.py) |
| 6a | Generate orders + products and land them as files | ✅ | [orders.py](../src/lakehouse/generator/orders.py) `generate_orders`, `customer_pools_from_cdc`; [products.py](../src/lakehouse/generator/products.py) `generate_products`; [landing.py](../src/lakehouse/generator/landing.py) `write_batch`, `write_products`; CLI `python -m lakehouse.generator land` | seed + config + the CDC customer pool → `landing/{brand}/orders/{date}/batch-NNNNN.ndjson` (one order per line, items embedded, `sales_channel` from batch 10) + `.manifest.json` (line count, sha256, defect per line), written data first, manifest last; products once per brand (D-070) | `python -m lakehouse.generator land --dry-run --out-dir build/landing` and open the NDJSON / manifest in VS Code; [test_landing.py](../tests/unit/generator/test_landing.py) |
| 9a | Landing files → Bronze | ✅ | [batch.py](../src/lakehouse/ingestion/batch.py) `to_bronze_rows`, `write_bronze_files`; [bronze_batch.py](../src/lakehouse/ingestion/jobs/bronze_batch.py) | new `*.ndjson` file → one append-only row per line in `bronze.raw_orders` / `raw_products`, `_source_ref` = `landing/...ndjson#L<n>`, `brand_id` from the path, payload brand in `payload_brand_id`, unparseable lines kept with `_parse_error`; `availableNow`, the checkpoint remembers processed files (D-071) | `python scripts/submit_job.py bronze_batch`; [test_bronze_batch.py](../tests/integration/test_bronze_batch.py) (rows per file = manifest line count) |
| 10 | Bronze → DLQ | ✅ | [rules.py](../src/lakehouse/quality/rules.py) `RULES_BY_TABLE`, `apply_rules`; [dlq_gate.py](../src/lakehouse/quality/jobs/dlq_gate.py) `post_write_problems` | parsed Bronze row failing a contract rule → one row in `s3a://lakehouse/dead_letter/dlq_customers`, `dlq_clickstream` with fixed rule codes, `_error_category`, `_bronze_ref`, `_retryable`, `_replay_status` (insert-only MERGE on `_dlq_id`; parse errors stay in Bronze, D-056/D-057) | `python scripts/submit_job.py dlq_gate`; `python scripts/submit_job.py dlq_report --list-refs` (counts by code, category, tenant; masked refs); [dlq_report.py](../src/lakehouse/ops/jobs/dlq_report.py); [test_dlq_reconciliation.py](../tests/integration/test_dlq_reconciliation.py) |
| 10a | Register tables by name | ✅ | [tables.py](../src/lakehouse/common/tables.py) `resolve`, `register_table`; [register_tables.py](../src/lakehouse/ops/jobs/register_tables.py); [render_hive_site.py](../docker/spark/render_hive_site.py) | Delta location written by a job → catalog entry `bronze.raw_*` / `dead_letter.dlq_*` in the Postgres-backed Hive metastore; readers (gate, reports, dbt) use the name; namespaced test runs stay path-based (D-060) | `python scripts/submit_job.py register_tables`; `docker exec crm_spark_thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 -e "SHOW TABLES IN bronze"`; dbt probes `dbt_project/models/spike/` (`dbt build --select spike --profiles-dir profiles`), [ADR-0004](adr/0004-spark-dbt-ownership-and-adapter-capabilities.md) |
| 11 | Bronze → Silver | 📋 W3.D14–D17 | `src/lakehouse/silver/` | valid row → `MERGE` by `_cdc_sequence`, SCD2 | added at D14 |
| 12 | Reconcile counts | 📋 W3.D18 | `src/lakehouse/ops/` | layer counts → bronze = accepted + dlq + duplicates + stale + late | added at D18 |
| 13 | Silver → Gold | 📋 W4.D19–D21 | `dbt_project/` | Silver → dimensions, facts, Customer 360, RFM | added at D19 |

## Rules that hold across steps

- `brand_id` is part of every business key and join.
- Bronze keeps everything; nothing is dropped silently.
- CDC order comes from `_cdc_sequence`, never from wall-clock time.
- Money and timestamps stay strings until Silver checks and casts them (D-045).
