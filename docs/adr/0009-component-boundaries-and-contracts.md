# ADR-0009: Component boundaries, contracts and package layout

- **Status:** Accepted (owner delegated, 2026-11-05)
- **Date:** 2026-11-05

## Context
The owner wants a microservice-style architecture "as much as reasonably possible". In a data
platform, splitting pipeline stages into synchronous HTTP services adds latency, failure modes and
operational cost without benefit: the stages already communicate through durable logs and tables.
What *does* transfer from microservices is: independent deployability, explicit versioned
interfaces, independent tests, and single ownership of data. The repository used `src` itself as
the import package (`from src.generator ...`) and had no enforced boundaries.

## Decision
Event-driven components with data contracts, in one repository (monorepo) and one installable
package `lakehouse` (src-layout: `src/lakehouse/...`):

| Component | Deployable unit | Owns | Consumes | Produces (interface) |
|---|---|---|---|---|
| `generator` | `lakehouse-app` image / CLI `python -m lakehouse.generator` | synthetic data, ground-truth manifest | config | Kafka `customer_cdc`, `clickstream_events`; landing `landing/{brand_id}/{entity}/{date}/`; manifest |
| `ingestion` | Spark jobs `bronze_stream`, `bronze_batch` | Bronze tables | topics, landing files | `bronze.raw_*` |
| `quality` | library + Spark job `dlq_replay`, `reconcile` | DLQ tables, validation rules, reconciliation | Bronze | `dead_letter.dlq_*`, `ops.pipeline_metrics` |
| `silver` | Spark jobs `silver_customers`, `silver_orders`, `silver_clickstream` | Silver tables incl. SCD2 | Bronze, quality rules | `silver.*` |
| `ops` | CLI | erasure procedure, metrics report | all layers | `ops.erasure_log` |
| `dbt_project` | dbt project | Gold | Silver (by name) | `gold.*` |
| `dags` | Airflow | schedules only | CLIs / job submissions | — |
| `infra` | Terraform | AWS resources | — | — |
| `contracts`, `common` | shared library | event/table schemas (versioned), config, logging, Spark/storage adapters | — | — |

Rules:
1. Components interact only through `contracts`, topics, storage paths and tables — never by
   importing each other's internals. Exception: `silver` may import `quality` validation rules.
2. Enforced by **import-linter** contracts in CI (independence + layers).
3. Every event carries `schema_version`; breaking changes create a new version and a contract test.
   JSON + versioned JSON Schema (generated from Pydantic) instead of a schema registry in v1.
4. Each component has its own entry point, configuration prefix, test folder and runbook section.
5. Orchestration triggers components through their public CLI / job interface only.

## Consequences
- Rename `src/generator`, `src/utils` → `src/lakehouse/...` in W1.D1 (two files today — cheapest now).
- Two images: `lakehouse-app` (Python 3.11 slim, generator/CLIs) and `lakehouse-spark` (Spark jobs).
- Schema Registry (Avro/Protobuf) is a documented next-stage option, not built in v1.
- Interview narrative: "service boundaries are data contracts; each component is independently
  deployable and testable, and the boundaries are enforced in CI."
