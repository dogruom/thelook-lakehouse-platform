# ADR-0007: Runtime baseline, table catalog and Gold engine

- **Status:** Accepted (owner, 2026-11-05)
- **Date:** 2026-11-05

## Context
The audit of 2026-11-05 found:
- The Spark image (`apache/spark:3.5.1-python3`, Ubuntu 20.04) runs **Python 3.8.10**, while the
  project targets Python ≥ 3.11 and shares Pydantic contracts between generator and Spark jobs.
  Code valid on 3.11 (`X | None` at runtime, `StrEnum`, `datetime.UTC`) fails on executors.
- Pinned versions were ~2 years old: Spark 3.5.1, Delta 3.2.0, dbt-core 1.8, and Airflow 2.9.3,
  whose 2.x line reached end of life on 2026-04-22.
- No table catalog existed (no metastore, no Spark Thrift Server), yet the plan required dbt to
  build Gold from Silver tables by name. The Airflow image installed `dbt-postgres`, and Postgres
  had an unused `crm_dwh` database with a `snapshots` schema — inconsistent with Charter v7.
- The cloud target (ADR-0008) is EMR Serverless 7.x, which ships Spark 3.5 with Delta 3.3.x
  (EMR 7.13: Delta 3.3.2-amzn). Spark 4.0 is only in preview on EMR Serverless.

## Decision
| Component | Version / choice |
|---|---|
| Spark | **3.5.9** (latest 3.5 patch, Scala 2.12), Apache binary distribution `bin-hadoop3` |
| Delta Lake | **3.3.3** (`delta-spark_2.12`, `delta-storage`) |
| Python | **3.11** in every runtime (host venv, Spark image, Airflow image, EMR custom image, CI) |
| Java | 17 (Temurin / Debian `openjdk-17-jre-headless`) |
| Spark image base | `python:3.11-slim-bookworm` + Java 17 + Spark binary tarball + pinned JARs (hadoop-aws 3.3.4, aws-java-sdk-bundle 1.12.262, spark-sql-kafka 3.5.9, delta 3.3.3, PostgreSQL JDBC) |
| Catalog (local) | Spark's built-in Hive client with a **PostgreSQL-backed metastore** database (`hive_metastore`) in the existing Postgres; no separate metastore service |
| Catalog (AWS) | AWS Glue Data Catalog (same table names) |
| Gold engine | **dbt-core + dbt-spark** (`thrift` method) against a **Spark Thrift Server** container (`sql` compose profile). Gold tables are Delta tables in the lakehouse |
| Orchestration | Airflow **3.x** (latest 3.3 patch at implementation time), introduced in Milestone 2 |
| Kafka | 3.7.1 kept for M1 (KRaft); upgrade to 4.x optional, not required |

The dbt capability spike (incremental `merge`, composite `unique_key`, contracts, `persist_docs`)
moves to **W2.D11** — before any dbt modelling. Fallback if the Thrift Server is unstable:
dbt-spark `session` method inside the Spark image.

Spark 4.x / Delta 4.x is the documented upgrade path once EMR offers it as GA.

## Consequences
- One Spark image rebuild is required in W1.D2 (it was required anyway for Python 3.11).
- Compose project/image names (`crm_*`) differ from the images built earlier (`thelook_*`); the old
  images and volumes are left untouched.
- dbt on Spark mirrors a pattern used by Berlin employers (e.g. Spark + Kyuubi + dbt); Snowflake /
  BigQuery remain a documented gap (DELIVERY_PLAN §6).
- The PostgreSQL `crm_dwh` database and `snapshots` schema are removed from the init script in
  W1.D2; Postgres hosts Airflow metadata and the Hive metastore only.
- Local Delta 3.3.3 vs Glue 5.0 Delta 3.3.0 differences are recorded in `docs/LOCAL_VS_CLOUD.md` (ADR-0011).
- dbt package set checked against PyPI metadata on 2026-11-06 (all support Python 3.11):
  `dbt-core 1.12.6`, `dbt-adapters >=1.24.5,<1.25`, `dbt-spark[PyHive] 1.11.0` (thrift method),
  `dbt-databricks 1.13.0` (W6, requires `dbt-core <1.12.7`). Delta `merge` behaviour through the
  Thrift Server is verified in the W2.D11 spike before pinning.
