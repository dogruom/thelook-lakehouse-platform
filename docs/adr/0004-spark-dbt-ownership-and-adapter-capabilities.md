# ADR-0004: Spark vs. dbt ownership boundary and dbt adapter capabilities

- **Status:** Accepted (spike section W2.D11, 2026-11-10; ownership section W3.D15.3, 2026-11-11).
- **Date:** 2026-11-10 (ownership finalised 2026-11-11)
- **Related:** ADR-0007 (runtime, catalog, Gold engine), ADR-0009 (component boundaries),
  D-060 (catalog wiring), D-061 (dbt toolchain and workarounds), D-071 (file Bronze),
  D-074 (Silver customers), D-076 (SCD2 history)

## Context
Charter §3 fixes the ownership: Spark owns Bronze, DLQ, Silver and SCD2; dbt owns Gold only and
reads Silver by table name. ADR-0007 chose dbt-spark (`thrift` method) against a Spark Thrift
Server with a PostgreSQL-backed Hive metastore, and named the `session` method as the fallback.
None of that had been run. Before any Gold modelling, W2.D11 had to prove, against the real local
stack, that (a) tables written by Spark jobs are reachable by name, and (b) the dbt features the
Gold layer depends on work on Delta through the Thrift Server.

## Ownership (final, W3.D15.3)
| Layer | Owner | Written by | Addressed by |
|---|---|---|---|
| Bronze `bronze.raw_customers`, `raw_clickstream` | Spark | `bronze_stream` (path), registered in the catalog | name for readers |
| Bronze `bronze.raw_orders`, `raw_products` | Spark | `bronze_batch` (path), registered (D-071) | name for readers |
| DLQ `dead_letter.dlq_*` | Spark | `dlq_gate` (path), registered after write | name for readers |
| Silver `silver.customers` | Spark | `silver_customers`: sequence-guarded MERGE, tombstones, HMAC email token (D-074, D-075) | name |
| SCD2 `silver.customers_history` | Spark | `silver_customers_history`: rebuild of affected keys from Bronze ordered by `_cdc_sequence` (D-076) | name |
| Silver orders, items, clickstream | Spark | W3.D17 jobs | name |
| Gold | dbt | dbt models (Delta, Thrift Server) | name |

Writers keep the storage location as their write contract; the catalog is the read contract
(D-060). dbt never writes Bronze, DLQ or Silver, takes no dbt snapshots of customers (history is
Spark's `silver.customers_history`) and does no hashing of PII: privacy columns (`email_token`,
`phone_masked`) arrive pseudonymised from Silver. Why Spark owns Silver and SCD2: correctness
depends on per-event ordering by `_cdc_sequence`, Bronze lineage, re-read/duplicate detection
and an in-batch reconciliation check against MERGE metrics — logic that is tested in PySpark
(`tests/spark/test_silver_customers.py`, `test_customers_history.py`) and would be split across two
engines if dbt snapshots or incremental models held it. dbt keeps what it is good at: tested,
documented dimensional models on top of clean Silver.

## Spike (W2.D11) — observed results

Stack: Spark 3.5.9, Delta 3.3.3, built-in Hive client 2.3.9, PostgreSQL 15.7 metastore database
`hive_metastore`, Spark Thrift Server (`sql` profile, 1 core / 512 MB executor), dbt-core 1.12.6,
dbt-spark 1.11.0 (`thrift`, PyHive with pure-sasl) from the Windows host.
Commands: `dbt debug`, `dbt build --select spike assert_spike_merge_state --full-refresh`, then the
same with `--vars "{spike_round: 2}"` (incremental round), plus the negative probes listed.

| # | Capability | Probe | Observed | Verdict |
|---|---|---|---|---|
| C1 | Hive metastore on PostgreSQL | Thrift Server start, `SHOW DATABASES` | Metastore schema created in Postgres (28 tables), warehouse `s3a://lakehouse/warehouse` | ✅ with workaround W1 |
| C2 | Spark-written Delta table by name | `CREATE TABLE bronze.raw_customers USING DELTA LOCATION …`; `SELECT count(*)` via Thrift | `3060` rows — equals the W2.D10 Bronze count | ✅ |
| C3 | `dbt debug` over Thrift | `dbt debug --profiles-dir profiles` | `Connection test: OK`, all checks passed | ✅ |
| C4 | dbt reads a Spark table by name | `spike_bronze_by_name` (`source('bronze','raw_customers')`) | view built; `not_null` + `unique` on `brand_id` pass | ✅ |
| C5 | Incremental `merge` on Delta | `spike_merge_composite`, round 1 then round 2 | generated `MERGE INTO … USING … ON` ; round 2 updates one row and inserts one | ✅ |
| C6 | Composite `unique_key` incl. `brand_id` | same model; `brand_b/c1` shares `customer_id` with the updated `brand_a/c1` | ON clause has both columns; `brand_b/c1` untouched; singular test passes in both rounds and **fails (3 rows)** when run with the wrong round's expectation | ✅ |
| C7 | Enforced model contract — types | `spike_contract`; negative `spike_contract_violation` (bigint declared, string produced) | positive builds; negative fails at compile time with the contract mismatch table, nothing written | ✅ |
| C8 | Contract constraint `not_null` (column level) | `constraints: [{type: not_null}]` | **fails**: dbt-spark adds it after CTAS with `ALTER … SET NOT NULL`; OSS Delta 3.3.3 raises `Cannot change nullable column to non-nullable` | ❌ → workaround W3 |
| C9 | Contract constraint `check` (column level) | column-level `type: check` | ignored with warning `Only not_null is supported` | ❌ (not enforced) |
| C10 | Contract constraint `check` (model level) | model-level `brand_id is not null` | becomes Delta table constraint `delta.constraints.brand_id_not_null`; `INSERT … NULL` rejected with `DELTA_VIOLATE_CONSTRAINT_WITH_VALUES` | ✅ |
| C11 | `persist_docs` relation | table comment | `DESCRIBE TABLE EXTENDED` shows the model description | ✅ |
| C12 | `persist_docs` columns | column comments on Delta | upstream macro **fails** (`'NoneType' object is not iterable`) on every model; with workaround W2 the comments are present (`DESCRIBE TABLE`) | ✅ with workaround W2 |

Workarounds (each one recorded in D-060 / D-061 and removable when upstream changes):
- **W1 — metastore schema without foreign keys.** With `datanucleus.schema.autoCreateAll=true`
  the first `CREATE TABLE` self-deadlocked on PostgreSQL: DataNucleus adds a foreign key
  (`ALTER TABLE "TBL_PRIVS" ADD CONSTRAINT … FOREIGN KEY`) on a second connection while the first
  connection still holds the open `INSERT` transaction on `TBLS` (seen in `pg_stat_activity` and a
  JVM thread dump). Tables and columns are now auto-created, constraints are not. Proper fix: the
  official Hive 2.3 PostgreSQL schema script (schematool equivalent); not vendored because the
  repository blocks network downloads from the shell. Local only — AWS uses the Glue Data Catalog.
- **W2 — `spark__alter_column_comment` override** in `dbt_project/macros/`: same SQL, executed as
  a statement without fetching a result set (`run_query` expects one; DDL over PyHive returns none).
- **W3 — not-null via a model-level `check` constraint** plus a `not_null` data test. Column-level
  `not_null` constraints are not used on Delta in this project.

## Decision (W2.D11.3)
- **Keep the Thrift method.** Every capability probed in the spike (C1–C7, C10–C12) works through
  the Spark Thrift Server; not yet verified at volume or under concurrent job and dbt use (V-15).
  The `session` fallback is not needed. Reasons to stay: one long-lived SQL endpoint shared by dbt,
  beeline and later BI tools; the same metastore as the jobs; matches the Spark + Kyuubi/Thrift +
  dbt pattern of ADR-0007.
- **Gold contract style:** enforced contracts for column names and types; NOT NULL and other
  invariants as model-level `check` constraints (become Delta constraints) and dbt data tests.
- **dbt toolchain** in its own virtual environment (`requirements/dbt.txt`, `.venv-dbt/`); the
  committed `dbt_project/profiles/profiles.yml` carries no credentials.

## Consequences
- dbt models must reference Spark tables through `source()` on catalog names; paths never appear in
  dbt SQL.
- Two upstream gaps are carried as code (W2 macro override) and convention (W3); both are listed in
  [VERIFICATION_REGISTER.md](../VERIFICATION_REGISTER.md) to revisit on dbt-spark upgrades.
- The local metastore has no referential integrity in the database (W1); acceptable for a
  single-user local catalog, not a pattern to copy to a shared metastore.
- The Thrift Server has no authentication; it is published on 127.0.0.1 only, but any container
  on the compose network can use it with the S3 and catalog rights of the Spark image (local only;
  V-16).
- Not verified here: concurrency between a running job and dbt on the same metastore, dbt
  `snapshot` (not used, Charter), Delta `merge` performance at volume, Glue/Databricks behaviour
  (W5/W6).
