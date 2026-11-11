"""Silver ``customers_history``: SCD Type 2 versions rebuilt from the Bronze CDC log (DATA_CONTRACTS §4.2).

Spark owns this table (Charter invariant 6, no dbt snapshots). Instead of patching the previous
version when an event arrives, every micro-batch *rebuilds* the complete history of the keys it
touches from Bronze and replaces what is stored for them. The result depends only on the set of
valid Bronze events of a customer, never on the order in which they arrived (D-076).

Rebuild of one ``(brand_id, customer_id)``:

1. Rows with ``_parse_error`` are dropped before ``brand_id`` is used (Q-003); rows failing a
   ``customer_cdc`` rule are dropped (``apply_rules`` inline, as in ``silver.customers``).
2. One row per full key ``(brand_id, customer_id, _cdc_sequence)``: the first valid occurrence by
   Kafka ``(partition, offset)`` (D-029), so duplicates and re-reads never add a version. Copies
   of one key that differ in content are not merged: the lowest offset wins, the others are ignored.
3. Versions are numbered ``1..n`` by ``_cdc_sequence`` only. ``_cdc_ts`` never orders anything.
4. ``valid_from`` is the version's ``_cdc_ts``; ``valid_to`` is the next version's ``_cdc_ts``
   (NULL for the last); ``is_current`` marks the last version. ``D`` is a tombstone version
   (``is_deleted``, PII columns NULL) and ``valid_to`` of the delete closes the previous version.

Equal ``_cdc_ts`` values keep their sequence order, so ``valid_to`` may equal ``valid_from`` (a
zero-length version). If ``_cdc_ts`` runs against the sequence, ``valid_to`` may even be earlier than
``valid_from``; the sequence order is authoritative and the timestamps are informational.

Write: the rebuilt versions are compared with the stored versions of the affected keys. Unchanged
versions are skipped, new or changed ones are upserted and stored versions that no longer exist
(e.g. renumbered after a late intermediate event, or a key that shrank) are deleted, all in **one
Delta MERGE** on ``(brand_id, customer_id, version)`` (a single transaction). The deletes are part of
the MERGE source (an ``_op`` column from a full outer join restricted to the affected keys) because
``WHEN NOT MATCHED BY SOURCE`` conditions cannot express key membership of a DataFrame. A re-run
therefore writes nothing. Single writer per table is assumed (the plan is read before the MERGE).

Performance limit: the rebuild scans Bronze for the batch's keys (O(Bronze), acceptable at local
scale; revisit with partition pruning in M3) and a key's whole history is rewritten when an older
event arrives (the rebuild-vs-patch trade-off, chosen for determinism over write cost).

``_updated_at`` is wall clock and is excluded from every content hash. PySpark and Delta are
imported lazily so this module imports on hosts without them.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from hashlib import sha256
from typing import TYPE_CHECKING

from lakehouse.common.pii import pii_key_broadcast
from lakehouse.quality.rules import apply_rules, cdc_rules
from lakehouse.silver.customers import (
    FULL_KEY,
    KEY,
    ReconciliationError,
    is_valid,
    kafka_position,
    typed_silver_rows,
)


if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import datetime

    from pyspark import Broadcast
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


VERSION_KEY = [*KEY, "version"]
_CONSTRAINT = "brand_id_not_null"
_COLUMNS = (
    "brand_id",
    "customer_id",
    "version",
    "valid_from",
    "valid_to",
    "is_current",
    "is_deleted",
    "_cdc_action",
    "_cdc_sequence",
    "_cdc_ts",
    "first_name",
    "last_name",
    "email_token",
    "phone_masked",
    "city",
    "country",
    "loyalty_tier",
    "signup_date",
    "event_id",
    "_bronze_ref",
    "_updated_at",
)
_CONTENT = tuple(c for c in _COLUMNS if c != "_updated_at")


@dataclass(frozen=True)
class HistoryBatchCounts:
    """Effect of one micro-batch: ``rows_read`` Bronze rows touched ``affected_keys`` customers;
    ``versions_written`` versions were inserted or changed and ``versions_deleted`` removed."""

    rows_read: int
    affected_keys: int
    versions_written: int
    versions_deleted: int

    def as_dict(self) -> dict[str, int]:
        return {
            "rows_read": self.rows_read,
            "affected_keys": self.affected_keys,
            "versions_written": self.versions_written,
            "versions_deleted": self.versions_deleted,
        }


def history_schema() -> StructType:
    """Schema of ``silver.customers_history``; ``brand_id``, ``customer_id``, ``version`` NOT NULL."""
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    types = {
        "brand_id": StringType(),
        "customer_id": StringType(),
        "version": IntegerType(),
        "valid_from": TimestampType(),
        "valid_to": TimestampType(),
        "is_current": BooleanType(),
        "is_deleted": BooleanType(),
        "_cdc_action": StringType(),
        "_cdc_sequence": LongType(),
        "_cdc_ts": TimestampType(),
        "first_name": StringType(),
        "last_name": StringType(),
        "email_token": StringType(),
        "phone_masked": StringType(),
        "city": StringType(),
        "country": StringType(),
        "loyalty_tier": StringType(),
        "signup_date": DateType(),
        "event_id": StringType(),
        "_bronze_ref": StringType(),
        "_updated_at": TimestampType(),
    }
    return StructType([StructField(c, types[c], c not in VERSION_KEY) for c in _COLUMNS])


def ensure_history_table(spark: SparkSession, path: str) -> None:
    """Create the empty history table (NOT NULL keys, CHECK ``brand_id IS NOT NULL``) if missing."""
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        return
    if "`" in path:
        raise ValueError("path must not contain a backtick")
    spark.createDataFrame([], history_schema()).write.format("delta").save(path)
    spark.sql(
        f"ALTER TABLE delta.`{path}` ADD CONSTRAINT {_CONSTRAINT} CHECK (brand_id IS NOT NULL)"
    )


def affected_keys(batch: DataFrame) -> DataFrame:
    """Distinct ``(brand_id, customer_id)`` of the batch's valid rows (parse-ok, contract-valid)."""
    from pyspark.sql import functions as F

    parsed = batch.where(F.col("_parse_error").isNull())
    checked = apply_rules(parsed, cdc_rules())
    return checked.where(is_valid()).select(*KEY).distinct()


def rebuild_history(
    spark: SparkSession,
    bronze_df: DataFrame,
    keys_df: DataFrame,
    key: bytes | Broadcast[bytes],
) -> DataFrame:
    """Complete SCD2 history of the customers in ``keys_df`` (``brand_id``, ``customer_id``).

    Deterministic: the output depends only on the valid Bronze events of those customers. The
    columns are those of :func:`history_schema`; ``_updated_at`` is NULL (the writer stamps it).
    ``key`` is the HMAC key or a handle from ``pii_key_broadcast`` (which the caller destroys).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    parsed = bronze_df.where(F.col("_parse_error").isNull())
    history = apply_rules(parsed.join(F.broadcast(keys_df), KEY, "left_semi"), cdc_rules())
    valid = history.where(is_valid())

    first_occurrence = Window.partitionBy(*FULL_KEY).orderBy(
        *kafka_position(), F.col("_ingest_ts").asc_nulls_last(), F.col("_batch_id").asc_nulls_last()
    )
    firsts = (
        valid.withColumn("_occurrence", F.row_number().over(first_occurrence))
        .where(F.col("_occurrence") == 1)
        .drop("_occurrence")
    )
    typed = typed_silver_rows(firsts, spark, key)

    by_sequence = Window.partitionBy(*KEY).orderBy(F.col("_cdc_sequence").asc())
    return typed.select(
        "brand_id",
        "customer_id",
        F.row_number().over(by_sequence).cast("int").alias("version"),
        F.col("_cdc_ts").alias("valid_from"),
        F.lead("_cdc_ts").over(by_sequence).alias("valid_to"),
        F.lead("_cdc_sequence").over(by_sequence).isNull().alias("is_current"),
        "is_deleted",
        "_cdc_action",
        "_cdc_sequence",
        "_cdc_ts",
        "first_name",
        "last_name",
        "email_token",
        "phone_masked",
        "city",
        "country",
        "loyalty_tier",
        "signup_date",
        "event_id",
        "_bronze_ref",
        F.lit(None).cast("timestamp").alias("_updated_at"),
    )


def history_content_hash(
    df: DataFrame, exclude: Iterable[str] = ("_updated_at", "_bronze_ref")
) -> str:
    """SHA-256 over the rows of ``df``: independent of row and column order, NULL-aware.

    Hashes the business content and the version chain only. ``_updated_at`` (wall clock) and
    ``_bronze_ref`` (lineage: Kafka offsets legitimately differ between in-order and shuffled
    deliveries of the same events) are excluded by default; ``exclude`` replaces that set.
    """
    from pyspark.sql import functions as F

    dropped = set(exclude)
    columns = sorted(c for c in df.columns if c not in dropped)
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("\u0000NULL")) for c in columns]
    digest = F.sha2(F.concat_ws("\u001f", *parts), 256)
    row_hashes = sorted(str(row[0]) for row in df.select(digest).collect())
    return sha256("\n".join([",".join(columns), *row_hashes]).encode("utf-8")).hexdigest()


def _plan(rebuilt: DataFrame, existing: DataFrame) -> DataFrame:
    """Merge source: ``rebuilt`` vs ``existing`` versions with ``_op`` in {upsert, delete}.

    Unchanged versions (all columns but ``_updated_at`` equal) are left out, so nothing is rewritten.
    """
    from pyspark.sql import functions as F

    new = rebuilt.withColumn("_in_new", F.lit(True)).alias("n")
    old = existing.select(*_CONTENT).withColumn("_in_old", F.lit(True)).alias("o")
    joined = new.join(old, VERSION_KEY, "full_outer")
    changed = reduce(
        lambda a, b: a | b,
        [~F.col(f"n.{c}").eqNullSafe(F.col(f"o.{c}")) for c in _CONTENT if c not in VERSION_KEY],
    )
    op = (
        F.when(F.col("_in_new").isNull(), "delete")
        .when(F.col("_in_old").isNull() | changed, "upsert")
        .otherwise("unchanged")
    )
    others = [F.col(f"n.{c}").alias(c) for c in _COLUMNS if c not in VERSION_KEY]
    return joined.select(*VERSION_KEY, *others, op.alias("_op")).where(F.col("_op") != "unchanged")


def apply_history(
    spark: SparkSession, path: str, rebuilt: DataFrame, keys_df: DataFrame
) -> tuple[int, int]:
    """Replace the stored versions of ``keys_df`` by ``rebuilt`` in one MERGE; ``(written, deleted)``.

    The counts are the plan's and are cross-checked against the MERGE's own metrics
    (:class:`ReconciliationError` on disagreement).
    """
    from delta.tables import DeltaTable
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    target = DeltaTable.forPath(spark, path)
    existing = target.toDF().join(keys_df, KEY, "left_semi")
    plan = _plan(rebuilt, existing).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        by_op = {
            row["_op"]: int(row["n"])
            for row in plan.groupBy("_op").agg(F.count(F.lit(1)).alias("n")).collect()
        }
        written, deleted = by_op.get("upsert", 0), by_op.get("delete", 0)
        if written + deleted == 0:
            return 0, 0

        version_before = int(target.history(1).collect()[0]["version"])
        values = {c: f"s.{c}" for c in _COLUMNS}
        (
            target.alias("t")
            .merge(
                plan.alias("s"),
                "t.brand_id = s.brand_id AND t.customer_id = s.customer_id "
                "AND t.version = s.version",
            )
            .whenMatchedDelete(condition="s._op = 'delete'")
            .whenMatchedUpdate(condition="s._op = 'upsert'", set=values)
            .whenNotMatchedInsert(condition="s._op = 'upsert'", values=values)
            .execute()
        )
        commit = (
            target.history(2)
            .where(F.col("version") == version_before + 1)
            .select("operation", "operationMetrics")
            .collect()
        )
    finally:
        plan.unpersist()
    metrics = (
        (commit[0]["operationMetrics"] or {})
        if commit and commit[0]["operation"] == "MERGE"
        else {}
    )
    merged = int(metrics.get("numTargetRowsInserted", 0)) + int(
        metrics.get("numTargetRowsUpdated", 0)
    )
    removed = int(metrics.get("numTargetRowsDeleted", 0))
    if (merged, removed) != (written, deleted):
        raise ReconciliationError(
            f"silver.customers_history: planned {written} written / {deleted} deleted versions "
            f"but MERGE reported {merged} / {removed}"
        )
    return written, deleted


def process_history_batch(
    spark: SparkSession,
    batch: DataFrame,
    bronze: DataFrame,
    path: str,
    key: bytes,
    now: datetime,
) -> HistoryBatchCounts:
    """Rebuild and replace the history of the batch's affected keys; one Delta transaction."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    ensure_history_table(spark, path)
    rows_read = batch.count()
    keys_df = affected_keys(batch).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        key_count = keys_df.count()
        if key_count == 0:
            return HistoryBatchCounts(rows_read, 0, 0, 0)
        with pii_key_broadcast(spark, key) as key_handle:  # destroyed when the block exits
            rebuilt = rebuild_history(spark, bronze, keys_df, key_handle).withColumn(
                "_updated_at", F.lit(now).cast("timestamp")
            )
            written, deleted = apply_history(spark, path, rebuilt, keys_df)
    finally:
        keys_df.unpersist()
    return HistoryBatchCounts(rows_read, key_count, written, deleted)
