"""Silver ``customers``: sequence-ordered CDC MERGE of Bronze ``raw_customers`` (DATA_CONTRACTS §4.1).

One micro-batch of Bronze rows is classified, reduced to at most one row per ``(brand_id,
customer_id)`` and merged with ``source._cdc_sequence > target._cdc_sequence`` as the only apply
rule, so replaying a batch leaves the table unchanged (an idempotent outcome, not exactly-once).

Gate order per Bronze row (D-022, D-023, D-029), reported in ``_outcome``:

1. ``reread``         the row repeats a ``_source_ref`` (a checkpoint-loss re-read of one Kafka
                      message): either another row of the same batch has it (one per ref
                      continues, the rest are counted), or a *valid* earlier Bronze copy exists
                      with a smaller ``_ingest_ts`` (D-057, re-read in a later batch). Re-reads
                      are counted (``reread_rows``), they are neither duplicates nor noops.
                      An invalid (parse/DLQ) re-read in a later batch is not recognised here and
                      stays in its exclusion counter.
2. ``parse_excluded`` ``_parse_error`` is set: decided before any ``brand_id`` is used (Q-003).
3. ``dlq_excluded``   the row fails a ``customer_cdc`` rule (``apply_rules``, evaluated inline; no
                      anti-join on the DLQ table, so no ordering dependency on ``dlq_gate``).
4. ``duplicate``      the row is not the first valid occurrence of its full key ``(brand_id,
                      customer_id, _cdc_sequence)`` in Bronze, ordered by Kafka ``(partition,
                      offset)`` parsed from ``_source_ref`` (a customer's key pins it to one
                      partition). Only valid rows count, so a DLQ row never makes a later valid
                      replay a duplicate, and a replay of an event that ended as NOOP is still one.
5. ``accepted``       the highest-sequence remaining row of its ``(brand_id, customer_id)`` in the
                      batch (the merge candidate).
6. ``stale_noop``     every other remaining row of the key in the batch.

Agreement with ``generator.manifest.classify_outcomes`` for multi-event batches: that oracle takes
the max-sequence non-duplicate valid candidate per key and batch, marks it ACCEPT iff its sequence
exceeds the sequence applied by earlier batches, and every other candidate NOOP. Steps 1 to 6 are the
same rule; the only part that needs the target is "exceeds what is applied", so ``classify_batch``
cannot decide it. ``process_batch`` does: a candidate the MERGE would not apply is counted
``stale_cdc_noop``, an applied one ``silver_accepted`` (predicted from the target before the MERGE
and cross-checked against the MERGE's own insert/update metrics). A candidate whose sequence ties the
target's is a NOOP, as in the oracle.

Tombstone (``D``): ``is_deleted = true``; ``first_name``, ``last_name``, ``email_token``,
``phone_masked`` and ``city`` are NULL; ``country``, ``loyalty_tier`` and ``signup_date`` stay. A
``D`` into an empty target inserts the tombstone. ``email_token`` is an HMAC pseudonym (see
``lakehouse.common.pii``), personal data, not anonymous. Resurrection: a ``U`` with a higher
sequence than the tombstone's is applied and clears ``is_deleted`` (sequence order wins; a lower
one stays a noop).

Performance limit: the per-batch history lookup scans Bronze for the batch's full keys (O(Bronze),
acceptable at local scale; revisit with partition pruning in M3).

``_updated_at`` is wall clock (the ``now`` of ``process_batch``) and belongs in no content hash.
PySpark and Delta are imported lazily so this module imports on hosts without them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakehouse.common.pii import email_token_column, phone_last4_column, pii_key_broadcast
from lakehouse.quality.rules import _canonical_ts, apply_rules, cdc_rules


if TYPE_CHECKING:
    from datetime import datetime

    from pyspark import Broadcast
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.types import DataType, StructType


OUTCOMES = ("parse_excluded", "dlq_excluded", "reread", "duplicate", "stale_noop", "accepted")
_KEY = ["brand_id", "customer_id"]
_FULL_KEY = [*_KEY, "_cdc_sequence"]
_CONSTRAINT = "brand_id_not_null"


class MultipleMatchError(RuntimeError):
    """The MERGE source holds more than one row for a ``(brand_id, customer_id)`` key."""


class ReconciliationError(RuntimeError):
    """A batch's counters or the MERGE metrics disagree with each other."""


@dataclass(frozen=True)
class CustomerBatchCounts:
    """Outcome counts of one micro-batch, in Bronze rows (``reread_rows`` are extra copies).

    ``rows_read - reread_rows == parse_excluded + dlq_excluded + duplicates_removed +
    stale_cdc_noop + silver_accepted``. ``late_beyond_window`` is constant 0 by design: the
    sequence-guarded MERGE has no event-time window.
    """

    rows_read: int
    reread_rows: int
    parse_excluded: int
    dlq_excluded: int
    duplicates_removed: int
    stale_cdc_noop: int
    silver_accepted: int
    late_beyond_window: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "rows_read": self.rows_read,
            "reread_rows": self.reread_rows,
            "parse_excluded": self.parse_excluded,
            "dlq_excluded": self.dlq_excluded,
            "duplicates_removed": self.duplicates_removed,
            "stale_cdc_noop": self.stale_cdc_noop,
            "silver_accepted": self.silver_accepted,
            "late_beyond_window": self.late_beyond_window,
        }


def silver_customers_schema() -> StructType:
    """Schema of ``silver.customers``; ``brand_id`` and ``customer_id`` are NOT NULL."""
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    def field(name: str, data_type: DataType, nullable: bool = True) -> StructField:
        return StructField(name, data_type, nullable)

    return StructType(
        [
            field("brand_id", StringType(), False),
            field("customer_id", StringType(), False),
            field("first_name", StringType()),
            field("last_name", StringType()),
            field("email_token", StringType()),
            field("phone_masked", StringType()),
            field("city", StringType()),
            field("country", StringType()),
            field("loyalty_tier", StringType()),
            field("signup_date", DateType()),
            field("is_deleted", BooleanType()),
            field("_cdc_action", StringType()),
            field("_cdc_sequence", LongType()),
            field("_cdc_ts", TimestampType()),
            field("produced_at", TimestampType()),
            field("event_id", StringType()),
            field("_bronze_ref", StringType()),
            field("_bronze_batch_id", StringType()),
            field("_updated_at", TimestampType()),
        ]
    )


def _is_valid() -> Column:
    from pyspark.sql import functions as F

    return F.col("_parse_error").isNull() & (F.size("_rejection_codes") == 0)


def _kafka_position() -> list[Column]:
    """``(partition, offset)`` parsed from ``topic/partition/offset``; NULL when not that shape."""
    from pyspark.sql import functions as F

    ref = F.col("_source_ref")
    return [
        F.regexp_extract(ref, r"/([0-9]+)/([0-9]+)$", 1).cast("long"),
        F.regexp_extract(ref, r"/([0-9]+)/([0-9]+)$", 2).cast("long"),
        ref,
    ]


def classify_batch(batch: DataFrame, bronze: DataFrame) -> DataFrame:
    """``batch`` (Bronze rows of one micro-batch) plus the rule columns and ``_outcome``.

    ``bronze`` is the Bronze table, read for earlier occurrences of the batch's full keys; rows of
    ``batch`` itself need not be in it. ``accepted`` means "merge candidate": whether the target
    applies it is decided by the MERGE (see the module docstring).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    rules = cdc_rules()
    one_per_ref = Window.partitionBy("_source_ref").orderBy(
        F.col("_ingest_ts").asc_nulls_last(), F.col("_batch_id").asc_nulls_last()
    )
    checked = apply_rules(batch.withColumn("_ref_rank", F.row_number().over(one_per_ref)), rules)
    valid = _is_valid()
    first_copy = valid & (F.col("_ref_rank") == 1)

    current = checked.where(first_copy)
    keys = current.select(*_FULL_KEY).distinct()
    history = apply_rules(bronze.join(F.broadcast(keys), _FULL_KEY, "left_semi"), rules)
    # One row per valid Bronze/current ``_source_ref`` with its earliest ingest time.
    refs = (
        history.where(_is_valid())
        .select(*_FULL_KEY, "_source_ref", "_ingest_ts")
        .unionByName(current.select(*_FULL_KEY, "_source_ref", "_ingest_ts"))
        .groupBy("_source_ref", *_FULL_KEY)
        .agg(F.min("_ingest_ts").alias("_first_ingest_ts"))
    )
    occurrence = Window.partitionBy(*_FULL_KEY).orderBy(*_kafka_position())
    ranked = refs.select(
        "_source_ref", "_first_ingest_ts", F.row_number().over(occurrence).alias("_dup_rank")
    )

    joined = (
        checked.join(ranked, "_source_ref", "left")
        .withColumn("_dup_rank", F.coalesce(F.col("_dup_rank"), F.lit(1)))
        .withColumn(
            "_later_reread",
            F.coalesce(valid & (F.col("_ingest_ts") > F.col("_first_ingest_ts")), F.lit(False)),
        )
    )
    candidate = first_copy & ~F.col("_later_reread") & (F.col("_dup_rank") == 1)
    # Rows that are not candidates get a per-row partition so they never pile into one task.
    by_key = Window.partitionBy(
        F.when(candidate, F.col("brand_id")).otherwise(F.concat(F.lit("~"), F.col("_source_ref"))),
        F.when(candidate, F.col("customer_id")),
    ).orderBy(F.col("_cdc_sequence").desc(), F.col("_source_ref").asc())
    ranked_rows = joined.withColumn("_key_rank", F.row_number().over(by_key))
    outcome = (
        F.when((F.col("_ref_rank") > 1) | F.col("_later_reread"), "reread")
        .when(F.col("_parse_error").isNotNull(), "parse_excluded")
        .when(F.size("_rejection_codes") > 0, "dlq_excluded")
        .when(F.col("_dup_rank") > 1, "duplicate")
        .when(F.col("_key_rank") == 1, "accepted")
        .otherwise("stale_noop")
    )
    return ranked_rows.withColumn("_outcome", outcome).drop(
        "_ref_rank", "_dup_rank", "_key_rank", "_first_ingest_ts", "_later_reread"
    )


def merge_source(
    classified: DataFrame, spark: SparkSession, key: bytes | Broadcast[bytes]
) -> DataFrame:
    """Typed Silver rows (without ``_updated_at``) of the ``accepted`` candidates, one per key.

    Casts happen here, after the rules (D-045). A ``D`` row becomes a tombstone (PII columns NULL).
    ``key`` is the HMAC key or a handle from ``pii_key_broadcast`` (which the caller destroys).
    """
    from pyspark.sql import functions as F

    tombstone = F.col("_cdc_action") == "D"

    def kept(column: Column) -> Column:
        return F.when(~tombstone, column)

    return classified.where(F.col("_outcome") == "accepted").select(
        F.col("brand_id"),
        F.col("customer_id"),
        kept(F.col("first_name")).alias("first_name"),
        kept(F.col("last_name")).alias("last_name"),
        email_token_column(spark, key, kept(F.col("email"))).alias("email_token"),
        kept(phone_last4_column(F.col("phone"))).alias("phone_masked"),
        kept(F.col("city")).alias("city"),
        F.col("country"),
        F.col("loyalty_tier"),
        F.col("signup_date").cast("date").alias("signup_date"),
        tombstone.alias("is_deleted"),
        F.col("_cdc_action"),
        F.col("_cdc_sequence").cast("long").alias("_cdc_sequence"),
        F.to_timestamp(_canonical_ts(F.col("_cdc_ts"))).alias("_cdc_ts"),
        F.to_timestamp(_canonical_ts(F.col("produced_at"))).alias("produced_at"),
        F.col("event_id"),
        F.col("_source_ref").alias("_bronze_ref"),
        F.col("_batch_id").alias("_bronze_batch_id"),
    )


def ensure_table(spark: SparkSession, path: str) -> None:
    """Create the empty Silver table (NOT NULL keys, CHECK ``brand_id IS NOT NULL``) if missing."""
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        return
    if "`" in path:
        raise ValueError("path must not contain a backtick")
    spark.createDataFrame([], silver_customers_schema()).write.format("delta").save(path)
    spark.sql(
        f"ALTER TABLE delta.`{path}` ADD CONSTRAINT {_CONSTRAINT} CHECK (brand_id IS NOT NULL)"
    )


def merge_customers(spark: SparkSession, path: str, source: DataFrame) -> tuple[int, int]:
    """MERGE ``source`` into the table at ``path``; returns ``(inserted, updated)`` rows.

    Raises :class:`MultipleMatchError` when ``source`` has two rows for one key (Delta would fail
    with a less specific message, or worse, pick one). The counts are the MERGE's own metrics, read
    from the history row of version ``before + 1`` (0/0 when nothing was committed).
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if "_updated_at" not in source.columns:
        source = source.withColumn("_updated_at", F.current_timestamp())
    stats = source.agg(
        F.count(F.lit(1)).alias("rows"), F.countDistinct(F.struct(*_KEY)).alias("distinct")
    ).first()
    rows = int(stats["rows"]) if stats is not None else 0
    if rows == 0:
        return 0, 0
    distinct = int(stats["distinct"]) if stats is not None else 0
    if rows != distinct:
        raise MultipleMatchError(
            f"MERGE source has {rows} rows for {distinct} distinct (brand_id, customer_id) keys"
        )

    target = DeltaTable.forPath(spark, path)
    version_before = int(target.history(1).collect()[0]["version"])
    (
        target.alias("t")
        .merge(source.alias("s"), "t.brand_id = s.brand_id AND t.customer_id = s.customer_id")
        .whenMatchedUpdateAll(condition="s._cdc_sequence > t._cdc_sequence")
        .whenNotMatchedInsertAll()
        .execute()
    )
    history = (
        target.history(2)
        .where(F.col("version") == version_before + 1)
        .select("operation", "operationMetrics")
        .collect()
    )
    if not history or history[0]["operation"] != "MERGE":
        return 0, 0
    metrics = history[0]["operationMetrics"] or {}
    return int(metrics.get("numTargetRowsInserted", 0)), int(metrics.get("numTargetRowsUpdated", 0))


def process_batch(
    spark: SparkSession,
    batch: DataFrame,
    bronze: DataFrame,
    path: str,
    key: bytes,
    now: datetime,
) -> CustomerBatchCounts:
    """Classify, merge and reconcile one micro-batch; raises :class:`ReconciliationError`."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    ensure_table(spark, path)
    classified = classify_batch(batch, bronze).persist(StorageLevel.MEMORY_AND_DISK)
    source = None
    try:
        with pii_key_broadcast(spark, key) as key_handle:  # destroyed when the block exits
            # One pass: rows per outcome and the distinct _source_refs of the non-reread rows.
            stats = classified.agg(
                F.countDistinct(F.when(F.col("_outcome") != "reread", F.col("_source_ref"))).alias(
                    "_distinct_refs"
                ),
                *[
                    F.count(F.when(F.col("_outcome") == name, F.lit(1))).alias(name)
                    for name in OUTCOMES
                ],
            ).first()
            by_outcome = {name: int(stats[name]) for name in OUTCOMES} if stats else {}
            distinct_refs = int(stats["_distinct_refs"]) if stats else 0
            source = (
                merge_source(classified, spark, key_handle)
                .withColumn("_updated_at", F.lit(now).cast("timestamp"))
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            target = spark.read.format("delta").load(path)
            applicable = (
                source.join(
                    target.select(*_KEY, F.col("_cdc_sequence").alias("_target_sequence")),
                    _KEY,
                    "left",
                )
                .where(
                    F.col("_target_sequence").isNull()
                    | (F.col("_cdc_sequence") > F.col("_target_sequence"))
                )
                .count()
            )
            inserted, updated = merge_customers(spark, path, source)
    finally:
        classified.unpersist()
        if source is not None:
            source.unpersist()

    rows_read = sum(by_outcome.values())
    candidates = by_outcome.get("accepted", 0)
    counts = CustomerBatchCounts(
        rows_read=rows_read,
        reread_rows=by_outcome.get("reread", 0),
        parse_excluded=by_outcome.get("parse_excluded", 0),
        dlq_excluded=by_outcome.get("dlq_excluded", 0),
        duplicates_removed=by_outcome.get("duplicate", 0),
        stale_cdc_noop=by_outcome.get("stale_noop", 0) + candidates - applicable,
        silver_accepted=applicable,
    )
    problems: list[str] = []
    if inserted + updated != counts.silver_accepted:
        problems.append(
            f"silver_accepted={counts.silver_accepted} but MERGE inserted {inserted} and "
            f"updated {updated}"
        )
    accounted = (
        counts.parse_excluded
        + counts.dlq_excluded
        + counts.duplicates_removed
        + counts.stale_cdc_noop
        + counts.silver_accepted
    )
    if accounted != distinct_refs or rows_read - counts.reread_rows != distinct_refs:
        problems.append(
            f"distinct _source_ref={distinct_refs} but outcomes add up to {accounted} "
            f"(rows_read={rows_read}, reread_rows={counts.reread_rows})"
        )
    if problems:
        raise ReconciliationError("silver.customers batch: " + "; ".join(problems))
    return counts
