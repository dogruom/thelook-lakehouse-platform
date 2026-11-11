"""Silver ``clickstream``: windowed dedup + insert-only MERGE of Bronze ``raw_clickstream`` (§4.3).

One micro-batch of Bronze rows is classified and the surviving events are merged insert-only on
``(brand_id, event_id, event_date)`` (D-079). Replaying a batch leaves the table unchanged (an
idempotent outcome, not exactly-once).

Gate order per Bronze row, reported in ``_outcome``:

1. ``reread``         the row repeats a ``_source_ref`` inside the batch (checkpoint-loss re-read); counted
                      in ``reread_rows``. A re-read in a later batch is not recognised (no Bronze
                      history lookup here) and ends as a MERGE-matched duplicate.
2. ``parse_excluded`` ``_parse_error`` is set (stays in Bronze only).
3. ``dlq_excluded``   the row fails a ``clickstream_event`` rule (``apply_rules``, inline); ``dlq_gate``
                      owns those DLQ rows.
4. ``late_beyond_window`` ``produced_at - event_timestamp > lookback_days`` (both from the payload, the
                      simulated clock; never wall clock, never the target or the rest of the batch).
                      Rows that reach this gate passed the rules, so both timestamps are valid. Late
                      rows stay in Bronze and are only counted.
5. ``duplicate``      not the lowest Kafka ``(partition, offset)`` of its
                      ``(brand_id, event_id, event_date)`` among the remaining rows of the batch.
6. ``accepted``       merge candidate; one whose key is already in the target is a MERGE-matched
                      duplicate and is added to ``duplicates_removed`` (not to ``silver_accepted``).

The late test is a pure function of the row, so the outcome of an event does not depend on batch
boundaries, arrival order or on any other event (a far-future ``event_timestamp`` moves nothing).
The dedup key includes ``event_date`` because an exact re-send repeats the payload and so shares its
``event_date``; that lets the MERGE prune to the partitions of the source (an explicit date list is
added to the condition). Two copies of one ``event_id`` with *different* ``event_timestamp`` dates
are therefore kept as two rows; the contract treats ``event_id`` as unique per event, so the
generator does not produce that. The window is a deliberate trade: an event produced more than
``lookback_days`` after it happened is never written, even when it is new. No foreign keys
(``customer_id`` may be NULL for guests). The table is partitioned by ``event_date``.
PySpark and Delta are imported lazily so this module imports on hosts without them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakehouse.quality.rules import apply_rules, canonical_ts, clickstream_rules
from lakehouse.silver.customers import (
    MultipleMatchError,
    ReconciliationError,
    is_valid,
    kafka_position,
)


if TYPE_CHECKING:
    from datetime import date, datetime

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


OUTCOMES = ("parse_excluded", "dlq_excluded", "reread", "late", "duplicate", "accepted")
KEY = ["brand_id", "event_id", "event_date"]
DEFAULT_LOOKBACK_DAYS = 7
_CONSTRAINT = "brand_id_not_null"
_MICROS_PER_DAY = 86_400_000_000


@dataclass(frozen=True)
class ClickstreamBatchCounts:
    """Outcome counts of one micro-batch, in Bronze rows (``reread_rows`` are extra copies).

    ``rows_read - reread_rows == parse_excluded + dlq_excluded + duplicates_removed +
    late_beyond_window + silver_accepted``.
    """

    rows_read: int
    reread_rows: int
    parse_excluded: int
    dlq_excluded: int
    duplicates_removed: int
    late_beyond_window: int
    silver_accepted: int

    def as_dict(self) -> dict[str, int]:
        return {
            "rows_read": self.rows_read,
            "reread_rows": self.reread_rows,
            "parse_excluded": self.parse_excluded,
            "dlq_excluded": self.dlq_excluded,
            "duplicates_removed": self.duplicates_removed,
            "late_beyond_window": self.late_beyond_window,
            "silver_accepted": self.silver_accepted,
        }


def silver_clickstream_schema() -> StructType:
    """Schema of ``silver.clickstream``; ``brand_id`` and ``event_id`` are NOT NULL."""
    from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("brand_id", StringType(), False),
            StructField("event_id", StringType(), False),
            StructField("session_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("event_type", StringType()),
            StructField("page_url", StringType()),
            StructField("device_category", StringType()),
            StructField("event_timestamp", TimestampType()),
            StructField("event_date", DateType()),
            StructField("produced_at", TimestampType()),
            StructField("_kafka_ts", TimestampType()),
            StructField("_bronze_ref", StringType()),
            StructField("_bronze_batch_id", StringType()),
            StructField("_updated_at", TimestampType()),
        ]
    )


def ensure_table(spark: SparkSession, path: str) -> None:
    """Create the empty Silver table (partitioned by ``event_date``) if missing."""
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        return
    if "`" in path:
        raise ValueError("path must not contain a backtick")
    (
        spark.createDataFrame([], silver_clickstream_schema())
        .write.format("delta")
        .partitionBy("event_date")
        .save(path)
    )
    spark.sql(
        f"ALTER TABLE delta.`{path}` ADD CONSTRAINT {_CONSTRAINT} CHECK (brand_id IS NOT NULL)"
    )


def check_clickstream(batch: DataFrame) -> DataFrame:
    """``batch`` plus ``_ref_rank``, the rule columns, ``_event_ts``, ``_produced_ts``, ``_event_date``."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    one_per_ref = Window.partitionBy("_source_ref").orderBy(
        F.col("_ingest_ts").asc_nulls_last(), F.col("_batch_id").asc_nulls_last()
    )
    stamped = (
        batch.withColumn("_ref_rank", F.row_number().over(one_per_ref))
        .withColumn("_event_ts", F.to_timestamp(canonical_ts(F.col("event_timestamp"))))
        .withColumn("_produced_ts", F.to_timestamp(canonical_ts(F.col("produced_at"))))
    )
    return apply_rules(stamped, clickstream_rules()).withColumn(
        "_event_date", F.to_date(F.col("_event_ts"))
    )


def classify_checked(checked: DataFrame, lookback_days: int) -> DataFrame:
    """Add ``_outcome`` to the output of :func:`check_clickstream`.

    A row is late iff ``_produced_ts - _event_ts`` exceeds ``lookback_days`` (microsecond exact;
    NULL timestamps, which only occur on rows that fail a rule, are never late).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    valid = is_valid()
    lag_micros = F.unix_micros(F.col("_produced_ts")) - F.unix_micros(F.col("_event_ts"))
    late = F.coalesce(lag_micros > F.lit(lookback_days * _MICROS_PER_DAY), F.lit(False))
    candidate = valid & (F.col("_ref_rank") == 1) & ~late
    # Rows that are not candidates get a per-row partition so they never pile into one task.
    by_key = Window.partitionBy(
        F.when(candidate, F.col("brand_id")).otherwise(F.concat(F.lit("~"), F.col("_source_ref"))),
        F.when(candidate, F.col("event_id")),
        F.when(candidate, F.col("_event_date")),
    ).orderBy(*kafka_position())
    ranked = checked.withColumn("_key_rank", F.row_number().over(by_key))
    outcome = (
        F.when(F.col("_ref_rank") > 1, "reread")
        .when(F.col("_parse_error").isNotNull(), "parse_excluded")
        .when(F.size("_rejection_codes") > 0, "dlq_excluded")
        .when(late, "late")
        .when(F.col("_key_rank") > 1, "duplicate")
        .otherwise("accepted")
    )
    return ranked.withColumn("_outcome", outcome).drop("_ref_rank", "_key_rank")


def classify_clickstream(batch: DataFrame, lookback_days: int) -> DataFrame:
    """``batch`` plus the rule columns, ``_event_ts``, ``_event_date`` and ``_outcome``."""
    return classify_checked(check_clickstream(batch), lookback_days)


def typed_clickstream(rows: DataFrame) -> DataFrame:
    """Typed ``silver.clickstream`` columns (without ``_updated_at``) of classified ``rows``."""
    from pyspark.sql import functions as F

    return rows.select(
        F.col("brand_id"),
        F.col("event_id"),
        F.col("session_id"),
        F.col("customer_id"),
        F.col("event_type"),
        F.col("page_url"),
        F.col("device_category"),
        F.col("_event_ts").alias("event_timestamp"),
        F.col("_event_date").alias("event_date"),
        F.to_timestamp(canonical_ts(F.col("produced_at"))).alias("produced_at"),
        F.col("_kafka_ts"),
        F.col("_source_ref").alias("_bronze_ref"),
        F.col("_batch_id").alias("_bronze_batch_id"),
    )


def _merge_insert_only(
    spark: SparkSession, path: str, source: DataFrame, candidates: int
) -> tuple[int, int]:
    """Insert-only MERGE on ``(brand_id, event_id, event_date)``; returns ``(matched, inserted)``.

    The condition lists the source's ``event_date`` values so Delta prunes to those partitions.
    ``matched`` (source keys already in the target) is predicted before the MERGE; ``inserted`` is
    the MERGE's own metric, read from the history row of version ``before + 1``.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    dates: list[date] = sorted(
        r["event_date"] for r in source.select("event_date").distinct().collect()
    )
    in_dates = (
        spark.read.format("delta").load(path).where(F.col("event_date").isin(dates)).select(*KEY)
    )
    matched = source.join(in_dates, KEY, "left_semi").count()
    distinct = source.select(*KEY).distinct().count()
    if distinct != candidates:
        raise MultipleMatchError(
            f"MERGE source has {candidates} rows for {distinct} distinct "
            "(brand_id, event_id, event_date) keys"
        )
    target = DeltaTable.forPath(spark, path)
    version_before = int(target.history(1).collect()[0]["version"])
    listed = ", ".join(f"DATE'{d.isoformat()}'" for d in dates)
    (
        target.alias("t")
        .merge(
            source.alias("s"),
            "t.brand_id = s.brand_id AND t.event_id = s.event_id "
            f"AND t.event_date = s.event_date AND t.event_date IN ({listed})",
        )
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
        return matched, 0
    return matched, int((history[0]["operationMetrics"] or {}).get("numTargetRowsInserted", 0))


def process_clickstream_batch(
    spark: SparkSession,
    batch: DataFrame,
    path: str,
    lookback_days: int,
    now: datetime,
) -> ClickstreamBatchCounts:
    """Classify, merge and reconcile one micro-batch; raises :class:`ReconciliationError`."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    if lookback_days < 0:
        raise ValueError("lookback_days must be >= 0")
    ensure_table(spark, path)
    classified = classify_clickstream(batch, lookback_days).persist(StorageLevel.MEMORY_AND_DISK)
    source = None
    try:
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
            typed_clickstream(classified.where(F.col("_outcome") == "accepted"))
            .withColumn("_updated_at", F.lit(now).cast("timestamp"))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        candidates = by_outcome.get("accepted", 0)
        matched = 0
        inserted = 0
        if candidates:
            matched, inserted = _merge_insert_only(spark, path, source, candidates)
    finally:
        classified.unpersist()
        if source is not None:
            source.unpersist()

    counts = ClickstreamBatchCounts(
        rows_read=sum(by_outcome.values()),
        reread_rows=by_outcome.get("reread", 0),
        parse_excluded=by_outcome.get("parse_excluded", 0),
        dlq_excluded=by_outcome.get("dlq_excluded", 0),
        duplicates_removed=by_outcome.get("duplicate", 0) + matched,
        late_beyond_window=by_outcome.get("late", 0),
        silver_accepted=inserted,
    )
    problems: list[str] = []
    if inserted != candidates - matched:
        problems.append(
            f"MERGE inserted {inserted} but {candidates} candidates minus {matched} matched is "
            f"{candidates - matched}"
        )
    accounted = (
        counts.parse_excluded
        + counts.dlq_excluded
        + counts.duplicates_removed
        + counts.late_beyond_window
        + counts.silver_accepted
    )
    if accounted != distinct_refs or counts.rows_read - counts.reread_rows != distinct_refs:
        problems.append(
            f"distinct _source_ref={distinct_refs} but outcomes add up to {accounted} "
            f"(rows_read={counts.rows_read}, reread_rows={counts.reread_rows})"
        )
    if problems:
        raise ReconciliationError("silver.clickstream batch: " + "; ".join(problems))
    return counts
