"""Silver ``clickstream``: windowed dedup + insert-only MERGE of Bronze ``raw_clickstream`` (§4.3).

One micro-batch of Bronze rows is classified and the surviving events are merged insert-only on
``(brand_id, event_id)``, bounded by an ``event_date`` lookback window so the MERGE only looks at
recent partitions (D-079). Replaying a batch leaves the table unchanged (an idempotent outcome, not
exactly-once).

Gate order per Bronze row, reported in ``_outcome``:

1. ``reread``         the row repeats a ``_source_ref`` inside the batch (checkpoint-loss re-read); counted
                      in ``reread_rows``. A re-read in a later batch is not recognised (no Bronze
                      history lookup here) and ends as a MERGE-matched duplicate.
2. ``parse_excluded`` ``_parse_error`` is set (stays in Bronze only).
3. ``dlq_excluded``   the row fails a ``clickstream_event`` rule (``apply_rules``, inline); ``dlq_gate``
                      owns those DLQ rows.
4. ``late_beyond_window`` ``event_date < date(hwm) - lookback_days``, where ``hwm`` is the maximum
                      ``event_timestamp`` over the target and the valid rows of this batch (event time:
                      ``_kafka_ts`` is wall clock). Late rows stay in Bronze and are only counted.
5. ``duplicate``      not the lowest Kafka ``(partition, offset)`` of its ``(brand_id, event_id)`` among
                      the remaining rows of the batch.
6. ``accepted``       merge candidate; one already in the window of the target is a MERGE-matched
                      duplicate and is added to ``duplicates_removed`` (not to ``silver_accepted``).

The window is a deliberate trade: an event older than the window is never written even when it is
new. A far-future ``event_timestamp`` moves ``hwm`` and so the window (there is no future check in the
clickstream contract); revisit with a sanity bound if that appears in real data. No foreign keys
(``customer_id`` may be NULL for guests). The table is partitioned by ``event_date``.
PySpark and Delta are imported lazily so this module imports on hosts without them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

from lakehouse.quality.rules import _canonical_ts, apply_rules, clickstream_rules
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
KEY = ["brand_id", "event_id"]
DEFAULT_LOOKBACK_DAYS = 7
_CONSTRAINT = "brand_id_not_null"


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


def _high_watermark_date(spark: SparkSession, path: str, checked: DataFrame) -> date | None:
    """UTC date of the maximum ``event_timestamp`` over the target and the valid rows of ``checked``."""
    from pyspark.sql import functions as F

    batch_row = checked.where(is_valid()).agg(F.max("_event_date").alias("d")).first()
    target_row = spark.read.format("delta").load(path).agg(F.max("event_date").alias("d")).first()
    found = [r["d"] for r in (batch_row, target_row) if r is not None and r["d"] is not None]
    return max(found) if found else None


def check_clickstream(batch: DataFrame) -> DataFrame:
    """``batch`` plus ``_ref_rank``, the rule columns, ``_event_ts`` and ``_event_date``."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    one_per_ref = Window.partitionBy("_source_ref").orderBy(
        F.col("_ingest_ts").asc_nulls_last(), F.col("_batch_id").asc_nulls_last()
    )
    stamped = batch.withColumn("_ref_rank", F.row_number().over(one_per_ref)).withColumn(
        "_event_ts", F.to_timestamp(_canonical_ts(F.col("event_timestamp")))
    )
    return apply_rules(stamped, clickstream_rules()).withColumn(
        "_event_date", F.to_date(F.col("_event_ts"))
    )


def classify_checked(checked: DataFrame, lower: date | None) -> DataFrame:
    """Add ``_outcome`` to the output of :func:`check_clickstream`.

    ``lower`` is the first ``event_date`` inside the window (``None``: no window, nothing is late).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    valid = is_valid()
    late = (
        F.lit(False)
        if lower is None
        else F.coalesce(F.col("_event_date") < F.lit(lower.isoformat()).cast("date"), F.lit(False))
    )
    candidate = valid & (F.col("_ref_rank") == 1) & ~late
    # Rows that are not candidates get a per-row partition so they never pile into one task.
    by_key = Window.partitionBy(
        F.when(candidate, F.col("brand_id")).otherwise(F.concat(F.lit("~"), F.col("_source_ref"))),
        F.when(candidate, F.col("event_id")),
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


def classify_clickstream(batch: DataFrame, lower: date | None) -> DataFrame:
    """``batch`` plus the rule columns, ``_event_ts``, ``_event_date`` and ``_outcome``."""
    return classify_checked(check_clickstream(batch), lower)


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
        F.to_timestamp(_canonical_ts(F.col("produced_at"))).alias("produced_at"),
        F.col("_kafka_ts"),
        F.col("_source_ref").alias("_bronze_ref"),
        F.col("_batch_id").alias("_bronze_batch_id"),
    )


def _merge_in_window(
    spark: SparkSession, path: str, source: DataFrame, lower: date, candidates: int
) -> tuple[int, int]:
    """Insert-only MERGE bounded to ``event_date >= lower``; returns ``(matched, inserted)``.

    ``matched`` (source keys already in the window of the target) is predicted before the MERGE;
    ``inserted`` is the MERGE's own metric, read from the history row of version ``before + 1``.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    window = F.lit(lower.isoformat()).cast("date")
    in_window = spark.read.format("delta").load(path).where(F.col("event_date") >= window)
    matched = source.join(in_window.select(*KEY), KEY, "left_semi").count()
    distinct = source.select(*KEY).distinct().count()
    if distinct != candidates:
        raise MultipleMatchError(
            f"MERGE source has {candidates} rows for {distinct} distinct (brand_id, event_id) keys"
        )
    target = DeltaTable.forPath(spark, path)
    version_before = int(target.history(1).collect()[0]["version"])
    (
        target.alias("t")
        .merge(
            source.alias("s"),
            "t.brand_id = s.brand_id AND t.event_id = s.event_id "
            f"AND t.event_date >= DATE'{lower.isoformat()}'",
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
    # The window needs the rule result and the target: rules run once, the window then decides.
    checked = check_clickstream(batch).persist(StorageLevel.MEMORY_AND_DISK)
    classified = None
    source = None
    try:
        hwm = _high_watermark_date(spark, path, checked)
        lower = hwm - timedelta(days=lookback_days) if hwm is not None else None
        classified = classify_checked(checked, lower).persist(StorageLevel.MEMORY_AND_DISK)
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
        if candidates and lower is not None:
            matched, inserted = _merge_in_window(spark, path, source, lower, candidates)
    finally:
        checked.unpersist()
        if classified is not None:
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
