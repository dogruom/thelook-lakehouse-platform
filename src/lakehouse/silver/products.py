"""Silver ``products``: latest valid Bronze copy per ``(brand_id, product_id)`` (D-091).

``silver.products`` is Spark-owned (dbt reads it, never writes it). One run reads the whole Bronze
``raw_products`` table and classifies every row (``_outcome``):

1. ``parse_excluded`` ``_parse_error`` is set (stays in Bronze only).
2. ``dlq_excluded``   the row fails a ``product`` rule (``apply_rules``, inline: no dependency on
                      ``dlq_gate`` having run). The ``dlq_gate`` job owns those DLQ rows.
3. ``superseded``     a *valid* copy of the same ``(brand_id, product_id)`` with a later
                      ``(_ingest_ts, landing file key, line number)`` exists (ties on the full
                      ``_source_ref``): a re-landed catalog replaces the older copy.
4. ``accepted``       the latest valid copy; merge candidate.

The accepted rows are merged on ``(brand_id, product_id)``: inserted when new, updated only when the
Bronze reference (``_bronze_ref``) differs from the target's, so a rerun on unchanged Bronze
rewrites nothing (an idempotent outcome, not exactly-once). Nothing is deleted. ``brand_id`` is the
landing-path brand; a payload/path mismatch is already a rule failure.
PySpark and Delta are imported lazily so this module imports on hosts without them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakehouse.quality.rules import apply_rules, product_rules
from lakehouse.silver.customers import ReconciliationError, is_valid
from lakehouse.silver.orders import _landing_position


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

    from lakehouse.common.tables import ResolvedTable


OUTCOMES = ("parse_excluded", "dlq_excluded", "superseded", "accepted")
KEY = ["brand_id", "product_id"]
SOURCE_TABLE = "bronze.raw_products"
_CONSTRAINT = "brand_id_not_null"


@dataclass(frozen=True)
class ProductCounts:
    """Counts of one run, in Bronze rows.

    ``bronze_rows == parse_excluded + dlq_excluded + superseded + accepted``;
    ``inserted + updated <= accepted`` (an unchanged key is neither).
    """

    bronze_rows: int
    parse_excluded: int
    dlq_excluded: int
    superseded: int
    accepted: int
    inserted: int
    updated: int

    def as_dict(self) -> dict[str, int]:
        return {
            "bronze_rows": self.bronze_rows,
            "parse_excluded": self.parse_excluded,
            "dlq_excluded": self.dlq_excluded,
            "superseded": self.superseded,
            "accepted": self.accepted,
            "inserted": self.inserted,
            "updated": self.updated,
        }

    def as_metrics(self) -> dict[str, int]:
        """Terms of ``ops.pipeline_metrics`` (``superseded`` is the duplicates term; no re-reads)."""
        return {
            "rows_read": self.bronze_rows,
            "reread_rows": 0,
            "parse_excluded": self.parse_excluded,
            "dlq_excluded": self.dlq_excluded,
            "duplicates_removed": self.superseded,
            "silver_accepted": self.accepted,
        }


def check_equation(
    bronze_rows: int, parse_excluded: int, dlq_excluded: int, superseded: int, accepted: int
) -> None:
    """Raise :class:`ReconciliationError` unless every Bronze row has exactly one outcome."""
    total = parse_excluded + dlq_excluded + superseded + accepted
    if bronze_rows != total:
        raise ReconciliationError(
            f"silver_products: bronze_rows={bronze_rows} != parse_excluded={parse_excluded} + "
            f"dlq_excluded={dlq_excluded} + superseded={superseded} + accepted={accepted}"
        )


def silver_products_schema() -> StructType:
    """Schema of ``silver.products``; ``brand_id`` and ``product_id`` are NOT NULL."""
    from pyspark.sql.types import (
        BooleanType,
        DecimalType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    money = DecimalType(10, 2)
    return StructType(
        [
            StructField("brand_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType()),
            StructField("category", StringType()),
            StructField("retail_price", money),
            StructField("cost_price", money),
            StructField("is_active", BooleanType()),
            StructField("_bronze_ref", StringType()),
            StructField("_bronze_batch_id", StringType()),
            StructField("_updated_at", TimestampType()),
        ]
    )


def ensure_table(spark: SparkSession, path: str) -> None:
    """Create the empty Silver table (CHECK ``brand_id IS NOT NULL``) if it is missing."""
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        return
    if "`" in path:
        raise ValueError("path must not contain a backtick")
    spark.createDataFrame([], silver_products_schema()).write.format("delta").save(path)
    spark.sql(
        f"ALTER TABLE delta.`{path}` ADD CONSTRAINT {_CONSTRAINT} CHECK (brand_id IS NOT NULL)"
    )


def classify_products(bronze: DataFrame) -> DataFrame:
    """``bronze`` plus the string column ``_outcome`` (one of :data:`OUTCOMES`).

    Only valid rows compete for "latest": a DLQ row never supersedes a valid copy and is never
    superseded. The order is ``(_ingest_ts, landing file key, line number, _source_ref)``, all
    descending: ``_ingest_ts`` is constant per Bronze batch, so within one batch the later landing
    line wins numerically (``#L10`` beats ``#L9``); the full ``_source_ref`` is the last tie-break.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    checked = apply_rules(bronze, product_rules()).withColumn("_is_valid", is_valid())
    latest_first = Window.partitionBy(*KEY, "_is_valid").orderBy(
        F.col("_ingest_ts").desc_nulls_last(),
        *[position.desc_nulls_last() for position in _landing_position()],
    )
    outcome = (
        F.when(F.col("_parse_error").isNotNull(), "parse_excluded")
        .when(F.size("_rejection_codes") > 0, "dlq_excluded")
        .when(F.row_number().over(latest_first) > 1, "superseded")
        .otherwise("accepted")
    )
    return checked.withColumn("_outcome", outcome).drop(
        "_is_valid", "_rejection_codes", "_error_category", "_rejection_reason", "_retryable"
    )


def _typed(classified: DataFrame) -> DataFrame:
    """Silver columns (without ``_updated_at``) of the ``accepted`` rows of ``classified``."""
    from pyspark.sql import functions as F

    return classified.where(F.col("_outcome") == "accepted").select(
        F.col("brand_id"),
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("retail_price").cast("decimal(10,2)").alias("retail_price"),
        F.col("cost_price").cast("decimal(10,2)").alias("cost_price"),
        F.coalesce(F.col("is_active"), F.lit(True)).alias("is_active"),  # contract default
        F.col("_source_ref").alias("_bronze_ref"),
        F.col("_batch_id").alias("_bronze_batch_id"),
    )


def latest_products(bronze: DataFrame) -> DataFrame:
    """The accepted rows of ``bronze`` in the Silver schema, without ``_updated_at``."""
    return _typed(classify_products(bronze))


def _merge(spark: SparkSession, path: str, source: DataFrame) -> tuple[int, int]:
    """MERGE ``source`` into the table at ``path``; ``(inserted, updated)`` from the MERGE metrics."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    target = DeltaTable.forPath(spark, path)
    version_before = int(target.history(1).collect()[0]["version"])
    condition = " AND ".join(f"t.{name} = s.{name}" for name in KEY)
    (
        target.alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdateAll(condition="NOT (t._bronze_ref <=> s._bronze_ref)")
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


def run_products(spark: SparkSession, bronze: DataFrame, target: ResolvedTable) -> ProductCounts:
    """Classify ``bronze``, merge the accepted rows into ``target`` (created if absent), count."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    classified = classify_products(bronze).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        bronze_rows = classified.count()
        by_outcome = {
            row["_outcome"]: int(row["n"])
            for row in classified.groupBy("_outcome").agg(F.count(F.lit(1)).alias("n")).collect()
        }
        terms = {name: by_outcome.get(name, 0) for name in OUTCOMES}
        check_equation(bronze_rows, **terms)
        ensure_table(spark, target.location)
        inserted = updated = 0
        if terms["accepted"]:
            source = _typed(classified).withColumn("_updated_at", F.current_timestamp())
            inserted, updated = _merge(spark, target.location, source)
    finally:
        classified.unpersist()
    return ProductCounts(bronze_rows=bronze_rows, inserted=inserted, updated=updated, **terms)
