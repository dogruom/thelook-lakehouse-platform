"""Dead-letter table writes shared by the DLQ gate, the Silver jobs and the replay (D-077).

``dlq_rows`` projects rows with a non-empty ``_rejection_codes`` onto the DLQ schema;
``merge_dlq`` appends them with an insert-only MERGE keyed on ``_dlq_id`` and ``brand_id``.
``_dlq_id`` = sha256 of ``coalesce(brand_id,'') | source_table | _source_ref``, so one Bronze
message or line maps to one DLQ id however many times it is re-read. DLQ rows carry codes only,
never payload values (PII). ``brand_id`` is the Bronze ``brand_id``: the payload brand for Kafka
tables, the landing-path brand for file tables.

``set_replay_outcome`` is the only writer that updates a DLQ row: it moves ``_replay_status`` and
bumps ``_replay_count`` (the replay job).

PySpark and Delta are imported lazily so this module imports on hosts without them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

PENDING = "PENDING"
REPLAYED = "REPLAYED"
FAILED_AGAIN = "FAILED_AGAIN"
DISCARDED = "DISCARDED"

DLQ_TABLE_BY_BRONZE = {
    "raw_customers": "dlq_customers",
    "raw_clickstream": "dlq_clickstream",
    "raw_orders": "dlq_orders",
    "raw_products": "dlq_products",
}

_KEY = "t._dlq_id = s._dlq_id AND t.brand_id <=> s.brand_id"


def dlq_rows(checked: DataFrame, source_table: str, run_id: str) -> DataFrame:
    """Project rejected rows (non-empty ``_rejection_codes``) onto the DLQ schema.

    ``event_id`` is NULL when the source has no such column (file tables).
    """
    from pyspark.sql import functions as F

    dlq_id = F.sha2(
        F.concat(
            F.coalesce(F.col("brand_id"), F.lit("")),
            F.lit("|"),
            F.lit(source_table),
            F.lit("|"),
            F.col("_source_ref"),
        ),
        256,
    )
    event_id = (
        F.col("event_id").cast("string")
        if "event_id" in checked.columns
        else F.lit(None).cast("string")
    )
    return checked.where(F.size("_rejection_codes") > 0).select(
        dlq_id.alias("_dlq_id"),
        F.col("brand_id").cast("string").alias("brand_id"),
        F.lit(source_table).alias("_source_table"),
        F.col("_source_ref").alias("_bronze_ref"),
        event_id.alias("event_id"),
        F.col("_error_category"),
        F.col("_rejection_reason"),
        F.col("_rejection_codes"),
        F.col("_retryable"),
        F.lit(PENDING).alias("_replay_status"),
        F.lit(0).alias("_replay_count"),
        F.current_timestamp().alias("_rejected_at"),
        F.lit(run_id).alias("_pipeline_run_id"),
    )


def _merge_metric(spark: SparkSession, path: str, version_before: int, name: str) -> int:
    """``operationMetrics[name]`` of the MERGE at version ``before + 1`` (0 if none committed)."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    rows = (
        DeltaTable.forPath(spark, path)
        .history()
        .where(F.col("version") == version_before + 1)
        .select("operation", "operationMetrics")
        .collect()
    )
    if not rows or rows[0]["operation"] != "MERGE":
        return 0
    return int((rows[0]["operationMetrics"] or {}).get(name, 0))


def merge_dlq(spark: SparkSession, path: str, rows: DataFrame) -> int:
    """Insert-only MERGE of ``rows`` (``dlq_rows`` shape) into the DLQ table at ``path``.

    Returns the rows actually inserted, read from the history row of version ``before + 1`` (not
    "the latest row"), so a MERGE that committed nothing reports 0. A missing table is created empty
    from the schema of ``rows`` first. ``rows`` must be unique on ``_dlq_id`` and ``brand_id``.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        rows.limit(0).write.format("delta").mode("append").save(path)
    target = DeltaTable.forPath(spark, path)
    version_before = int(target.history(1).collect()[0]["version"])
    target.alias("t").merge(rows.alias("s"), _KEY).whenNotMatchedInsertAll().execute()
    return _merge_metric(spark, path, version_before, "numTargetRowsInserted")


def set_replay_outcome(spark: SparkSession, path: str, outcomes: DataFrame) -> int:
    """Apply replay outcomes (``_dlq_id``, ``brand_id``, ``_replay_status``) to the DLQ table.

    A matched row that is not yet ``REPLAYED`` gets the new status and ``_replay_count + 1``.
    Returns the rows updated (Delta's own metric). ``outcomes`` must be unique on the key.
    """
    from delta.tables import DeltaTable

    target = DeltaTable.forPath(spark, path)
    version_before = int(target.history(1).collect()[0]["version"])
    (
        target.alias("t")
        .merge(outcomes.alias("s"), _KEY)
        .whenMatchedUpdate(
            condition=f"t._replay_status <> '{REPLAYED}'",
            set={"_replay_status": "s._replay_status", "_replay_count": "t._replay_count + 1"},
        )
        .execute()
    )
    return _merge_metric(spark, path, version_before, "numTargetRowsUpdated")
