"""``ops.pipeline_metrics``: the wide reconciliation metrics table (W3.D18, D-081).

One row per Silver micro-batch (``scope='batch'``, ``brand_id`` NULL) written by the Silver jobs,
and one row per ``(run_id, source_table, brand_id)`` (``scope='reconcile'``) written by the
``reconcile`` job. Per batch, counted in Bronze events::

    bronze = silver_accepted + dlq + duplicates_removed + stale_cdc_noop + late_beyond_window

with ``bronze = rows_read - reread_rows - parse_excluded`` and ``dlq = dlq_excluded`` (+
``fk_orphans`` for orders). Writes are MERGEs, so a re-executed batch overwrites its row (last
attempt wins) and a repeated ``reconcile`` run does not add rows. The table is path-addressed
(``ops/[<ns>/]pipeline_metrics``, never registered in the catalog). PySpark is imported lazily.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lakehouse.common.logging import get_logger
from lakehouse.common.tables import PIPELINE_METRICS, resolve


if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

    from lakehouse.common.spark import SparkSettings

_log = get_logger(__name__)

SCOPE_BATCH = "batch"
SCOPE_RECONCILE = "reconcile"
TERMS = (
    "silver_accepted",
    "dlq",
    "duplicates_removed",
    "stale_cdc_noop",
    "late_beyond_window",
)
COUNT_FIELDS = ("rows_read", "reread_rows", "parse_errors", "bronze", *TERMS)
_STRING_FIELDS = ("job", "source_table", "query_id", "run_id", "scope", "brand_id")
FIELDS = (
    "job",
    "source_table",
    "query_id",
    "batch_id",
    "run_id",
    "scope",
    "brand_id",
    *COUNT_FIELDS,
    "balanced",
    "recorded_at",
)
_MERGE_CONDITION = {
    SCOPE_BATCH: (
        "t.scope = 'batch' AND t.job = s.job AND t.query_id = s.query_id "
        "AND t.batch_id = s.batch_id"
    ),
    SCOPE_RECONCILE: (
        "t.scope = 'reconcile' AND t.run_id = s.run_id AND t.source_table = s.source_table "
        "AND t.brand_id <=> s.brand_id"
    ),
}
_ATTEMPTS = 3
_QUERY_ID_WAIT_S = 120.0


def metrics_path(settings: SparkSettings, namespace: str | None) -> str:
    """Location of ``ops.pipeline_metrics`` for this run (``ops/[<ns>/]pipeline_metrics``)."""
    return resolve(settings, PIPELINE_METRICS, namespace).location


def metrics_schema() -> StructType:
    """Schema of the table; every column but the key parts is nullable."""
    from pyspark.sql.types import (
        BooleanType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    def field(name: str) -> StructField:
        if name in _STRING_FIELDS:
            return StructField(name, StringType())
        if name == "balanced":
            return StructField(name, BooleanType())
        if name == "recorded_at":
            return StructField(name, TimestampType())
        return StructField(name, LongType())  # batch_id and the counts

    return StructType([field(name) for name in FIELDS])


def is_balanced(row: Mapping[str, Any]) -> bool:
    """``bronze == sum of the terms`` (every term counted in Bronze events)."""
    return int(row["bronze"] or 0) == sum(int(row[t] or 0) for t in TERMS)


def batch_row(
    *,
    job: str,
    source_table: str,
    query_id: str,
    batch_id: int,
    run_id: str,
    counts: Mapping[str, int],
    recorded_at: datetime | None = None,
) -> dict[str, Any]:
    """Wide batch row from a Silver job's ``<Entity>BatchCounts.as_dict()``.

    ``bronze = rows_read - reread_rows - parse_excluded``; ``dlq = dlq_excluded + fk_orphans``
    (the latter exists for orders only). Terms a job does not have are 0.
    """
    rows_read = int(counts["rows_read"])
    reread = int(counts["reread_rows"])
    parse = int(counts["parse_excluded"])
    row: dict[str, Any] = {
        "job": job,
        "source_table": source_table,
        "query_id": query_id,
        "batch_id": int(batch_id),
        "run_id": run_id,
        "scope": SCOPE_BATCH,
        "brand_id": None,
        "rows_read": rows_read,
        "reread_rows": reread,
        "parse_errors": parse,
        "bronze": rows_read - reread - parse,
        "silver_accepted": int(counts.get("silver_accepted", 0)),
        "dlq": int(counts.get("dlq_excluded", 0)) + int(counts.get("fk_orphans", 0)),
        "duplicates_removed": int(counts.get("duplicates_removed", 0)),
        "stale_cdc_noop": int(counts.get("stale_cdc_noop", 0)),
        "late_beyond_window": int(counts.get("late_beyond_window", 0)),
        "recorded_at": recorded_at or datetime.now(UTC).replace(tzinfo=None),
    }
    row["balanced"] = is_balanced(row)
    return row


def merge_rows(
    spark: SparkSession, path: str, rows: Sequence[Mapping[str, Any]], scope: str
) -> None:
    """MERGE ``rows`` (all of ``scope``) into the metrics table at ``path`` (created on demand)."""
    from delta.tables import DeltaTable

    if scope not in _MERGE_CONDITION:
        raise ValueError(f"unknown scope {scope!r}")
    if not rows:
        return
    if any(r.get("scope") != scope for r in rows):
        raise ValueError(f"every row must have scope {scope!r}")
    schema = metrics_schema()
    source = spark.createDataFrame([tuple(r.get(name) for name in FIELDS) for r in rows], schema)
    for attempt in range(1, _ATTEMPTS + 1):
        try:
            if not DeltaTable.isDeltaTable(spark, path):
                spark.createDataFrame([], schema).write.format("delta").mode("append").save(path)
            (
                DeltaTable.forPath(spark, path)
                .alias("t")
                .merge(source.alias("s"), _MERGE_CONDITION[scope])
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            return
        except Exception as exc:
            # Concurrent writers (two Silver jobs creating/merging the table): retry, then raise.
            if "Concurrent" not in f"{type(exc).__name__} {exc}" or attempt == _ATTEMPTS:
                raise
            _log.warning("pipeline_metrics.retry", attempt=attempt)
            time.sleep(attempt)


def record_batch(spark: SparkSession, path: str, row: Mapping[str, Any]) -> None:
    """MERGE one batch row on ``(job, query_id, batch_id)``: a re-executed batch overwrites."""
    merge_rows(spark, path, [row], SCOPE_BATCH)


class BatchRecorder:
    """Records the batch rows of one Silver job run.

    The streaming query id is only known once ``start()`` returned, while the first micro-batch
    may already be running: :meth:`record` waits (bounded) until :meth:`bind_query` was called.
    """

    def __init__(
        self, spark: SparkSession, path: str, *, job: str, source_table: str, run_id: str
    ) -> None:
        self._spark = spark
        self._path = path
        self._job = job
        self._source_table = source_table
        self._run_id = run_id
        self._query_id: str | None = None
        self._ready = threading.Event()

    def bind_query(self, query_id: str) -> None:
        self._query_id = query_id
        self._ready.set()

    def record(self, batch_id: int, counts: Mapping[str, int]) -> None:
        if not self._ready.wait(_QUERY_ID_WAIT_S) or self._query_id is None:
            raise RuntimeError(f"{self._job}: streaming query id was never bound")
        row = batch_row(
            job=self._job,
            source_table=self._source_table,
            query_id=self._query_id,
            batch_id=batch_id,
            run_id=self._run_id,
            counts=counts,
        )
        record_batch(self._spark, self._path, row)
        _log.info(
            "pipeline_metrics.batch_recorded",
            job=self._job,
            batch_id=batch_id,
            balanced=row["balanced"],
        )
