"""Bronze ``raw_clickstream`` -> Silver ``clickstream``: one ``availableNow`` Delta-stream query.

Run as ``python scripts/submit_job.py silver_clickstream [--namespace <segment>]
[--max-files-per-trigger N] [--lookback-days D]``. The job streams the Bronze Delta table (checkpoint
``checkpoints/silver_clickstream/[<ns>/]clickstream``). For every micro-batch it drops parse and
contract failures (``dlq_gate`` owns the DLQ rows), counts events older than the lookback window
(default 7 ``event_date`` days behind the newest event time) as ``late_beyond_window`` and dedups the
rest on ``(brand_id, event_id)``: inside the batch (lowest Kafka partition/offset wins) and against the
window of the target by an insert-only MERGE (see ``lakehouse.silver.clickstream``). ``--namespace
<ns>`` reads ``bronze/<ns>`` and writes ``silver/<ns>``; such runs are never registered in the catalog
(D-060).

Re-running a batch (or the whole job after a checkpoint loss) leaves the Silver state unchanged: its
rows count as ``duplicates_removed`` (or ``late_beyond_window`` once the window moved on). This is an
idempotent outcome of at-least-once ingestion, not an exactly-once claim. Single writer per Silver
table is assumed. Logs carry counts only.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import (
    CATALOG_TABLES,
    delta_exists_at_location,
    register_table,
    resolve,
)
from lakehouse.silver.clickstream import (
    DEFAULT_LOOKBACK_DAYS,
    ClickstreamBatchCounts,
    ensure_table,
    process_clickstream_batch,
)
from lakehouse.silver.jobs.silver_customers import CatalogRegistrationError, _positive_int


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

    from lakehouse.common.jobs import JobContext
    from lakehouse.common.spark import SparkSettings

_log = get_logger(__name__)
QUERY_NAME = "silver_clickstream"
BRONZE_TABLE = "raw_clickstream"
SILVER_TABLE = "clickstream"


def _non_negative_int(value: str) -> int:
    import argparse as _argparse

    try:
        number = int(value)
    except ValueError:
        raise _argparse.ArgumentTypeError(f"{value!r} is not an integer") from None
    if number < 0:
        raise _argparse.ArgumentTypeError(f"must be >= 0, got {number}")
    return number


class _Counters:
    """Totals over the micro-batches of one run, accumulated on the driver."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.batches = 0
        self.totals = ClickstreamBatchCounts(0, 0, 0, 0, 0, 0, 0)

    def add(self, counts: ClickstreamBatchCounts) -> None:
        with self._lock:
            self.batches += 1
            old = self.totals.as_dict()
            new = counts.as_dict()
            self.totals = ClickstreamBatchCounts(**{k: old[k] + new[k] for k in old})


class SilverClickstreamJob(SparkJob):
    """Dedup the Bronze clickstream into ``silver.clickstream``."""

    name = "silver_clickstream"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--namespace", default=None, help="optional single path segment isolating this run"
        )
        parser.add_argument(
            "--max-files-per-trigger",
            type=_positive_int,
            default=100,
            help="upper bound of Bronze files per micro-batch (>= 1)",
        )
        parser.add_argument(
            "--lookback-days",
            type=_non_negative_int,
            default=DEFAULT_LOOKBACK_DAYS,
            help="event_date days behind the newest event time that still count (>= 0)",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        namespace: str | None = args.namespace
        parts = [namespace] if namespace else []
        bronze = resolve(ctx.settings, CATALOG_TABLES[BRONZE_TABLE], namespace)
        silver = resolve(ctx.settings, CATALOG_TABLES[SILVER_TABLE], namespace)
        if not delta_exists_at_location(spark, bronze):
            raise RuntimeError(f"no Bronze table at {bronze.location}; bronze_stream never ran")
        checkpoint = ctx.settings.layer_uri("checkpoints", QUERY_NAME, *parts, SILVER_TABLE)
        ensure_table(spark, silver.location)

        counters = _Counters()
        query = start_query(
            spark,
            bronze.location,
            silver.location,
            checkpoint,
            args.max_files_per_trigger,
            args.lookback_days,
            counters,
        )
        try:
            query.awaitTermination()
            error = query.exception()
            if error is not None:
                raise RuntimeError(f"silver_clickstream query failed: {error}")
        finally:
            if query.isActive:
                query.stop()

        totals = counters.totals.as_dict()
        _log.info(
            "silver_clickstream.query_done",
            batches=counters.batches,
            lookback_days=args.lookback_days,
            **totals,
        )
        # After the counters are logged: a registration failure must not hide the evidence.
        catalog = register_silver_table(spark, ctx.settings, namespace)
        return {
            "batches": counters.batches,
            "counts": totals,
            "lookback_days": args.lookback_days,
            "query_id": str(query.id),
            "path": silver.location,
            "checkpoint": checkpoint,
            **({"catalog": catalog} if catalog else {}),
        }


def register_silver_table(
    spark: SparkSession, settings: SparkSettings, namespace: str | None
) -> dict[str, str]:
    """Register ``silver.clickstream`` after a successful run (writes stay path-based, D-060).

    Namespaced runs and the in-memory catalog resolve to no name: skipped. A failure is re-raised
    as :class:`CatalogRegistrationError`; a rerun is safe (registration is idempotent).
    """
    spec = CATALOG_TABLES[SILVER_TABLE]
    resolved = resolve(settings, spec, namespace)
    if resolved.name is None:
        return {}
    try:
        if delta_exists_at_location(spark, resolved):
            return {SILVER_TABLE: register_table(spark, spec, resolved)}
    except Exception as exc:
        raise CatalogRegistrationError(
            f"Silver data is committed; catalog registration of {resolved.name} failed: {exc}"
        ) from exc
    return {}


def start_query(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    checkpoint: str,
    max_files: int,
    lookback_days: int,
    counters: _Counters,
) -> StreamingQuery:
    """Start the ``availableNow`` query that drains the Bronze clickstream table into Silver."""

    def on_batch(batch_df: DataFrame, batch_id: int) -> None:
        _log.info("silver_clickstream.batch_started", batch_id=batch_id)
        counts = process_clickstream_batch(
            spark, batch_df, silver_path, lookback_days, datetime.now(UTC)
        )
        counters.add(counts)
        _log.info("silver_clickstream.batch", batch_id=batch_id, **counts.as_dict())

    source = (
        spark.readStream.format("delta").option("maxFilesPerTrigger", max_files).load(bronze_path)
    )
    query = (
        source.writeStream.queryName(QUERY_NAME)
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint)
        .foreachBatch(on_batch)
        .start()
    )
    _log.info("silver_clickstream.query_started", query_id=str(query.id))
    return query


JOB = SilverClickstreamJob
