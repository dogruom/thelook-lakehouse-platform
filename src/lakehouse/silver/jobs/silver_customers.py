"""Bronze ``raw_customers`` -> Silver ``customers``: one ``availableNow`` Delta-stream query.

Run as ``python scripts/submit_job.py silver_customers [--namespace <segment>]
[--max-files-per-trigger N]``. The job streams the Bronze Delta table (checkpoint
``checkpoints/silver_customers/[<ns>/]customers``), and for every micro-batch classifies the rows
and applies them with a sequence-guarded MERGE (see ``lakehouse.silver.customers``). It stops when no
new Bronze data is left. ``--namespace <ns>`` reads ``bronze/<ns>`` and writes ``silver/<ns>``; such
runs are never registered in the catalog (D-060).

The HMAC key (``PII_HMAC_KEY``) is read from the driver environment once, before the query starts;
it never enters Spark conf, arguments or logs. Logs carry counts only.

Re-running a batch (or the whole job after a checkpoint loss) leaves the Silver state unchanged: the
MERGE applies a row only when its ``_cdc_sequence`` is higher than the stored one. The replay's
counters differ (its rows count as ``stale_cdc_noop`` / ``duplicates_removed``), which is expected;
the metrics count rows *seen* in micro-batches. This is an idempotent outcome of at-least-once
ingestion, not an exactly-once claim. Single writer per Silver table is assumed.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.pii import load_pii_key
from lakehouse.common.tables import (
    CATALOG_TABLES,
    delta_exists_at_location,
    register_table,
    resolve,
)
from lakehouse.ops.metrics import BatchRecorder, metrics_path
from lakehouse.silver.customers import CustomerBatchCounts, ensure_table, process_batch


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

    from lakehouse.common.jobs import JobContext
    from lakehouse.common.spark import SparkSettings

_log = get_logger(__name__)
QUERY_NAME = "silver_customers"
BRONZE_TABLE = "raw_customers"
SILVER_TABLE = "customers"


class CatalogRegistrationError(RuntimeError):
    """Silver data is committed, but the catalog registration afterwards failed."""


def _positive_int(value: str) -> int:
    import argparse as _argparse

    try:
        number = int(value)
    except ValueError:
        raise _argparse.ArgumentTypeError(f"{value!r} is not an integer") from None
    if number < 1:
        raise _argparse.ArgumentTypeError(f"must be >= 1, got {number}")
    return number


class _Counters:
    """Totals over the micro-batches of one run, accumulated on the driver."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.batches = 0
        self.totals = CustomerBatchCounts(0, 0, 0, 0, 0, 0, 0)
        self.recorder: BatchRecorder | None = None

    def add(self, counts: CustomerBatchCounts) -> None:
        with self._lock:
            self.batches += 1
            old = self.totals.as_dict()
            new = counts.as_dict()
            self.totals = CustomerBatchCounts(**{k: old[k] + new[k] for k in old})

    def record(self, batch_id: int, counts: Mapping[str, int]) -> None:
        """Write the batch's ``ops.pipeline_metrics`` row (no-op without a recorder)."""
        if self.recorder is not None:
            self.recorder.record(batch_id, counts)


class SilverCustomersJob(SparkJob):
    """Apply the Bronze customer CDC to ``silver.customers``."""

    name = "silver_customers"

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

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        namespace: str | None = args.namespace
        parts = [namespace] if namespace else []
        key = load_pii_key()  # fail before anything is written; the key stays in this process
        bronze = resolve(ctx.settings, CATALOG_TABLES[BRONZE_TABLE], namespace)
        silver = resolve(ctx.settings, CATALOG_TABLES[SILVER_TABLE], namespace)
        if not delta_exists_at_location(spark, bronze):
            raise RuntimeError(f"no Bronze table at {bronze.location}; bronze_stream never ran")
        checkpoint = ctx.settings.layer_uri("checkpoints", QUERY_NAME, *parts, SILVER_TABLE)
        ensure_table(spark, silver.location)

        counters = _Counters()
        counters.recorder = BatchRecorder(
            spark,
            metrics_path(ctx.settings, namespace),
            job=QUERY_NAME,
            source_table=BRONZE_TABLE,
            run_id=ctx.run_id,
        )
        query = start_query(
            spark,
            bronze.location,
            silver.location,
            checkpoint,
            args.max_files_per_trigger,
            key,
            counters,
        )
        counters.recorder.bind_query(str(query.id))
        try:
            query.awaitTermination()
            error = query.exception()
            if error is not None:
                raise RuntimeError(f"silver_customers query failed: {error}")
        finally:
            if query.isActive:
                query.stop()

        totals = counters.totals.as_dict()
        _log.info("silver_customers.query_done", batches=counters.batches, **totals)
        # After the counters are logged: a registration failure must not hide the evidence.
        catalog = register_silver_table(spark, ctx.settings, namespace)
        return {
            "batches": counters.batches,
            "counts": totals,
            "query_id": str(query.id),
            "path": silver.location,
            "checkpoint": checkpoint,
            **({"catalog": catalog} if catalog else {}),
        }


def register_silver_table(
    spark: SparkSession, settings: SparkSettings, namespace: str | None
) -> dict[str, str]:
    """Register ``silver.customers`` after a successful run (writes stay path-based, D-060).

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
    key: bytes,
    counters: _Counters,
) -> StreamingQuery:
    """Start the ``availableNow`` query that drains the Bronze customers table into Silver."""

    def on_batch(batch_df: DataFrame, batch_id: int) -> None:
        _log.info("silver_customers.batch_started", batch_id=batch_id)
        bronze = spark.read.format("delta").load(bronze_path)
        counts = process_batch(spark, batch_df, bronze, silver_path, key, datetime.now(UTC))
        counters.record(batch_id, counts.as_dict())
        counters.add(counts)
        _log.info("silver_customers.batch", batch_id=batch_id, **counts.as_dict())

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
    _log.info("silver_customers.query_started", query_id=str(query.id))
    return query


JOB = SilverCustomersJob
