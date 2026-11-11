"""Bronze ``raw_customers`` -> Silver ``customers_history`` (SCD2): one ``availableNow`` query.

Run as ``python scripts/submit_job.py silver_customers_history [--namespace <segment>]
[--max-files-per-trigger N]``. The job streams the Bronze Delta table (checkpoint
``checkpoints/silver_customers_history/[<ns>/]customers_history``, never shared with
``silver_customers``) and, for every micro-batch, rebuilds the complete history of the customers
the batch touches from Bronze and replaces their stored versions in one Delta MERGE (see
``lakehouse.silver.customers_history``). ``--namespace <ns>`` reads ``bronze/<ns>`` and writes
``silver/<ns>``; such runs are never registered in the catalog (D-060).

The HMAC key (``PII_HMAC_KEY``) is read from the driver environment once, before the query starts;
it never enters Spark conf, arguments or logs. Logs carry counts only.

Replaying a batch, or the whole job after a checkpoint loss, leaves the history unchanged: the
rebuild depends only on the Bronze content and unchanged versions are not rewritten. This is an
idempotent outcome, not an exactly-once claim. Single writer per Silver table is assumed.
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
from lakehouse.silver.customers_history import (
    HistoryBatchCounts,
    ensure_history_table,
    process_history_batch,
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
QUERY_NAME = "silver_customers_history"
BRONZE_TABLE = "raw_customers"
SILVER_TABLE = "customers_history"


class _Counters:
    """Totals over the micro-batches of one run, accumulated on the driver."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.batches = 0
        self.totals = HistoryBatchCounts(0, 0, 0, 0)

    def add(self, counts: HistoryBatchCounts) -> None:
        with self._lock:
            self.batches += 1
            old = self.totals.as_dict()
            new = counts.as_dict()
            self.totals = HistoryBatchCounts(**{k: old[k] + new[k] for k in old})


class SilverCustomersHistoryJob(SparkJob):
    """Rebuild the SCD2 history of the customers touched by the Bronze CDC stream."""

    name = "silver_customers_history"

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
        ensure_history_table(spark, silver.location)

        counters = _Counters()
        query = start_query(
            spark,
            bronze.location,
            silver.location,
            checkpoint,
            args.max_files_per_trigger,
            key,
            counters,
        )
        try:
            query.awaitTermination()
            error = query.exception()
            if error is not None:
                raise RuntimeError(f"silver_customers_history query failed: {error}")
        finally:
            if query.isActive:
                query.stop()

        totals = counters.totals.as_dict()
        _log.info("silver_customers_history.query_done", batches=counters.batches, **totals)
        # After the counters are logged: a registration failure must not hide the evidence.
        catalog = register_history_table(spark, ctx.settings, namespace)
        return {
            "batches": counters.batches,
            "counts": totals,
            "query_id": str(query.id),
            "path": silver.location,
            "checkpoint": checkpoint,
            **({"catalog": catalog} if catalog else {}),
        }


def register_history_table(
    spark: SparkSession, settings: SparkSettings, namespace: str | None
) -> dict[str, str]:
    """Register ``silver.customers_history`` after a successful run (writes stay path-based).

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
    """Start the ``availableNow`` query that drains the Bronze customers table into history."""

    def on_batch(batch_df: DataFrame, batch_id: int) -> None:
        _log.info("silver_customers_history.batch_started", batch_id=batch_id)
        bronze = spark.read.format("delta").load(bronze_path)
        counts = process_history_batch(spark, batch_df, bronze, silver_path, key, datetime.now(UTC))
        counters.add(counts)
        _log.info("silver_customers_history.batch", batch_id=batch_id, **counts.as_dict())

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
    _log.info("silver_customers_history.query_started", query_id=str(query.id))
    return query


JOB = SilverCustomersHistoryJob
