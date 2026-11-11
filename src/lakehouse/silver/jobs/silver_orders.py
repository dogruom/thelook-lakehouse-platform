"""Bronze ``raw_orders`` -> Silver ``orders`` and ``order_items``: one ``availableNow`` query.

Run as ``python scripts/submit_job.py silver_orders [--namespace <segment>]
[--max-files-per-trigger N]``. The job streams the Bronze Delta table (checkpoint
``checkpoints/silver_orders/[<ns>/]orders``). For every micro-batch it classifies the rows, inserts
new ``(brand_id, order_id)`` keys and their items insert-only, and writes foreign-key orphans to
``dead_letter.dlq_orders`` (``_retryable = true``; the ``replay`` job recovers them once the parent
exists). ``--namespace <ns>`` reads and writes the ``<ns>`` paths of every table; such runs are never
registered in the catalog (D-060).

Prerequisites: ``silver.customers`` (``silver_customers`` ran) and Bronze ``raw_products``
(``bronze_batch`` ingested the product files). Both are read in full at every micro-batch; a missing
table fails the job before anything is written, because every order would otherwise look like an
orphan. Contract failures are not written here: ``dlq_gate`` owns them (this job only skips them).

Re-running a batch (or the whole job after a checkpoint loss) leaves the Silver state unchanged: an
existing key is never rewritten. The replay's counters differ (its rows count as
``duplicates_removed``), which is expected; this is an idempotent outcome of at-least-once ingestion,
not an exactly-once claim. Single writer per Silver table is assumed. Logs carry counts only.
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
from lakehouse.silver.jobs.silver_customers import CatalogRegistrationError, _positive_int
from lakehouse.silver.orders import OrderBatchCounts, ensure_tables, process_orders_batch


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

    from lakehouse.common.jobs import JobContext
    from lakehouse.common.spark import SparkSettings

_log = get_logger(__name__)
QUERY_NAME = "silver_orders"
BRONZE_TABLE = "raw_orders"
CUSTOMERS_TABLE = "customers"
PRODUCTS_TABLE = "raw_products"
ORDERS_TABLE = "orders"
ITEMS_TABLE = "order_items"
DLQ_TABLE = "dlq_orders"
REGISTERED_TABLES = (ORDERS_TABLE, ITEMS_TABLE, DLQ_TABLE)


class _Counters:
    """Totals over the micro-batches of one run, accumulated on the driver."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.batches = 0
        self.totals = OrderBatchCounts(0, 0, 0, 0, 0, 0, 0, 0)

    def add(self, counts: OrderBatchCounts) -> None:
        with self._lock:
            self.batches += 1
            old = self.totals.as_dict()
            new = counts.as_dict()
            self.totals = OrderBatchCounts(**{k: old[k] + new[k] for k in old})


class SilverOrdersJob(SparkJob):
    """Insert the Bronze orders and their items into Silver; route orphans to the DLQ."""

    name = "silver_orders"

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
        tables = {
            name: resolve(ctx.settings, CATALOG_TABLES[name], namespace)
            for name in (
                BRONZE_TABLE,
                CUSTOMERS_TABLE,
                PRODUCTS_TABLE,
                ORDERS_TABLE,
                ITEMS_TABLE,
                DLQ_TABLE,
            )
        }
        for name in (BRONZE_TABLE, CUSTOMERS_TABLE, PRODUCTS_TABLE):  # fail before writing
            if not delta_exists_at_location(spark, tables[name]):
                raise RuntimeError(
                    f"no table {name} at {tables[name].location}; its producing job never ran"
                )
        checkpoint = ctx.settings.layer_uri("checkpoints", QUERY_NAME, *parts, ORDERS_TABLE)
        ensure_tables(spark, tables[ORDERS_TABLE].location, tables[ITEMS_TABLE].location)

        counters = _Counters()
        query = start_query(
            spark,
            {name: table.location for name, table in tables.items()},
            checkpoint,
            args.max_files_per_trigger,
            ctx.run_id,
            counters,
        )
        try:
            query.awaitTermination()
            error = query.exception()
            if error is not None:
                raise RuntimeError(f"silver_orders query failed: {error}")
        finally:
            if query.isActive:
                query.stop()

        totals = counters.totals.as_dict()
        _log.info("silver_orders.query_done", batches=counters.batches, **totals)
        # After the counters are logged: a registration failure must not hide the evidence.
        catalog = register_silver_tables(spark, ctx.settings, namespace)
        return {
            "batches": counters.batches,
            "counts": totals,
            "query_id": str(query.id),
            "path": tables[ORDERS_TABLE].location,
            "items_path": tables[ITEMS_TABLE].location,
            "dlq_path": tables[DLQ_TABLE].location,
            "checkpoint": checkpoint,
            **({"catalog": catalog} if catalog else {}),
        }


def register_silver_tables(
    spark: SparkSession, settings: SparkSettings, namespace: str | None
) -> dict[str, str]:
    """Register ``silver.orders``, ``silver.order_items`` and ``dead_letter.dlq_orders`` (D-060).

    Namespaced runs and the in-memory catalog resolve to no name: skipped. A failure is re-raised
    as :class:`CatalogRegistrationError`; a rerun is safe (registration is idempotent).
    """
    registered: dict[str, str] = {}
    for name in REGISTERED_TABLES:
        spec = CATALOG_TABLES[name]
        resolved = resolve(settings, spec, namespace)
        if resolved.name is None:
            continue
        try:
            if delta_exists_at_location(spark, resolved):
                registered[name] = register_table(spark, spec, resolved)
        except Exception as exc:
            raise CatalogRegistrationError(
                f"Silver data is committed; catalog registration of {resolved.name} failed: {exc}"
            ) from exc
    return registered


def start_query(
    spark: SparkSession,
    paths: Mapping[str, str],
    checkpoint: str,
    max_files: int,
    run_id: str,
    counters: _Counters,
) -> StreamingQuery:
    """Start the ``availableNow`` query that drains the Bronze orders table into Silver."""

    def read(name: str) -> DataFrame:
        return spark.read.format("delta").load(paths[name])

    def on_batch(batch_df: DataFrame, batch_id: int) -> None:
        _log.info("silver_orders.batch_started", batch_id=batch_id)
        counts = process_orders_batch(
            spark,
            batch_df,
            read(BRONZE_TABLE),
            read(CUSTOMERS_TABLE),
            read(PRODUCTS_TABLE),
            paths[ORDERS_TABLE],
            paths[ITEMS_TABLE],
            paths[DLQ_TABLE],
            run_id,
            datetime.now(UTC),
        )
        counters.add(counts)
        _log.info("silver_orders.batch", batch_id=batch_id, **counts.as_dict())

    source = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", max_files)
        .load(paths[BRONZE_TABLE])
    )
    query = (
        source.writeStream.queryName(QUERY_NAME)
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint)
        .foreachBatch(on_batch)
        .start()
    )
    _log.info("silver_orders.query_started", query_id=str(query.id))
    return query


JOB = SilverOrdersJob
