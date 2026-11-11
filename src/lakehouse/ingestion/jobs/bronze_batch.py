"""Landing files -> Bronze Delta: one ``availableNow`` file-source query per entity.

Run as ``python scripts/submit_job.py bronze_batch [--namespace <segment>]``. The job reads the new
``*.ndjson`` files under ``landing/*/orders/*`` and ``landing/*/products/*`` (manifests are not
ingested), writes every line as one Bronze row (see ``lakehouse.ingestion.batch``) and stops when
no new file is left. ``--namespace <ns>`` isolates the whole run: it reads ``landing/<ns>/...`` and
writes ``bronze/<ns>/...`` with its own checkpoint (D-069). Spark owns the set of processed files in the checkpoint
(``checkpoints/bronze_batch/<table>``), so a rerun without new files writes nothing.

Idempotence follows ``bronze_stream``: every micro-batch is written with Delta's ``txnAppId`` (the
streaming query id, stored in the checkpoint) and ``txnVersion`` (the batch id); a skipped write
must be a true replay of the stored batch, otherwise ``CheckpointRewoundError`` fails the query.
This is not an "exactly-once" claim. A file already processed is not read again unless the
checkpoint is lost; a lost checkpoint re-ingests the landing files and appends duplicates, which
Silver dedup counts (same rule as the Kafka path, ADR-0006).

``sales_channel`` is not an evolution event of this job: the reader projects the contract schema
(``order_event.v1``), which already contains the column, so it exists from batch 1 and is NULL
until batch 10. Unknown JSON fields are dropped from the columns (kept only in ``_raw_value``).
``mergeSchema`` matters when a new contract version adds a column to an existing table.

Metrics count lines *seen* in micro-batches, not rows written. Tables are registered by name after
a successful run (D-060); namespaced runs are never registered.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import (
    CATALOG_TABLES,
    delta_exists_at_location,
    register_table,
    resolve,
)
from lakehouse.ingestion.batch import (
    FILE_PATH_COLUMN,
    ORDERS,
    PRODUCTS,
    BatchTarget,
    BrandCounts,
    FileBatchCounts,
    write_bronze_files,
)
from lakehouse.ingestion.jobs.bronze_stream import (
    CatalogRegistrationError,
    _await_all,
    _find_query_id,
    _positive_int,
    _stop_all,
)


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

    from lakehouse.common.jobs import JobContext
    from lakehouse.common.spark import SparkSettings

_log = get_logger(__name__)
TARGETS = (ORDERS, PRODUCTS)


def query_name(target: BatchTarget) -> str:
    """Name of the streaming query of ``target`` (also how the callback finds its query id)."""
    return f"bronze_batch_{target.table}"


def source_glob(landing_root: str, target: BatchTarget) -> str:
    """Glob of the landing files of ``target`` across brands and dates."""
    return f"{landing_root.rstrip('/')}/*/{target.entity}/*/*"


class _Counters:
    """Per-query counters, accumulated on the driver from the foreachBatch callback."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.files = 0
        self.lines = 0
        self.parse_errors = 0
        self.batches = 0
        self.per_brand: dict[str, BrandCounts] = {}

    def add(self, counts: FileBatchCounts) -> None:
        with self._lock:
            self.files += counts.files
            self.lines += counts.lines
            self.parse_errors += counts.parse_errors
            self.batches += 1
            for brand, c in counts.per_brand.items():
                old = self.per_brand.get(brand, BrandCounts(0, 0, 0))
                self.per_brand[brand] = BrandCounts(
                    old.files + c.files, old.lines + c.lines, old.parse_errors + c.parse_errors
                )

    def brands(self) -> dict[str, dict[str, int]]:
        return {
            b: {"files": c.files, "lines": c.lines, "parse_errors": c.parse_errors}
            for b, c in sorted(self.per_brand.items())
        }


class BronzeBatchJob(SparkJob):
    """Ingest the landed ``orders`` and ``products`` files into the Bronze Delta tables."""

    name = "bronze_batch"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--namespace", default=None, help="optional single path segment isolating this run"
        )
        parser.add_argument(
            "--max-files-per-trigger",
            type=_positive_int,
            default=100,
            help="upper bound of files per micro-batch (>= 1)",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        namespace: str | None = args.namespace
        parts = [namespace] if namespace else []
        landing_root = ctx.settings.layer_uri("landing", *parts)
        counters: dict[str, _Counters] = {t.table: _Counters() for t in TARGETS}
        info: dict[str, dict[str, str]] = {}
        queries: dict[str, StreamingQuery] = {}
        try:
            for target in TARGETS:
                path = ctx.settings.layer_uri("bronze", *parts, target.table)
                checkpoint = ctx.settings.layer_uri(
                    "checkpoints", "bronze_batch", *parts, target.table
                )
                info[target.table] = {"path": path, "checkpoint": checkpoint}
                queries[target.table] = start_query(
                    spark,
                    source_glob(landing_root, target),
                    target,
                    path,
                    checkpoint,
                    args.max_files_per_trigger,
                    counters[target.table],
                )
            _await_all(queries)
        finally:
            _stop_all(queries)

        result: dict[str, Any] = {}
        for table, query in queries.items():
            c = counters[table]
            result[table] = {
                "files": c.files,
                "lines": c.lines,
                "parse_errors": c.parse_errors,
                "batches": c.batches,
                "per_brand": c.brands(),
                "query_id": str(query.id),
                **info[table],
            }
            _log.info(
                "bronze_batch.query_done",
                table=table,
                files=c.files,
                lines=c.lines,
                parse_errors=c.parse_errors,
                batches=c.batches,
                per_brand=c.brands(),
            )
        # After the counters are logged: a registration failure must not hide the ingest evidence.
        catalog = register_batch_tables(spark, ctx.settings, TARGETS, namespace)
        return {"queries": result, **({"catalog": catalog} if catalog else {})}


def register_batch_tables(
    spark: SparkSession,
    settings: SparkSettings,
    targets: tuple[BatchTarget, ...],
    namespace: str | None,
) -> dict[str, str]:
    """Register every existing Bronze table of the run (writes stay path-based, D-060).

    Namespaced runs and the in-memory catalog resolve to no name: skipped. Any failure is re-raised
    as :class:`CatalogRegistrationError`; a rerun is safe (registration is idempotent).
    """
    catalog: dict[str, str] = {}
    for target in targets:
        spec = CATALOG_TABLES[target.table]
        resolved = resolve(settings, spec, namespace)
        if resolved.name is None:
            continue
        try:
            if delta_exists_at_location(spark, resolved):
                catalog[target.table] = register_table(spark, spec, resolved)
        except Exception as exc:
            raise CatalogRegistrationError(
                f"Bronze ingest committed; catalog registration of {resolved.name} failed: {exc}"
            ) from exc
    return catalog


def file_stream(spark: SparkSession, glob: str, max_files: int) -> DataFrame:
    """Streaming frame of whole ``*.ndjson`` files: ``_file_path`` and the file text ``value``."""
    from pyspark.sql import functions as F

    return (
        spark.readStream.format("text")
        .option("wholetext", "true")
        .option("pathGlobFilter", "*.ndjson")
        .option("maxFilesPerTrigger", max_files)
        .load(glob)
        .select(F.col("_metadata.file_path").alias(FILE_PATH_COLUMN), F.col("value"))
    )


def start_query(
    spark: SparkSession,
    glob: str,
    target: BatchTarget,
    path: str,
    checkpoint: str,
    max_files: int,
    counters: _Counters,
) -> StreamingQuery:
    """Start the ``availableNow`` query that drains the landing files of ``target``."""
    name = query_name(target)

    def on_batch(batch_df: DataFrame, batch_id: int) -> None:
        # Looked up through the outer session by name: the id is the one stored in the checkpoint.
        query_id = _find_query_id(spark, name)
        _log.info("bronze_batch.batch_started", table=target.table, batch_id=batch_id)
        counts = write_bronze_files(
            batch_df, path, target=target, app_id=query_id, batch_id=batch_id
        )
        counters.add(counts)
        _log.info(
            "bronze_batch.batch",
            table=target.table,
            batch_id=batch_id,
            files=counts.files,
            lines=counts.lines,
            parse_errors=counts.parse_errors,
            per_brand={
                b: {"files": c.files, "lines": c.lines, "parse_errors": c.parse_errors}
                for b, c in sorted(counts.per_brand.items())
            },
        )

    query = (
        file_stream(spark, glob, max_files)
        .writeStream.queryName(name)
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint)
        .foreachBatch(on_batch)
        .start()
    )
    _log.info("bronze_batch.query_started", table=target.table, query_id=str(query.id))
    return query


JOB = BronzeBatchJob
