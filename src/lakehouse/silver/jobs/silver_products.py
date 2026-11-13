"""Bronze ``raw_products`` -> Silver ``products``: one batch run (D-091).

Run as ``python scripts/submit_job.py silver_products [--namespace <segment>]``. The job reads the
whole Bronze table (the catalog is small and a re-landed catalog supersedes older copies, so there
is no checkpoint), keeps the latest valid copy per ``(brand_id, product_id)`` and MERGEs it into
``silver.products`` (insert new keys, update a key only when its Bronze reference changed). Parse
and contract failures are skipped; ``dlq_gate`` owns the DLQ rows. ``--namespace <ns>`` reads and
writes the ``<ns>`` paths; such runs are never registered in the catalog (D-060).

Re-running on unchanged Bronze rewrites nothing (an idempotent outcome, not exactly-once). Single
writer per Silver table is assumed. One ``ops.pipeline_metrics`` row is recorded per run. Logs carry
counts only.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.jobs import main as run_job
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import (
    CATALOG_TABLES,
    delta_exists_at_location,
    register_table,
    resolve,
)
from lakehouse.ops.metrics import BatchRecorder, metrics_path
from lakehouse.silver.jobs.silver_customers import CatalogRegistrationError
from lakehouse.silver.products import run_products


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import SparkSession

    from lakehouse.common.jobs import JobContext
    from lakehouse.common.spark import SparkSettings

_log = get_logger(__name__)
JOB_NAME = "silver_products"
BRONZE_TABLE = "raw_products"
SILVER_TABLE = "products"


class SilverProductsJob(SparkJob):
    """Keep the latest valid Bronze product copy per key in ``silver.products``."""

    name = JOB_NAME

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--namespace", default=None, help="optional single path segment isolating this run"
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        namespace: str | None = args.namespace
        bronze = resolve(ctx.settings, CATALOG_TABLES[BRONZE_TABLE], namespace)
        silver = resolve(ctx.settings, CATALOG_TABLES[SILVER_TABLE], namespace)
        if not delta_exists_at_location(spark, bronze):
            raise RuntimeError(f"no Bronze table at {bronze.location}; bronze_batch never ran")

        counts = run_products(spark, spark.read.format("delta").load(bronze.location), silver)
        _log.info("silver_products.done", **counts.as_dict())
        recorder = BatchRecorder(
            spark,
            metrics_path(ctx.settings, namespace),
            job=JOB_NAME,
            source_table=BRONZE_TABLE,
            run_id=ctx.run_id,
        )
        recorder.bind_query(ctx.run_id)  # a batch run has no streaming query: the run id stands in
        recorder.record(0, counts.as_metrics())
        # After the counters are logged: a registration failure must not hide the evidence.
        catalog = register_silver_table(spark, ctx.settings, namespace)
        return {
            "counts": counts.as_dict(),
            "path": silver.location,
            **({"catalog": catalog} if catalog else {}),
        }


def register_silver_table(
    spark: SparkSession, settings: SparkSettings, namespace: str | None
) -> dict[str, str]:
    """Register ``silver.products`` after a successful run (writes stay path-based, D-060).

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


def main(argv: list[str] | None = None) -> int:
    """Run this job through the shared runner: ``argv`` are the job's own arguments."""
    return run_job([JOB_NAME, *(sys.argv[1:] if argv is None else argv)])


JOB = SilverProductsJob

if __name__ == "__main__":
    raise SystemExit(main())
