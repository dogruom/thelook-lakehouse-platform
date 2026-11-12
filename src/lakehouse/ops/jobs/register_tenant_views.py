"""Register one Silver view per (table, brand) in the catalog (W3.D18, D-085).

Run as ``python scripts/submit_job.py register_tenant_views --brand <brand_id> [--brand ...]``.
Creates ``silver.v_<table>__<brand>`` = ``SELECT * FROM silver.<table> WHERE brand_id = '<brand>'``
for ``customers``, ``orders`` and ``order_items`` (idempotent ``CREATE OR REPLACE``). The brands
are validated before any SQL runs. Needs a catalog (``hive`` / ``glue``); with the in-memory
catalog or a ``--namespace`` run nothing is registered (namespaced runs never touch the shared
catalog), so the job refuses. The views are a convenience, not access control.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.silver.views import tenant_view_sql


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import SparkSession

    from lakehouse.common.jobs import JobContext

TENANT_TABLES = ("customers", "orders", "order_items")
_log = get_logger(__name__)


class RegisterTenantViewsJob(SparkJob):
    """Create the per-brand Silver views."""

    name = "register_tenant_views"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--brand",
            action="append",
            required=True,
            help="brand_id (repeatable); validated before any SQL is issued",
        )
        parser.add_argument("--namespace", default=None, help="refused: named runs only")

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        if ctx.settings.effective_catalog == "in_memory":
            raise RuntimeError(
                "catalog is in_memory: nothing to register (set LAKEHOUSE_CATALOG=hive or glue)"
            )
        if getattr(args, "namespace", None):
            raise RuntimeError("namespaced runs are never registered in the shared catalog")
        brands: list[str] = list(dict.fromkeys(args.brand))
        statements = [
            (table, brand, tenant_view_sql(table, brand, temp=False))
            for table in TENANT_TABLES
            for brand in brands
        ]  # builds (and so validates) every statement before the first spark.sql
        for table in TENANT_TABLES:
            if not spark.catalog.tableExists(f"silver.{table}"):
                raise RuntimeError(f"silver.{table} is not registered; run its job first")
        views: list[str] = []
        for table, brand, sql in statements:
            spark.sql(sql)
            views.append(f"silver.v_{table}__{brand}")
            _log.info("register_tenant_views.view", table=table, brand_id=brand)
        return {"views": views}


JOB = RegisterTenantViewsJob
