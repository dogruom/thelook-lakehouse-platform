"""Register the existing Bronze and DLQ Delta tables in the catalog (Hive Metastore / Glue).

Run as ``python scripts/submit_job.py register_tables [--table <name>]...`` (default: every table
in :data:`lakehouse.common.tables.CATALOG_TABLES`). Data stays where it is; this only points
``bronze.raw_customers`` etc. at the existing Delta locations so jobs and dbt can address them by
name. A table without a Delta table at its location is reported as ``missing`` (not an error: the
stream or the gate has not produced it yet). Re-running is idempotent (``exists``). A table already
registered at a different location fails the job (never re-pointed silently).

Requires a catalog (``LAKEHOUSE_CATALOG=hive`` or ``glue``); with the in-memory catalog nothing
survives the session, so the job refuses to run. Namespaced test runs are never registered.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import (
    CATALOG_TABLES,
    delta_exists_at_location,
    register_table,
    resolve,
)


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import SparkSession

    from lakehouse.common.jobs import JobContext

_log = get_logger(__name__)


class RegisterTablesJob(SparkJob):
    """Register Bronze and DLQ tables in the catalog."""

    name = "register_tables"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--table",
            action="append",
            default=None,
            choices=sorted(CATALOG_TABLES),
            help="table name (repeatable; default: all catalog tables)",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        if ctx.settings.effective_catalog == "in_memory":
            raise RuntimeError(
                "catalog is in_memory: nothing to register (set LAKEHOUSE_CATALOG=hive or glue)"
            )
        keys: list[str] = list(dict.fromkeys(args.table or sorted(CATALOG_TABLES)))
        report: dict[str, Any] = {}
        for key in keys:
            spec = CATALOG_TABLES[key]
            resolved = resolve(ctx.settings, spec)
            if resolved.name is None:  # unreachable with a catalog and no namespace
                raise RuntimeError(f"{key} resolved without a catalog name")
            if not delta_exists_at_location(spark, resolved):
                status = "missing"
            else:
                status = register_table(spark, spec, resolved)
            report[key] = {"name": resolved.name, "location": resolved.location, "status": status}
            _log.info("register_tables.table", table=key, **report[key])
        return {"tables": report}


JOB = RegisterTablesJob
