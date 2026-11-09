"""Smoke job: write a small Delta table, read it back and compare counts.

``--expect-packaged`` additionally proves that the application code reached the driver and the
executors through ``--py-files`` (``lakehouse_app-<sha12>.zip``) and not through the source mount.
PySpark and Delta are imported lazily so this module imports on hosts without them.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger


if TYPE_CHECKING:
    import argparse
    from collections.abc import Iterable, Mapping

    from pyspark.sql import SparkSession

    from lakehouse.common.jobs import JobContext

PACKAGE_MARKER = "lakehouse_app-<hash>.zip"
# The zip is content-addressed (``lakehouse_app-<sha12>.zip``); the origin must be inside one.
_PACKAGED_ORIGIN = re.compile(r"lakehouse_app-[0-9a-f]+\.zip/lakehouse/")
_log = get_logger(__name__)


def _positive_int(value: str) -> int:
    import argparse as _argparse

    try:
        number = int(value)
    except ValueError:
        raise _argparse.ArgumentTypeError(f"{value!r} is not an integer") from None
    if number < 1:
        raise _argparse.ArgumentTypeError(f"must be >= 1, got {number}")
    return number


def code_origin(_: object = None) -> str:
    """Return the file the ``lakehouse`` package was imported from (runs on driver/executors)."""
    import lakehouse

    return str(lakehouse.__file__)


def assert_packaged(origins: Iterable[str]) -> None:
    """Raise unless there is at least one origin and every origin is inside the shipped zip."""
    found = sorted(set(origins))
    if not found:
        raise RuntimeError("no code origins reported; cannot prove the code was packaged")
    bad = [origin for origin in found if _PACKAGED_ORIGIN.search(origin) is None]
    if bad:
        raise RuntimeError(f"code not loaded from {PACKAGE_MARKER}: {bad}")


class HelloDeltaJob(SparkJob):
    """Write ``--rows`` rows as Delta, read them back and verify the count."""

    name = "hello_delta"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--rows", type=_positive_int, default=5, help="rows to write (>= 1)")
        parser.add_argument("--path", default=None, help="target URI (default: unique per run)")
        parser.add_argument(
            "--expect-packaged",
            action="store_true",
            help="fail unless driver and executors load the code from lakehouse_app-<hash>.zip",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        path: str = args.path or ctx.settings.layer_uri("ops", "hello_delta", ctx.run_id)
        rows: int = args.rows
        df = spark.range(rows).withColumn("run_id", F.lit(ctx.run_id))
        df.write.format("delta").save(path)

        rows_read = spark.read.format("delta").load(path).count()
        if rows_read != rows:
            raise RuntimeError(f"read {rows_read} rows from {path}, wrote {rows}")
        version = int(DeltaTable.forPath(spark, path).history(1).collect()[0]["version"])

        metrics: dict[str, Any] = {
            "rows_written": rows,
            "rows_read": rows_read,
            "delta_version": version,
            "path": path,
        }
        if args.expect_packaged:
            driver = code_origin()
            executors = sorted(
                spark.sparkContext.parallelize(range(2), 2).map(code_origin).distinct().collect()
            )
            assert_packaged([driver, *executors])
            metrics["driver_code"] = driver
            metrics["executor_code"] = executors
        _log.info("hello_delta.verified", rows=rows_read, delta_version=version)
        return metrics


JOB = HelloDeltaJob
