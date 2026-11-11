"""Content hash of Delta tables, for before/after comparisons (W3.D18, D-084).

Run as ``python scripts/submit_job.py content_hash --table <name>... [--namespace <segment>]
[--exclude <column>]...``. For every ``--table`` (a name of ``CATALOG_TABLES``, e.g. ``orders``)
the job computes :func:`lakehouse.common.hashing.content_hash` and returns
``{"hashes": {table: hash}, "rows": {table: n}}`` in the ``job.succeeded`` metrics, so a test can
compare two runs without parsing Delta on the host. ``--exclude`` (repeatable) names the columns left
out of the hash and replaces the default ``_updated_at`` (a wall clock). Only counts and hashes are
logged, never row values.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lakehouse.common.hashing import DEFAULT_EXCLUDE, content_hash
from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import CATALOG_TABLES, read_delta, resolve, table_exists


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import SparkSession

    from lakehouse.common.jobs import JobContext

_log = get_logger(__name__)


class ContentHashJob(SparkJob):
    """Hash the content of one or more tables."""

    name = "content_hash"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--namespace", default=None, help="optional single path segment")
        parser.add_argument(
            "--table",
            action="append",
            required=True,
            choices=sorted(CATALOG_TABLES),
            help="table name (repeatable)",
        )
        parser.add_argument(
            "--exclude",
            action="append",
            default=None,
            help=f"column left out of the hash (repeatable; default: {', '.join(DEFAULT_EXCLUDE)})",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        tables: list[str] = list(dict.fromkeys(args.table))
        exclude: list[str] = list(dict.fromkeys(args.exclude or DEFAULT_EXCLUDE))
        sources = {t: resolve(ctx.settings, CATALOG_TABLES[t], args.namespace) for t in tables}
        for name, source in sources.items():  # fail before hashing anything
            if not table_exists(spark, source):
                raise RuntimeError(f"no table {name} at {source.location}")
        hashes: dict[str, str] = {}
        rows: dict[str, int] = {}
        for name, source in sources.items():
            df = read_delta(spark, source)
            hashes[name] = content_hash(df, exclude)
            rows[name] = df.count()
            _log.info("content_hash.table", table=name, rows=rows[name], hash=hashes[name])
        return {"hashes": hashes, "rows": rows, "exclude": exclude}


JOB = ContentHashJob
