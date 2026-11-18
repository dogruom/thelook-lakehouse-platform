"""Read-only ops report: reconciliation terms, DLQ rate, freshness and lag per table and tenant.

Run as ``python scripts/submit_job.py metrics_report [--namespace <segment>] [--table <name>]...
[--output-uri <base>]`` (on AWS as the Glue job ``crm-lakehouse-dev-metrics_report``, D-103). It
reads ``ops.pipeline_metrics`` (latest ``scope='reconcile'`` row per table and tenant, plus the sums
of the ``scope='batch'`` rows per table) and, per Bronze table and tenant, ``count``,
``max(_ingest_ts)`` and, where the column exists, ``max(_kafka_ts)``. Only small aggregated rows
reach the driver; all rules (rates, NULL handling, sorting, formatting) live in
``lakehouse.ops.report``. Logs the markdown (``metrics_report.markdown``) and, with
``--output-uri``, writes ``report.md`` and ``report.json`` to ``<base>/<run_id>/`` through the
Hadoop FileSystem API (no boto3 in Spark jobs; works for ``s3a://`` locally, ``s3://`` on AWS and
``file://``; the base is typically ``<layer root>/ops/reports``, nothing is assumed about the
endpoint). A missing ``ops.pipeline_metrics`` fails the job; a missing Bronze table is skipped with
a warning (its freshness is then ``n/a``). Counts only; no payload value is read or logged.

Lag: Kafka consumer offsets are not available (not on Glue, not through Spark here), so the report
shows ``max(_ingest_ts) - max(_kafka_ts)`` for Kafka-sourced Bronze tables and ``n/a`` otherwise.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lakehouse.common.brand_key import brand_key
from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import (
    CATALOG_TABLES,
    ResolvedTable,
    delta_exists_at_location,
    read_delta,
    resolve,
    table_exists,
)
from lakehouse.ops.metrics import SCOPE_BATCH, SCOPE_RECONCILE, TERMS, metrics_path
from lakehouse.ops.report import build_batch_totals, build_rows, to_json, to_markdown


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import SparkSession

    from lakehouse.common.jobs import JobContext

DEFAULT_TABLES = ("raw_customers", "raw_clickstream", "raw_orders", "raw_products")
_RECONCILE_COLUMNS = (
    "source_table",
    "brand_id",
    "run_id",
    "scope",
    "recorded_at",
    "balanced",
    "bronze",
    "parse_errors",
    *TERMS,
)
_BATCH_SUMS = ("bronze", "parse_errors", *TERMS)
_URI_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9+.-]*://\S+$|^/\S+$")
_log = get_logger(__name__)


def _output_uri(value: str) -> str:
    import argparse as _argparse

    base = value.rstrip("/")
    if not _URI_RE.match(base) or ".." in base.split("/"):
        raise _argparse.ArgumentTypeError(
            "--output-uri must be a URI or absolute path without '..'"
        )
    return base


def _reconcile_rows(spark: SparkSession, path: str) -> list[dict[str, Any]]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = spark.read.format("delta").load(path).where(F.col("scope") == SCOPE_RECONCILE)
    latest = Window.partitionBy("source_table", "brand_id").orderBy(
        F.col("recorded_at").desc(), F.col("run_id").desc()
    )
    ranked = df.withColumn("_rn", F.row_number().over(latest)).where(F.col("_rn") == 1)
    return [r.asDict() for r in ranked.select(*_RECONCILE_COLUMNS).collect()]


def _batch_sums(spark: SparkSession, path: str) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    df = spark.read.format("delta").load(path).where(F.col("scope") == SCOPE_BATCH)
    out = df.groupBy("source_table").agg(
        F.count(F.lit(1)).alias("batches"),
        F.sum(F.when(F.coalesce(F.col("balanced"), F.lit(False)), 0).otherwise(1)).alias(
            "unbalanced_batches"
        ),
        *[F.sum(c).alias(c) for c in _BATCH_SUMS],
    )
    return [r.asDict() for r in out.collect()]


def _freshness(
    spark: SparkSession, ctx: JobContext, tables: list[str], namespace: str | None
) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    items: list[dict[str, Any]] = []
    for name in tables:
        source = resolve(ctx.settings, CATALOG_TABLES[name], namespace)
        if not table_exists(spark, source):
            _log.warning("metrics_report.bronze_missing", table=name, location=source.location)
            continue
        df = read_delta(spark, source)
        aggs = [F.max("_ingest_ts").alias("max_ingest_ts")]
        if "_kafka_ts" in df.columns:  # batch-loaded Bronze tables have no Kafka timestamp
            aggs.append(F.max("_kafka_ts").alias("max_kafka_ts"))
        for r in df.groupBy(brand_key().alias("brand_id")).agg(*aggs).collect():
            items.append({"source_table": name, **r.asDict()})
    return items


def write_report(spark: SparkSession, base_uri: str, run_id: str, files: Mapping[str, str]) -> str:
    """Write ``files`` (name -> text) to ``<base_uri>/<run_id>/`` with the Hadoop FileSystem API."""
    jvm: Any = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    directory = f"{base_uri}/{run_id}"
    path_cls = jvm.org.apache.hadoop.fs.Path
    fs = path_cls(directory).getFileSystem(conf)
    fs.mkdirs(path_cls(directory))
    for name, text in files.items():
        data = bytearray(text.encode("utf-8"))
        stream = fs.create(path_cls(f"{directory}/{name}"), True)
        try:
            stream.write(data, 0, len(data))
        finally:
            stream.close()
    return directory


class MetricsReportJob(SparkJob):
    """Build the per-table, per-tenant ops report from ``ops.pipeline_metrics`` and Bronze."""

    name = "metrics_report"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--namespace", default=None, help="optional single path segment")
        parser.add_argument(
            "--table",
            action="append",
            default=None,
            choices=DEFAULT_TABLES,
            help="Bronze table for freshness (repeatable; default: all four)",
        )
        parser.add_argument(
            "--output-uri",
            type=_output_uri,
            default=None,
            help="base URI; writes <base>/<run_id>/report.md and report.json (default: log only)",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        namespace: str | None = args.namespace
        tables: list[str] = list(dict.fromkeys(args.table or DEFAULT_TABLES))
        path = metrics_path(ctx.settings, namespace)
        if not delta_exists_at_location(spark, ResolvedTable(None, path)):
            raise RuntimeError(
                f"no ops.pipeline_metrics at {path}; run a Silver job and reconcile first"
            )
        now = datetime.now(UTC).replace(tzinfo=None)
        rows = build_rows(
            _reconcile_rows(spark, path), _freshness(spark, ctx, tables, namespace), now=now
        )
        totals = build_batch_totals(_batch_sums(spark, path))
        env = ctx.settings.env
        markdown = to_markdown(rows, totals, run_id=ctx.run_id, generated_at=now, env=env)
        _log.info("metrics_report.markdown", rows=len(rows), markdown=markdown)
        report_uri: str | None = None
        if args.output_uri:
            files = {
                "report.md": markdown,
                "report.json": to_json(rows, totals, run_id=ctx.run_id, generated_at=now, env=env),
            }
            report_uri = write_report(spark, args.output_uri, ctx.run_id, files)
            _log.info("metrics_report.written", uri=report_uri)
        mismatches = sum(1 for r in rows if r["reconcile"] == "MISMATCH")
        return {
            "rows": len(rows),
            "batch_total_tables": len(totals),
            "reconcile_mismatches": mismatches,
            "report_uri": report_uri,
        }


JOB = MetricsReportJob
