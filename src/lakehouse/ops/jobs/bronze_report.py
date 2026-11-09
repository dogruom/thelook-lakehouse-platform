"""Read-only report on Bronze tables: row counts, parse errors, NULL metadata and tenant split.

Run as ``python scripts/submit_job.py bronze_report [--namespace <segment>] [--table <name>]...``.
It answers "did every Kafka message become exactly one row?" without printing any payload value
(PII): only counts are logged. ``rows == distinct_source_refs`` means no duplicate message in the
table; ``null_counts`` should all be 0 except ``_raw_value``, which is legitimately NULL for Kafka
tombstones (``brand_id`` may also be NULL for unparseable rows without a header, and for contract
violations the D10 gate will reject). A missing table (empty topic, stream never ran) fails the
job with a clear message instead of a raw Spark error.

This job reads the table by path and imports nothing from ``lakehouse.ingestion`` (component
independence, ADR-0009); the column names are the Bronze contract.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import SparkSession

    from lakehouse.common.jobs import JobContext

DEFAULT_TABLES = ("raw_customers", "raw_clickstream")
NULL_COUNT_COLUMNS = (
    "_raw_value",
    "_source_ref",
    "_ingest_ts",
    "_batch_id",
    "_kafka_ts",
    "brand_id",
)
NULL_BRAND_KEY = "__null__"
_log = get_logger(__name__)


class BronzeReportJob(SparkJob):
    """Summarise one or more Bronze tables."""

    name = "bronze_report"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--namespace", default=None, help="optional single path segment")
        parser.add_argument(
            "--table",
            action="append",
            default=None,
            help="Bronze table name (repeatable; default: raw_customers and raw_clickstream)",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        ns_parts = [args.namespace] if args.namespace else []
        tables: list[str] = list(dict.fromkeys(args.table or DEFAULT_TABLES))
        report: dict[str, Any] = {}
        for table in tables:
            path = ctx.settings.layer_uri("bronze", *ns_parts, table)
            if not DeltaTable.isDeltaTable(spark, path):
                raise RuntimeError(f"no Bronze table at {path}; topic empty or stream not run")
            df = spark.read.format("delta").load(path)
            aggregates = [
                F.count(F.lit(1)).alias("rows"),
                F.countDistinct("_source_ref").alias("distinct_source_refs"),
                F.countDistinct("_batch_id").alias("distinct_batch_ids"),
                *[
                    F.count(F.when(F.col(c).isNull(), 1)).alias(f"null_{c}")
                    for c in NULL_COUNT_COLUMNS
                ],
            ]
            row = df.agg(*aggregates).first()
            if row is None:  # an aggregate without grouping always returns one row
                raise RuntimeError(f"no aggregate row for {path}")
            parse_errors = {
                str(r["_parse_error"]): int(r["n"])
                for r in df.where(F.col("_parse_error").isNotNull())
                .groupBy("_parse_error")
                .agg(F.count(F.lit(1)).alias("n"))
                .collect()
            }
            by_brand = {
                (NULL_BRAND_KEY if r["brand_id"] is None else str(r["brand_id"])): int(r["n"])
                for r in df.groupBy("brand_id").agg(F.count(F.lit(1)).alias("n")).collect()
            }
            report[table] = {
                "path": path,
                "rows": int(row["rows"]),
                "distinct_source_refs": int(row["distinct_source_refs"]),
                "parse_errors": parse_errors,
                "null_counts": {c: int(row[f"null_{c}"]) for c in NULL_COUNT_COLUMNS},
                "rows_by_brand": by_brand,
                "distinct_batch_ids": int(row["distinct_batch_ids"]),
            }
            _log.info("bronze_report.table", table=table, rows=report[table]["rows"])
        return {"tables": report}


JOB = BronzeReportJob
