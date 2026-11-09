"""Read-only report on Bronze tables: row counts, parse errors, NULL metadata and tenant split.

Run as ``python scripts/submit_job.py bronze_report [--namespace <segment>] [--table <name>]...``.
It answers "did every Kafka message become exactly one row?" without printing any payload value
(PII): only counts are logged; a brand header is unvalidated input, so values outside the
tenant-id contract are aggregated under ``__invalid__`` and never reported.
``rows == distinct_source_refs`` means no duplicate message in the table. ``null_counts`` should
all be 0 except ``_raw_value``, which is legitimately NULL for Kafka tombstones (``brand_id`` may
also be NULL for unparseable rows without a header, and for contract violations the D10 gate will
reject). ``offsets_by_partition`` (min/max/rows/distinct offset per partition) and
``distinct_lineages`` (checkpoint lineages seen in ``_batch_id``) prove the exact offset set. A missing table (the stream never ran for this namespace)
fails the job with a clear message instead of a raw Spark error. An empty topic is not a missing
table: its first run commits a zero-row batch, which creates the table with its schema.

This job reads the table by catalog name when the run has a catalog and no namespace (otherwise by
path, see ``lakehouse.common.tables``) and imports nothing from ``lakehouse.ingestion`` (component
independence, ADR-0009); the column names are the Bronze contract.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import TableSpec, read_delta, resolve, table_exists
from lakehouse.contracts.models import BRAND_ID_PATTERN


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
INVALID_BRAND_KEY = "__invalid__"
# Java's ``$`` also matches before a trailing newline; ``\z`` is the true end of input.
_BRAND_RLIKE = BRAND_ID_PATTERN.removesuffix("$") + r"\z"
_log = get_logger(__name__)


def _opt_int(value: object) -> int | None:
    return None if value is None else int(str(value))


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
        from pyspark.sql import functions as F

        tables: list[str] = list(dict.fromkeys(args.table or DEFAULT_TABLES))
        report: dict[str, Any] = {}
        for table in tables:
            source = resolve(ctx.settings, TableSpec("bronze", "bronze", table), args.namespace)
            path = source.location
            if not table_exists(spark, source):
                hint = "; run register_tables" if source.name is not None else ""
                raise RuntimeError(f"no Bronze table at {path}; stream never ran for it{hint}")
            df = read_delta(spark, source)
            aggregates = [
                F.count(F.lit(1)).alias("rows"),
                F.countDistinct("_source_ref").alias("distinct_source_refs"),
                F.countDistinct("_batch_id").alias("distinct_batch_ids"),
                # _batch_id = "<query id>:<batch id>"; the part before the last ":" is the lineage.
                F.countDistinct(F.regexp_replace("_batch_id", r":[^:]*\z", "")).alias(
                    "distinct_lineages"
                ),
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
            brand_key = (
                F.when(F.col("brand_id").isNull(), F.lit(NULL_BRAND_KEY))
                .when(F.col("brand_id").rlike(_BRAND_RLIKE), F.col("brand_id"))
                .otherwise(F.lit(INVALID_BRAND_KEY))
            )
            by_brand = {
                str(r["brand_key"]): int(r["n"])
                for r in df.groupBy(brand_key.alias("brand_key"))
                .agg(F.count(F.lit(1)).alias("n"))
                .collect()
            }
            # _source_ref = "<topic>/<partition>/<offset>" (topic names contain no "/").
            ref = F.split(F.col("_source_ref"), "/")
            offset = ref.getItem(2).cast("long")
            offsets_by_partition = {
                str(r["partition"]): {
                    "min": _opt_int(r["min_offset"]),
                    "max": _opt_int(r["max_offset"]),
                    "rows": int(r["n"]),
                    "distinct": int(r["distinct_offsets"]),
                }
                for r in df.where(F.col("_source_ref").isNotNull())
                .groupBy(ref.getItem(1).alias("partition"))
                .agg(
                    F.min(offset).alias("min_offset"),
                    F.max(offset).alias("max_offset"),
                    F.count(F.lit(1)).alias("n"),
                    F.countDistinct(offset).alias("distinct_offsets"),
                )
                .collect()
            }
            report[table] = {
                "path": path,
                "rows": int(row["rows"]),
                "distinct_source_refs": int(row["distinct_source_refs"]),
                "parse_errors": parse_errors,
                "null_counts": {c: int(row[f"null_{c}"]) for c in NULL_COUNT_COLUMNS},
                "rows_by_brand": by_brand,
                "distinct_batch_ids": int(row["distinct_batch_ids"]),
                "distinct_lineages": int(row["distinct_lineages"]),
                "offsets_by_partition": offsets_by_partition,
            }
            _log.info("bronze_report.table", table=table, rows=report[table]["rows"])
        return {"tables": report}


JOB = BronzeReportJob
