"""Read-only report on the dead-letter tables: counts by rule code, category, tenant and status.

Run as ``python scripts/submit_job.py dlq_report [--namespace <segment>] [--table <name>]...
[--list-refs]``. Only counts and codes are logged, never payload values (PII); a brand value is
unvalidated input, so values outside the tenant-id contract are aggregated under ``__invalid__``.
``rows == distinct_dlq_ids`` means no message was dead-lettered twice. ``--list-refs`` additionally
logs ``dlq_report.refs``: a sorted list of ``[event_id, _bronze_ref, _rejection_codes]`` triples so
a rejected message can be located in Bronze (``_bronze_ref`` = ``topic/partition/offset``); an
``event_id`` that is not a UUIDv4 is shown as ``__invalid__`` and NULL as ``__null__`` (the DLQ
table itself keeps the raw value).
A missing table (the gate never ran for this namespace) fails the job with a clear message.

This job reads the tables by catalog name when the run has a catalog and no namespace
(otherwise by path, see ``lakehouse.common.tables``) and imports nothing from
``lakehouse.quality`` (component independence, ADR-0009); the column names are the DLQ contract.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import TableSpec, read_delta, resolve, table_exists
from lakehouse.contracts.models import BRAND_ID_PATTERN, UUID_V4_PATTERN


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import Column, DataFrame, SparkSession

    from lakehouse.common.jobs import JobContext

DEFAULT_TABLES = ("dlq_customers", "dlq_clickstream")
NULL_BRAND_KEY = "__null__"
INVALID_BRAND_KEY = "__invalid__"
# Java's ``$`` also matches before a trailing newline; ``\z`` is the true end of input.
_BRAND_RLIKE = BRAND_ID_PATTERN.removesuffix("$") + r"\z"
_UUID_RE = re.compile(UUID_V4_PATTERN.removesuffix("$") + r"\Z")  # \Z: no trailing-newline match
_log = get_logger(__name__)


def _safe_event_id(value: str | None) -> str:
    """Log-safe event id: NULL -> ``__null__``, anything but a UUIDv4 -> ``__invalid__``."""
    if value is None:
        return NULL_BRAND_KEY
    return value if _UUID_RE.match(value) else INVALID_BRAND_KEY


def _brand_key() -> Column:
    from pyspark.sql import functions as F

    brand = F.col("brand_id")
    return (
        F.when(brand.isNull(), F.lit(NULL_BRAND_KEY))
        .when(brand.rlike(_BRAND_RLIKE), brand)
        .otherwise(F.lit(INVALID_BRAND_KEY))
    )


def _counts(df: DataFrame, key: Column, name: str) -> dict[str, int]:
    from pyspark.sql import functions as F

    return {
        str(r[name]): int(r["n"])
        for r in df.groupBy(key.alias(name)).agg(F.count(F.lit(1)).alias("n")).collect()
    }


class DlqReportJob(SparkJob):
    """Summarise one or more DLQ tables."""

    name = "dlq_report"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--namespace", default=None, help="optional single path segment")
        parser.add_argument(
            "--table",
            action="append",
            default=None,
            choices=DEFAULT_TABLES,
            help="DLQ table name (repeatable; default: dlq_customers and dlq_clickstream)",
        )
        parser.add_argument(
            "--list-refs",
            action="store_true",
            help="also log [event_id, _bronze_ref, _rejection_codes] per row (sorted)",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        from pyspark.sql import functions as F

        tables: list[str] = list(dict.fromkeys(args.table or DEFAULT_TABLES))
        report: dict[str, Any] = {}
        for table in tables:
            source = resolve(
                ctx.settings, TableSpec("dead_letter", "dead_letter", table), args.namespace
            )
            path = source.location
            if not table_exists(spark, source):
                hint = "; run register_tables" if source.name is not None else ""
                raise RuntimeError(f"no DLQ table at {path}; the gate never ran for it{hint}")
            df = read_delta(spark, source)
            row = df.agg(
                F.count(F.lit(1)).alias("rows"),
                F.countDistinct("_dlq_id").alias("distinct_dlq_ids"),
            ).first()
            if row is None:  # an aggregate without grouping always returns one row
                raise RuntimeError(f"no aggregate row for {path}")
            by_code = {
                str(r["code"]): int(r["n"])
                for r in df.select(F.explode("_rejection_codes").alias("code"))
                .groupBy("code")
                .agg(F.count(F.lit(1)).alias("n"))
                .collect()
            }
            report[table] = {
                "path": path,
                "rows": int(row["rows"]),
                "distinct_dlq_ids": int(row["distinct_dlq_ids"]),
                "by_code": by_code,
                "by_category": _counts(df, F.col("_error_category"), "category"),
                "by_brand": _counts(df, _brand_key(), "brand_key"),
                "replay_status": _counts(df, F.col("_replay_status"), "status"),
            }
            _log.info("dlq_report.table", table=table, **report[table])
            if args.list_refs:
                refs = sorted(
                    (
                        [
                            _safe_event_id(r["event_id"]),
                            r["_bronze_ref"],
                            list(r["_rejection_codes"]),
                        ]
                        for r in df.select("event_id", "_bronze_ref", "_rejection_codes").collect()
                    ),
                    key=lambda ref: (ref[0] or "", ref[1] or ""),
                )
                _log.info("dlq_report.refs", table=table, refs=refs)
        return {"tables": report}


JOB = DlqReportJob
