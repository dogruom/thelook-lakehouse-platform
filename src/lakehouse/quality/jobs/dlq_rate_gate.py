"""DLQ-rate quality gate: halt downstream when a brand's DLQ share is too high (W4.D23.2, D-099).

Run as ``python scripts/submit_job.py dlq_rate_gate --max-dlq-rate <0..1> --brand <id>...
[--namespace <segment>]`` (the Airflow DAG submits it once per brand after ``reconcile``).

For every requested brand and every Bronze table that has a DLQ table (customers, clickstream,
orders, products) it computes ``dlq_rate = dlq rows / parseable Bronze rows`` (definitions and the
"above, not equal" rule: :mod:`lakehouse.quality.dlq_rate`). The job logs ``dlq_rate_gate.report``
with the per-brand report; when any brand is above ``--max-dlq-rate`` it logs
``dlq_rate_gate.breach`` and raises :class:`DlqRateBreachError` (exit 1) naming the brands.
It is read-only: it writes no table. Only counts, rates and validated brand ids are logged.
A missing Bronze or DLQ table counts as 0 rows (nothing to judge), like in ``reconcile``.
"""

from __future__ import annotations

import argparse
import re
from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import CATALOG_TABLES, read_delta, resolve, table_exists
from lakehouse.contracts.models import BRAND_ID_PATTERN
from lakehouse.quality.dlq import DLQ_TABLE_BY_BRONZE, REPLAYED
from lakehouse.quality.dlq_rate import (
    BREACH_EVENT,
    DlqRateBreachError,
    breach_message,
    breached_brands,
    evaluate_rates,
    report,
    validate_max_rate,
)


if TYPE_CHECKING:
    from collections.abc import Mapping

    from pyspark.sql import DataFrame, SparkSession

    from lakehouse.common.jobs import JobContext

_log = get_logger(__name__)
_BRAND_RE = re.compile(BRAND_ID_PATTERN.removesuffix("$") + r"\Z")


def _brand(value: str) -> str:
    if not _BRAND_RE.match(value):
        raise argparse.ArgumentTypeError("not a valid brand id (brand_[a-z0-9_]{1,40})")
    return value


def _rate(value: str) -> float:
    try:
        return validate_max_rate(float(value))
    except ValueError:
        raise argparse.ArgumentTypeError("must be a number within [0, 1]") from None


def _read(
    spark: SparkSession, ctx: JobContext, name: str, namespace: str | None
) -> DataFrame | None:
    table = resolve(ctx.settings, CATALOG_TABLES[name], namespace)
    return read_delta(spark, table) if table_exists(spark, table) else None


def _per_brand(df: DataFrame, brands: list[str], ref: str, only: Any) -> dict[str, int]:
    """Distinct ``ref`` values per requested brand, for the rows selected by ``only``."""
    from pyspark.sql import functions as F

    out = (
        df.where(F.col("brand_id").isin(brands))
        .where(only)
        .groupBy("brand_id")
        .agg(F.countDistinct(ref).alias("n"))
    )
    return {str(r["brand_id"]): int(r["n"]) for r in out.collect()}


def read_counts(
    spark: SparkSession, ctx: JobContext, brands: list[str], namespace: str | None
) -> dict[tuple[str, str], tuple[int, int]]:
    """``{(brand, bronze table): (parseable Bronze rows, pending DLQ rows)}`` from Delta."""
    from pyspark.sql import functions as F

    counts: dict[tuple[str, str], tuple[int, int]] = {}
    for bronze_name, dlq_name in DLQ_TABLE_BY_BRONZE.items():
        bronze_df = _read(spark, ctx, bronze_name, namespace)
        if bronze_df is None:
            continue
        bronze = _per_brand(bronze_df, brands, "_source_ref", F.col("_parse_error").isNull())
        dlq_df = _read(spark, ctx, dlq_name, namespace)
        pending = (
            {}
            if dlq_df is None
            else _per_brand(
                dlq_df,
                brands,
                "_bronze_ref",
                F.coalesce(F.col("_replay_status"), F.lit("")) != REPLAYED,
            )
        )
        for brand in brands:
            counts[(brand, bronze_name)] = (bronze.get(brand, 0), pending.get(brand, 0))
    return counts


class DlqRateGateJob(SparkJob):
    """Fail when a brand's DLQ rate is above the threshold."""

    name = "dlq_rate_gate"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--namespace", default=None, help="optional single path segment")
        parser.add_argument(
            "--max-dlq-rate",
            type=_rate,
            required=True,
            help="highest accepted dlq_rate in [0, 1]; a rate above it breaches",
        )
        parser.add_argument(
            "--brand",
            action="append",
            required=True,
            type=_brand,
            help="brand id to judge (repeatable)",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        brands: list[str] = list(dict.fromkeys(args.brand))
        max_rate: float = args.max_dlq_rate
        counts = read_counts(spark, ctx, brands, args.namespace)
        results = evaluate_rates(brands, list(DLQ_TABLE_BY_BRONZE), counts, max_rate)
        summary = {"max_dlq_rate": max_rate, "brands": report(results)}
        _log.info("dlq_rate_gate.report", **summary)
        breached = breached_brands(results)
        if breached:
            _log.error(BREACH_EVENT, brands=breached, max_dlq_rate=max_rate)
            raise DlqRateBreachError(breach_message(results, max_rate))
        return summary


JOB = DlqRateGateJob
