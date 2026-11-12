"""Reconciliation of Bronze, DLQ and Silver against ``ops.pipeline_metrics`` (W3.D18, D-081/D-082).

Run as ``python scripts/submit_job.py reconcile [--namespace <segment>]`` after the Silver jobs
(and ``dlq_gate``, which fills the customer and clickstream DLQ). Per batch, counted in Bronze
events: ``bronze = silver_accepted + dlq + duplicates_removed + stale_cdc_noop +
late_beyond_window``. The job runs three checks and exits non-zero when one fails:

(a) every ``scope='batch'`` row balances (re-derived from its terms, not only the stored flag);
(b) data-derived totals per ``(source_table, brand_id)`` are consistent: ``bronze`` = distinct
    ``_source_ref`` without ``_parse_error``, ``parse_errors`` = distinct refs with one,
    ``reread_rows`` = rows - distinct refs, ``dlq`` = distinct ``_bronze_ref`` of the DLQ whose
    ``_replay_status`` is not ``REPLAYED``, ``silver_accepted`` = Silver rows (orders, clickstream),
    and the remainder ``bronze - silver_accepted - dlq`` is not negative;
(c) the sums of the batch terms equal the data totals where the data determines them.

Derivable from data: bronze, parse_errors, reread_rows for every table; for orders and clickstream
also ``silver_accepted`` and the remainder. Not derivable: customers' ``silver_accepted``,
``duplicates_removed`` and ``stale_cdc_noop`` (Silver keeps one row per key, the history lives in
``customers_history``, outside this equation) -> NULL on the reconcile row. For clickstream the
remainder mixes duplicates and late events and is reported in ``duplicates_removed``
(``late_beyond_window`` NULL). A DLQ row that ``replay`` recovered moves from the DLQ to Silver
after the batch row was written, so orders and clickstream compare the pooled
``silver_accepted + dlq`` (stable under replay) instead of the two terms. A clickstream re-read
across micro-batches counts as a duplicate in the batch row (D-079): such runs may mismatch
legitimately. After a crash re-execution the term split of a batch may differ from a clean run;
each batch still balances, and the data-derived totals are the authority (D-083).

Writes ``scope='reconcile'`` rows (MERGE on ``run_id, source_table, brand_id``; a NULL or
out-of-contract brand is stored as ``__null__`` / ``__invalid__``). Logs ``reconcile.mismatch`` per
failed check (counts and codes only). The manifest comparison lives in the integration test.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lakehouse.common.brand_key import brand_key
from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import CATALOG_TABLES, read_delta, resolve, table_exists
from lakehouse.ops.metrics import (
    SCOPE_BATCH,
    SCOPE_RECONCILE,
    TERMS,
    is_balanced,
    merge_rows,
    metrics_path,
)
from lakehouse.quality.dlq import REPLAYED


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping, Sequence

    from pyspark.sql import DataFrame, SparkSession

    from lakehouse.common.jobs import JobContext

_log = get_logger(__name__)
_MAX_LOGGED_VIOLATIONS = 20
BATCH_COLUMNS = (
    "job",
    "source_table",
    "query_id",
    "batch_id",
    "rows_read",
    "reread_rows",
    "parse_errors",
    "bronze",
    *TERMS,
    "balanced",
)


class ReconciliationMismatchError(RuntimeError):
    """At least one reconciliation check failed."""


@dataclass(frozen=True)
class Source:
    """A Bronze table with its DLQ table and, when Silver rows determine it, its Silver table."""

    bronze: str
    dlq: str
    silver: str | None


SOURCES = (
    Source("raw_customers", "dlq_customers", None),
    Source("raw_clickstream", "dlq_clickstream", "clickstream"),
    Source("raw_orders", "dlq_orders", "orders"),
)


# ---- pure logic (no Spark) ---------------------------------------------------------------------


def batch_violations(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Check (a): batch rows whose terms do not add up to ``bronze`` or that are not balanced."""
    problems: list[dict[str, Any]] = []
    for r in rows:
        derived = (
            int(r["rows_read"] or 0) - int(r["reread_rows"] or 0) - int(r["parse_errors"] or 0)
        )
        reasons = []
        if not is_balanced(r):
            reasons.append("terms_do_not_sum_to_bronze")
        if r["balanced"] is not True:
            reasons.append("balanced_flag_not_true")
        if derived != int(r["bronze"] or 0):
            reasons.append("bronze_not_rows_minus_reread_minus_parse")
        if reasons:
            problems.append(
                {
                    "batch_job": r["job"],
                    "batch_id": r["batch_id"],
                    "bronze": int(r["bronze"] or 0),
                    "reasons": reasons,
                }
            )
    return problems


def data_rows(
    source: Source, totals: Mapping[str, Mapping[str, int]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Check (b): reconcile rows per brand from data totals, plus the problems found.

    ``totals[brand]`` holds ``rows``, ``bronze``, ``parse_errors``, ``dlq`` and, when the Silver
    table determines it, ``silver``. The row's ``balanced`` is provisional (data level only).
    """
    rows: list[dict[str, Any]] = []
    problems: list[dict[str, Any]] = []
    for brand in sorted(totals):
        t = totals[brand]
        bronze, dlq = int(t["bronze"]), int(t["dlq"])
        reread = int(t["rows"]) - bronze - int(t["parse_errors"])
        silver: int | None = None
        remainder: int | None = None
        if source.silver is not None:
            silver = int(t["silver"])
            remainder = bronze - silver - dlq
            ok = remainder >= 0
        else:
            ok = dlq <= bronze
        if not ok:
            problems.append(
                {
                    "check": "data_remainder_negative",
                    "source_table": source.bronze,
                    "brand_id": brand,
                    "bronze": bronze,
                    "silver_accepted": silver,
                    "dlq": dlq,
                }
            )
        rows.append(
            {
                "job": "reconcile",
                "source_table": source.bronze,
                "query_id": None,
                "batch_id": None,
                "scope": SCOPE_RECONCILE,
                "brand_id": brand,
                "rows_read": int(t["rows"]),
                "reread_rows": reread,
                "parse_errors": int(t["parse_errors"]),
                "bronze": bronze,
                "silver_accepted": silver,
                "dlq": dlq,
                "duplicates_removed": remainder,
                "stale_cdc_noop": None if source.silver is None else 0,
                "late_beyond_window": 0 if source.bronze == "raw_orders" else None,
                "balanced": ok,
            }
        )
    return rows, problems


def compare_totals(
    source: Source, data: Sequence[Mapping[str, Any]], batch: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Check (c): the sums of the batch terms against the data totals of one source table."""

    def total(rows: Sequence[Mapping[str, Any]], *fields: str) -> int:
        return sum(int(r[f] or 0) for r in rows for f in fields)

    pairs = {
        "bronze": (total(data, "bronze"), total(batch, "bronze")),
        "parse_errors": (total(data, "parse_errors"), total(batch, "parse_errors")),
        "reread_rows": (total(data, "reread_rows"), total(batch, "reread_rows")),
    }
    if source.silver is not None:
        pairs["silver_accepted+dlq"] = (
            total(data, "silver_accepted", "dlq"),
            total(batch, "silver_accepted", "dlq"),
        )
        pairs["duplicates_removed+stale_cdc_noop+late_beyond_window"] = (
            total(data, "duplicates_removed"),
            total(batch, "duplicates_removed", "stale_cdc_noop", "late_beyond_window"),
        )
    else:  # customers: Silver rows do not determine the other terms
        pairs["dlq"] = (total(data, "dlq"), total(batch, "dlq"))
    return [
        {
            "check": "batch_sum_vs_data",
            "source_table": source.bronze,
            "term": term,
            "data": expected,
            "batch_rows_sum": actual,
        }
        for term, (expected, actual) in pairs.items()
        if expected != actual
    ]


# ---- Spark reads -------------------------------------------------------------------------------


def _read(
    spark: SparkSession, ctx: JobContext, name: str, namespace: str | None
) -> DataFrame | None:
    table = resolve(ctx.settings, CATALOG_TABLES[name], namespace)
    return read_delta(spark, table) if table_exists(spark, table) else None


def _bronze_totals(df: DataFrame) -> dict[str, dict[str, int]]:
    from pyspark.sql import functions as F

    ref = F.col("_source_ref")
    parsed = F.col("_parse_error").isNull()
    out = df.groupBy(brand_key().alias("_b")).agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(F.when(parsed, ref)).alias("bronze"),
        F.countDistinct(F.when(~parsed, ref)).alias("parse_errors"),
    )
    return {
        str(r["_b"]): {k: int(r[k]) for k in ("rows", "bronze", "parse_errors")}
        for r in out.collect()
    }


def _dlq_totals(df: DataFrame | None) -> dict[str, int]:
    if df is None:
        return {}
    from pyspark.sql import functions as F

    pending = F.coalesce(F.col("_replay_status"), F.lit("")) != REPLAYED
    out = (
        df.where(pending)
        .groupBy(brand_key().alias("_b"))
        .agg(F.countDistinct("_bronze_ref").alias("n"))
    )
    return {str(r["_b"]): int(r["n"]) for r in out.collect()}


def _silver_totals(df: DataFrame | None) -> dict[str, int]:
    if df is None:
        return {}
    from pyspark.sql import functions as F

    out = df.groupBy(brand_key().alias("_b")).agg(F.count(F.lit(1)).alias("n"))
    return {str(r["_b"]): int(r["n"]) for r in out.collect()}


def _source_totals(
    spark: SparkSession, ctx: JobContext, source: Source, namespace: str | None
) -> dict[str, dict[str, int]]:
    """``{brand: {rows, bronze, parse_errors, dlq[, silver]}}`` (empty: no Bronze table)."""
    bronze_df = _read(spark, ctx, source.bronze, namespace)
    if bronze_df is None:
        return {}
    bronze = _bronze_totals(bronze_df)
    dlq = _dlq_totals(_read(spark, ctx, source.dlq, namespace))
    silver = _silver_totals(_read(spark, ctx, source.silver, namespace)) if source.silver else None
    brands = set(bronze) | set(dlq) | set(silver or {})
    empty = {"rows": 0, "bronze": 0, "parse_errors": 0}
    totals: dict[str, dict[str, int]] = {}
    for brand in brands:
        entry = {**empty, **bronze.get(brand, {}), "dlq": dlq.get(brand, 0)}
        if silver is not None:
            entry["silver"] = silver.get(brand, 0)
        totals[brand] = entry
    return totals


def _batch_rows(spark: SparkSession, path: str) -> list[dict[str, Any]]:
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        return []
    df = spark.read.format("delta").load(path).where(f"scope = '{SCOPE_BATCH}'")
    return [r.asDict() for r in df.select(*BATCH_COLUMNS).collect()]


class ReconcileJob(SparkJob):
    """Reconcile Bronze, DLQ and Silver against the recorded batch metrics."""

    name = "reconcile"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--namespace", default=None, help="optional single path segment")

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        namespace: str | None = args.namespace
        path = metrics_path(ctx.settings, namespace)
        batch = _batch_rows(spark, path)
        violations = batch_violations(batch)
        problems: list[dict[str, Any]] = [
            {"check": "batch_row_unbalanced", **v} for v in violations[:_MAX_LOGGED_VIOLATIONS]
        ]
        if len(violations) > _MAX_LOGGED_VIOLATIONS:
            problems.append({"check": "batch_row_unbalanced", "reasons": ["more_not_listed"]})
        batch_failed = bool(problems)

        recorded_at = datetime.now(UTC).replace(tzinfo=None)
        all_rows: list[dict[str, Any]] = []
        totals: dict[str, dict[str, int]] = {}
        for source in SOURCES:
            data, data_problems = data_rows(source, _source_totals(spark, ctx, source, namespace))
            mine = [r for r in batch if r["source_table"] == source.bronze]
            table_problems = [*data_problems, *compare_totals(source, data, mine)]
            problems.extend(table_problems)
            for row in data:
                row["run_id"] = ctx.run_id
                row["recorded_at"] = recorded_at
                row["balanced"] = bool(row["balanced"]) and not table_problems and not batch_failed
            all_rows.extend(data)
            totals[source.bronze] = {
                term: sum(int(r[term] or 0) for r in data)
                for term in ("bronze", "parse_errors", "reread_rows", "silver_accepted", "dlq")
            }

        merge_rows(spark, path, all_rows, SCOPE_RECONCILE)
        for problem in problems:
            _log.error("reconcile.mismatch", **problem)
        if problems:
            raise ReconciliationMismatchError(
                f"{len(problems)} reconciliation check(s) failed; see reconcile.mismatch logs"
            )
        return {"batch_rows": len(batch), "reconcile_rows": len(all_rows), "totals": totals}


JOB = ReconcileJob
