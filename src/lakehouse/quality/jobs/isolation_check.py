"""Tenant isolation check over Silver (W3.D18, D-085).

Run as ``python scripts/submit_job.py isolation_check [--namespace <segment>] --expected-json
<json>`` where the JSON is ``{"customers": {"<brand_id>": <rows>}, "orders": {...},
"order_items": {...}}``. For every table in the JSON and every brand listed, the job creates a
*temporary* view (``tenant_view_sql(temp=True)``; nothing is registered) and checks that:

* the view holds only its own brand (no ``brand_id`` other than the view's);
* the view count equals the expected count of that brand;
* the view counts add up to the table count (no row outside the listed brands);
* a join on ``brand_id`` + ``customer_id`` produces no cross-tenant pair;
* the detector works: a deliberate join on ``customer_id`` only, over crafted colliding ids, is
  flagged (``CrossTenantLeakError``). The real data's own leaky join is reported as
  ``collision_pairs`` (informational; it is > 0 when the generator planted id collisions).

Any failed check is logged and fails the job (exit 1). Out of scope: row-level security, IAM.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import (
    CATALOG_TABLES,
    read_delta,
    resolve,
    table_exists,
    verify_location,
)
from lakehouse.quality.isolation import (
    CrossTenantLeakError,
    assert_no_cross_tenant_rows,
    brand_counts,
)
from lakehouse.silver.views import tenant_view_sql


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import DataFrame, SparkSession

    from lakehouse.common.jobs import JobContext

TABLES = ("customers", "orders", "order_items")
_log = get_logger(__name__)


class IsolationError(RuntimeError):
    """A tenant isolation check failed."""


def _expected_arg(value: str) -> dict[str, dict[str, int]]:
    import argparse as _argparse

    try:
        data = json.loads(value)
    except ValueError as exc:
        raise _argparse.ArgumentTypeError(f"--expected-json is not valid JSON: {exc}") from exc
    ok = isinstance(data, dict) and bool(data)
    if ok:
        for table, counts in data.items():
            ok = ok and table in TABLES and isinstance(counts, dict)
            ok = ok and all(
                isinstance(b, str) and isinstance(n, int) and not isinstance(n, bool) and n >= 0
                for b, n in counts.items()
            )
    if not ok:
        raise _argparse.ArgumentTypeError(
            f"--expected-json must be {{table: {{brand: count}}}} with tables in {list(TABLES)}"
        )
    return dict(data)


def _source_view(spark: SparkSession, ctx: JobContext, table: str, ns: str | None) -> str | None:
    """Make ``silver.<table>`` readable: named runs use the catalog, path runs a temp source."""
    spec = CATALOG_TABLES[table]
    res = resolve(ctx.settings, spec, ns)
    if not table_exists(spark, res):
        raise IsolationError(f"{spec.qualified_name} does not exist; run silver jobs first")
    verify_location(spark, spec, res)
    if res.name is not None:
        return None
    source = f"src_{table}"
    read_delta(spark, res).createOrReplaceTempView(source)
    return source


def _crafted_probe(spark: SparkSession) -> bool:
    """True when the detector flags a customer_id-only join over two colliding brands."""
    schema = "brand_id string, customer_id string"
    left = spark.createDataFrame([("brand_x", "c1"), ("brand_y", "c1")], schema)
    right = spark.createDataFrame([("brand_x", "c1"), ("brand_y", "c1")], schema)
    leaky = left.join(right, left["customer_id"] == right["customer_id"]).select(
        left["brand_id"], right["brand_id"].alias("right_brand_id")
    )
    try:
        assert_no_cross_tenant_rows(leaky, "brand_x", "brand_y")
    except CrossTenantLeakError:
        return True
    return False


def _self_join(customers: DataFrame, *, composite: bool) -> DataFrame:
    """Self-join on ``customer_id`` (plus ``brand_id`` when ``composite``).

    Columns are addressed by alias name (``l.x`` / ``r.x``): ``left["x"]`` on two aliases of the
    same DataFrame resolves to the same attribute and Spark rejects it as an ambiguous self-join.
    """
    from pyspark.sql import functions as F

    left, right = customers.alias("l"), customers.alias("r")
    cond = F.col("l.customer_id") == F.col("r.customer_id")
    if composite:
        cond = cond & (F.col("l.brand_id") == F.col("r.brand_id"))
    return left.join(right, cond).select(
        F.col("l.brand_id").alias("brand_id"), F.col("r.brand_id").alias("right_brand_id")
    )


def check_table(
    spark: SparkSession, table: str, expected: Mapping[str, int], source: str | None
) -> tuple[dict[str, int], list[str]]:
    """Per-brand view counts and the problems found for one table."""
    problems: list[str] = []
    counts: dict[str, int] = {}
    for brand, want in sorted(expected.items()):
        spark.sql(tenant_view_sql(table, brand, temp=True, source=source))
        got = brand_counts(spark.table(f"v_{table}__{brand}"))
        foreign = sorted(set(got) - {brand})
        if foreign:
            problems.append(f"{table}/{brand}: view holds rows of other brands {foreign}")
        counts[brand] = got.get(brand, 0)
        if counts[brand] != want:
            problems.append(f"{table}/{brand}: view count {counts[brand]} != expected {want}")
    total = spark.table(source or f"silver.{table}").count()
    if sum(counts.values()) != total:
        problems.append(f"{table}: view counts sum to {sum(counts.values())}, table has {total}")
    return counts, problems


class IsolationCheckJob(SparkJob):
    """Verify tenant isolation of the Silver tables."""

    name = "isolation_check"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--namespace", default=None, help="optional single path segment")
        parser.add_argument(
            "--expected-json",
            required=True,
            type=_expected_arg,
            help='{"<table>": {"<brand_id>": <row count>}} for customers/orders/order_items',
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        ns: str | None = args.namespace
        expected: dict[str, dict[str, int]] = args.expected_json
        problems: list[str] = []
        report: dict[str, Any] = {}
        sources = {t: _source_view(spark, ctx, t, ns) for t in expected}
        for table, want in expected.items():
            counts, found = check_table(spark, table, want, sources[table])
            report[table] = counts
            problems += found

        if "customers" in expected:
            customers = spark.table(sources["customers"] or "silver.customers")
            brands = sorted(brand_counts(customers))
            pair = (brands + brands)[:2] if brands else ["", ""]
            try:
                assert_no_cross_tenant_rows(_self_join(customers, composite=True), *pair)
            except CrossTenantLeakError as exc:
                problems.append(f"composite join leaks: {exc}")
            report["collision_pairs"] = (
                _self_join(customers, composite=False)
                .filter("NOT (brand_id <=> right_brand_id)")
                .count()
            )
        if not _crafted_probe(spark):
            problems.append("leak detector did not flag a crafted customer_id-only join")

        _log.info("isolation_check.done", problems=len(problems), tables=sorted(expected))
        for problem in problems:
            _log.error("isolation_check.problem", problem=problem)
        if problems:
            raise IsolationError("; ".join(problems))
        return {"counts": report, "problems": 0}


JOB = IsolationCheckJob
