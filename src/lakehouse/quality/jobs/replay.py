"""DLQ replay: re-check retryable foreign-key orphans of ``orders`` against the current Silver.

Run as ``python scripts/submit_job.py replay --table orders [--brand <brand_id>]
[--namespace <segment>] [--max-replays 3]``.

An order whose customer (or product) had not arrived when ``silver_orders`` ran sits in
``dead_letter.dlq_orders`` as ``_retryable`` with ``_replay_status`` ``PENDING``. The replay:

1. selects DLQ rows of ``bronze.raw_orders`` that are retryable, ``PENDING`` or ``FAILED_AGAIN``,
   with ``_replay_count < --max-replays`` (optionally one brand);
2. joins them back to Bronze on ``brand_id`` and ``_bronze_ref = _source_ref`` (one Bronze row per
   ref, so a checkpoint-loss re-read does not multiply a row);
3. re-checks the foreign keys against the current ``silver.customers`` and the valid
   ``bronze.raw_products`` (``silver.orders.recheck_fk``);
4. writes the recovered orders and their items with the insert-only Silver merge;
5. only then marks the DLQ rows: recovered -> ``REPLAYED``, the rest -> ``FAILED_AGAIN``, and
   ``_replay_count + 1`` for all of them.

Silver is written before the DLQ status. A crash in between leaves the DLQ rows ``PENDING``; the
rerun recovers the same orders, the insert-only merge adds nothing and the status is then fixed.
A second run selects no ``REPLAYED`` row, so it changes neither Silver nor the DLQ counts: an
idempotent outcome, not an exactly-once claim. Single writer per namespace is assumed (as for the
other batch jobs). Logs carry counts only; a DLQ row never holds payload values.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import (
    CATALOG_TABLES,
    delta_exists_at_location,
    read_delta,
    resolve,
    table_exists,
    verify_location,
)
from lakehouse.contracts.models import BRAND_ID_PATTERN
from lakehouse.quality.dlq import (
    DLQ_TABLE_BY_BRONZE,
    FAILED_AGAIN,
    PENDING,
    REPLAYED,
    set_replay_outcome,
)
from lakehouse.silver.orders import merge_orders, recheck_fk


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import DataFrame, SparkSession

    from lakehouse.common.jobs import JobContext

_log = get_logger(__name__)
# CLI table name -> the Bronze table whose DLQ rows are replayed. Only orders have retryable rows.
REPLAY_SOURCES = {"orders": "raw_orders"}
DEFAULT_MAX_REPLAYS = 3


class ReplayError(RuntimeError):
    """A replay precondition or post-write check failed."""


def _brand_arg(value: str) -> str:
    import argparse as _argparse

    if not re.fullmatch(BRAND_ID_PATTERN.replace("$", r"\Z"), value):
        raise _argparse.ArgumentTypeError(f"{value!r} is not a valid brand_id")
    return value


def _positive_int(value: str) -> int:
    import argparse as _argparse

    try:
        number = int(value)
    except ValueError:
        raise _argparse.ArgumentTypeError(f"{value!r} is not an integer") from None
    if number < 1:
        raise _argparse.ArgumentTypeError(f"must be >= 1, got {number}")
    return number


def select_candidates(
    dlq: DataFrame, source_table: str, max_replays: int, brand: str | None
) -> DataFrame:
    """Retryable DLQ rows still open for a replay: ``_dlq_id``, ``brand_id``, ``_bronze_ref``."""
    from pyspark.sql import functions as F

    open_rows = (
        (F.col("_source_table") == source_table)
        & F.col("_retryable")
        & F.col("_replay_status").isin(PENDING, FAILED_AGAIN)
        & (F.col("_replay_count") < max_replays)
    )
    if brand is not None:
        open_rows = open_rows & (F.col("brand_id") == brand)
    return (
        dlq.where(open_rows)
        .select("_dlq_id", "brand_id", "_bronze_ref")
        .dropDuplicates(["_dlq_id", "brand_id"])
    )


def replay_orders(
    spark: SparkSession,
    *,
    bronze: DataFrame,
    products: DataFrame,
    customers: DataFrame,
    dlq: DataFrame,
    dlq_path: str,
    orders_path: str,
    items_path: str,
    max_replays: int,
    brand: str | None,
) -> dict[str, int]:
    """One replay pass over the open retryable order orphans; returns the counts."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    source_table = f"bronze.{REPLAY_SOURCES['orders']}"
    candidates = select_candidates(dlq, source_table, max_replays, brand).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        n_candidates = candidates.count()
        if n_candidates == 0:
            return {
                "candidates": 0,
                "bronze_found": 0,
                "recovered": 0,
                "still_orphan": 0,
                "orders_inserted": 0,
                "items_inserted": 0,
                "replayed": 0,
                "failed_again": 0,
                "dlq_updated": 0,
            }
        bronze_rows = (
            bronze.alias("b")
            .join(
                candidates.alias("c"),
                (F.col("b.brand_id") == F.col("c.brand_id"))
                & (F.col("b._source_ref") == F.col("c._bronze_ref")),
                "left_semi",
            )
            .dropDuplicates(["brand_id", "_source_ref"])
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        try:
            bronze_found = bronze_rows.count()
            recovered, still_orphan = recheck_fk(bronze_rows, customers, products)
            recovered = recovered.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                n_recovered = recovered.count()
                n_orphan = still_orphan.count()
                # Silver first: a crash before the DLQ update is repaired by the rerun.
                orders_inserted, items_inserted = (
                    merge_orders(spark, orders_path, items_path, recovered)
                    if n_recovered
                    else (0, 0)
                )
                recovered_refs = (
                    recovered.select("brand_id", F.col("_source_ref").alias("_bronze_ref"))
                    .dropDuplicates()
                    .withColumn("_recovered", F.lit(True))
                )
                outcomes = (
                    candidates.join(recovered_refs, ["brand_id", "_bronze_ref"], "left")
                    .select(
                        "_dlq_id",
                        "brand_id",
                        F.when(F.col("_recovered"), F.lit(REPLAYED))
                        .otherwise(F.lit(FAILED_AGAIN))
                        .alias("_replay_status"),
                    )
                    .persist(StorageLevel.MEMORY_AND_DISK)
                )
                try:
                    status = {
                        str(r["_replay_status"]): int(r["n"])
                        for r in outcomes.groupBy("_replay_status")
                        .agg(F.count(F.lit(1)).alias("n"))
                        .collect()
                    }
                    dlq_updated = set_replay_outcome(spark, dlq_path, outcomes)
                finally:
                    outcomes.unpersist()
            finally:
                recovered.unpersist()
        finally:
            bronze_rows.unpersist()
    finally:
        candidates.unpersist()

    counts = {
        "candidates": n_candidates,
        "bronze_found": bronze_found,
        "recovered": n_recovered,
        "still_orphan": n_orphan,
        "orders_inserted": orders_inserted,
        "items_inserted": items_inserted,
        "replayed": status.get(REPLAYED, 0),
        "failed_again": status.get(FAILED_AGAIN, 0),
        "dlq_updated": dlq_updated,
    }
    problems: list[str] = []
    if counts["replayed"] + counts["failed_again"] != n_candidates:
        problems.append("outcome rows do not add up to the candidates")
    if dlq_updated != n_candidates:
        problems.append(f"{n_candidates} candidates but the DLQ MERGE updated {dlq_updated}")
    if n_recovered + n_orphan != bronze_found:
        problems.append(
            f"recovered + still_orphan ({n_recovered + n_orphan}) != bronze_found ({bronze_found})"
        )
    if problems:
        raise ReplayError("; ".join(problems) + f" {counts}")
    return counts


class ReplayJob(SparkJob):
    """Replay the retryable DLQ rows of one table."""

    name = "replay"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--namespace", default=None, help="optional single path segment")
        parser.add_argument(
            "--table", required=True, choices=sorted(REPLAY_SOURCES), help="entity to replay"
        )
        parser.add_argument("--brand", type=_brand_arg, default=None, help="only this brand_id")
        parser.add_argument(
            "--max-replays",
            type=_positive_int,
            default=DEFAULT_MAX_REPLAYS,
            help="replay attempts per DLQ row before it is left alone (>= 1)",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        ns: str | None = args.namespace
        bronze_name = REPLAY_SOURCES[args.table]
        specs = {
            "bronze": CATALOG_TABLES[bronze_name],
            "products": CATALOG_TABLES["raw_products"],
            "customers": CATALOG_TABLES["customers"],
            "orders": CATALOG_TABLES["orders"],
            "items": CATALOG_TABLES["order_items"],
            "dlq": CATALOG_TABLES[DLQ_TABLE_BY_BRONZE[bronze_name]],
        }
        res = {key: resolve(ctx.settings, spec, ns) for key, spec in specs.items()}
        for key in ("bronze", "products", "customers"):  # reads: by name or by location
            if not table_exists(spark, res[key]):
                raise ReplayError(f"{specs[key].qualified_name} does not exist; run its job first")
            verify_location(spark, specs[key], res[key])
        for key in ("orders", "items", "dlq"):  # writes: always by location
            if not delta_exists_at_location(spark, res[key]):
                raise ReplayError(
                    f"no Delta table at {res[key].location} ({specs[key].qualified_name}); "
                    "run silver_orders first"
                )

        counts = replay_orders(
            spark,
            bronze=read_delta(spark, res["bronze"]),
            products=read_delta(spark, res["products"]),
            customers=read_delta(spark, res["customers"]),
            dlq=spark.read.format("delta").load(res["dlq"].location),
            dlq_path=res["dlq"].location,
            orders_path=res["orders"].location,
            items_path=res["items"].location,
            max_replays=args.max_replays,
            brand=args.brand,
        )
        _log.info("replay.done", table=args.table, max_replays=args.max_replays, **counts)
        return {"table": args.table, "max_replays": args.max_replays, "counts": counts}


JOB = ReplayJob
