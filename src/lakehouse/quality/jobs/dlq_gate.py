"""Batch contract gate: Bronze rows that violate their contract go to the dead-letter tables.

Run as ``python scripts/submit_job.py dlq_gate [--namespace <segment>] [--table <name>]...``
(default tables: ``raw_customers`` and ``raw_clickstream``).

The job reads a whole Bronze table (one Delta snapshot, not a stream) and splits its rows:

* ``_parse_error`` set: stays in Bronze only, counted as ``parse_errors`` (parse failures are not
  contract failures);
* a row failing any rule of :mod:`lakehouse.quality.rules`: becomes a DLQ row;
* otherwise: ``valid_rows`` (Silver picks those up later; this job does not copy them).

DLQ rows carry codes and counts only, never payload values (PII). ``_bronze_ref`` is the Bronze
``_source_ref`` (``topic/partition/offset``). ``_dlq_id`` = sha256 of
``coalesce(brand_id,'') | source_table | _source_ref``, so one Kafka message maps to one DLQ id.
A lost checkpoint re-reads the same offsets into Bronze with a new ``_batch_id`` (same
``_source_ref``); the gate therefore deduplicates its source on ``_dlq_id`` before the MERGE, and
the MERGE (``ON _dlq_id AND brand_id <=> brand_id``, insert only) makes a rerun insert nothing.
That is an idempotent outcome of at-least-once Bronze ingestion, not exactly-once delivery.

Checks (per table, all after the MERGE; any failure is logged in ``dlq_gate.table`` and then
fails the job, exit 1). They are independent of how the counts were built: (a) the DLQ row count
after minus before equals ``dlq_inserted`` (the MERGE's own metric); (b) every deduplicated
rejected row is present in the DLQ table (anti-join on ``_dlq_id`` and ``brand_id <=>``, 0
``dlq_missing``); (c) ``dlq_inserted <= rejected_rows``. The arithmetic identity ``rows_read ==
parse_errors + valid_rows + rejected_rows + rejected_duplicates`` is only a sanity term (true by
construction of one aggregate). ``rejected_duplicates`` = extra copies of an already rejected
``_dlq_id`` inside this run; ``bronze_duplicate_rows`` = ``rows_read - distinct _source_ref`` over
all of Bronze (after a full checkpoint-loss re-read it equals the number of re-read refs).
``dlq_inserted`` is read from the Delta history row of version ``before + 1`` (0 when the MERGE
committed no new version).

An existing but empty Bronze table is valid (0 rows, the DLQ table is still created with its
schema); a missing Bronze table fails the job with a clear message. The DLQ table is a data store
like Bronze: it keeps the raw ``brand_id`` / ``event_id`` values (possibly invalid or NULL); only
logs and reports sanitise them. The gate rescans all of Bronze on every run (O(n)); fine for D10,
an incremental gate would need a watermark.

Single-writer assumption (as for the Bronze stream, D9): run one gate at a time per namespace.
Concurrent gates are unsupported and untested: Delta may reject one with a concurrent-modification
error, and the ``dlq_inserted`` metric and post-write checks are only meaningful for a single
writer (VERIFICATION_REGISTER V-05).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lakehouse.common.brand_key import brand_key
from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import (
    CATALOG_TABLES,
    ResolvedTable,
    read_delta,
    register_table,
    resolve,
    table_exists,
    verify_location,
)
from lakehouse.quality.rules import RULES_BY_TABLE, apply_rules


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import DataFrame, SparkSession

    from lakehouse.common.jobs import JobContext

DEFAULT_TABLES = tuple(RULES_BY_TABLE)
DLQ_TABLE_BY_BRONZE = {"raw_customers": "dlq_customers", "raw_clickstream": "dlq_clickstream"}
_log = get_logger(__name__)


class ReconciliationError(RuntimeError):
    """A post-write check of the gate failed (counts do not add up or DLQ rows are missing)."""


def _dlq_rows(checked: DataFrame, source_table: str, run_id: str) -> DataFrame:
    """Project rejected rows (non-empty ``_rejection_codes``) onto the DLQ schema."""
    from pyspark.sql import functions as F

    dlq_id = F.sha2(
        F.concat(
            F.coalesce(F.col("brand_id"), F.lit("")),
            F.lit("|"),
            F.lit(source_table),
            F.lit("|"),
            F.col("_source_ref"),
        ),
        256,
    )
    return checked.where(F.size("_rejection_codes") > 0).select(
        dlq_id.alias("_dlq_id"),
        F.col("brand_id").cast("string").alias("brand_id"),
        F.lit(source_table).alias("_source_table"),
        F.col("_source_ref").alias("_bronze_ref"),
        F.col("event_id").cast("string").alias("event_id"),
        F.col("_error_category"),
        F.col("_rejection_reason"),
        F.col("_rejection_codes"),
        F.col("_retryable"),
        F.lit("PENDING").alias("_replay_status"),
        F.lit(0).alias("_replay_count"),
        F.current_timestamp().alias("_rejected_at"),
        F.lit(run_id).alias("_pipeline_run_id"),
    )


def _merge_inserted(spark: SparkSession, path: str, rejected: DataFrame) -> int:
    """Insert-only MERGE of ``rejected`` into the DLQ table at ``path``; rows actually inserted.

    The count comes from the history row of version ``before + 1`` (not "the latest row"), so a
    MERGE that committed nothing reports 0.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    target = DeltaTable.forPath(spark, path)
    version_before = int(target.history(1).collect()[0]["version"])
    (
        target.alias("t")
        .merge(rejected.alias("s"), "t._dlq_id = s._dlq_id AND t.brand_id <=> s.brand_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
    rows = (
        target.history()
        .where(F.col("version") == version_before + 1)
        .select("operation", "operationMetrics")
        .collect()
    )
    if not rows or rows[0]["operation"] != "MERGE":
        return 0
    return int((rows[0]["operationMetrics"] or {}).get("numTargetRowsInserted", 0))


def _breakdowns(rejected: DataFrame) -> tuple[dict[str, int], dict[str, int]]:
    """Rejected rows per rule code and per log-safe tenant key (counts only)."""
    from pyspark.sql import functions as F

    by_code = {
        str(r["code"]): int(r["n"])
        for r in rejected.select(F.explode("_rejection_codes").alias("code"))
        .groupBy("code")
        .agg(F.count(F.lit(1)).alias("n"))
        .collect()
    }
    by_brand = {
        str(r["brand_key"]): int(r["n"])
        for r in rejected.groupBy(brand_key().alias("brand_key"))
        .agg(F.count(F.lit(1)).alias("n"))
        .collect()
    }
    return by_code, by_brand


def post_write_problems(
    *, rows_before: int, rows_after: int, inserted: int, missing: int, rejected_rows: int
) -> list[str]:
    """The independent post-MERGE checks; an empty list means the DLQ write is consistent."""
    problems: list[str] = []
    if rows_after - rows_before != inserted:
        problems.append(
            f"DLQ rows grew by {rows_after - rows_before} but the MERGE reported {inserted}"
        )
    if missing != 0:
        problems.append(f"{missing} rejected rows are not in the DLQ table")
    if inserted > rejected_rows:
        problems.append(f"dlq_inserted={inserted} > rejected_rows={rejected_rows}")
    return problems


def _write_and_check(
    spark: SparkSession, rejected: DataFrame, dlq_path: str
) -> tuple[dict[str, int], list[str]]:
    """MERGE ``rejected`` into the DLQ table, then run the independent post-write checks."""
    from delta.tables import DeltaTable

    rejected_rows = rejected.count()
    if not DeltaTable.isDeltaTable(spark, dlq_path):
        # An empty table with the schema (D-053); a first run must not skip creation.
        rejected.limit(0).write.format("delta").mode("append").save(dlq_path)
    rows_before = spark.read.format("delta").load(dlq_path).count()
    inserted = _merge_inserted(spark, dlq_path, rejected) if rejected_rows else 0
    after = spark.read.format("delta").load(dlq_path)
    rows_after = after.count()
    missing = rejected.join(
        after,
        (rejected["_dlq_id"] == after["_dlq_id"])
        & rejected["brand_id"].eqNullSafe(after["brand_id"]),
        "left_anti",
    ).count()
    problems = post_write_problems(
        rows_before=rows_before,
        rows_after=rows_after,
        inserted=inserted,
        missing=missing,
        rejected_rows=rejected_rows,
    )
    counts = {
        "rejected_rows": rejected_rows,
        "dlq_inserted": inserted,
        "dlq_rows_before": rows_before,
        "dlq_rows_after": rows_after,
        "dlq_missing": missing,
    }
    return counts, problems


def _gate_table(
    spark: SparkSession, *, table: str, bronze: ResolvedTable, dlq_path: str, run_id: str
) -> tuple[dict[str, Any], list[str]]:
    """Gate one Bronze table; returns the log record and the failed-check messages."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    source_table = f"bronze.{table}"
    checked = apply_rules(read_delta(spark, bronze), RULES_BY_TABLE[table]())
    failing = F.size("_rejection_codes") > 0
    totals = checked.agg(
        F.count(F.lit(1)).alias("rows_read"),
        F.count(F.when(F.col("_parse_error").isNotNull(), 1)).alias("parse_errors"),
        F.count(F.when(F.col("_parse_error").isNull() & ~failing, 1)).alias("valid"),
        F.count(F.when(F.col("_parse_error").isNull() & failing, 1)).alias("rejected"),
        F.count(F.when(F.col("_source_ref").isNull(), 1)).alias("null_refs"),
        F.countDistinct("_source_ref").alias("distinct_refs"),
    ).first()
    if totals is None:  # an aggregate without grouping always returns one row
        raise RuntimeError(f"no aggregate row for {bronze.location}")
    if int(totals["null_refs"]) > 0:
        raise ReconciliationError(
            f"{source_table}: {int(totals['null_refs'])} rows without _source_ref; "
            "cannot build a DLQ id"
        )
    rows_read = int(totals["rows_read"])
    rejected = (
        _dlq_rows(checked, source_table, run_id)
        .dropDuplicates(["_dlq_id"])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    try:
        counts, problems = _write_and_check(spark, rejected, dlq_path)
        by_code, by_brand = _breakdowns(rejected)
    finally:
        rejected.unpersist()

    rejected_duplicates = int(totals["rejected"]) - counts["rejected_rows"]
    parts = int(totals["parse_errors"]) + int(totals["valid"]) + int(totals["rejected"])
    if parts != rows_read:  # sanity term only: true by construction of the aggregate
        problems.append(f"rows_read={rows_read} != parse_errors + valid_rows + rejected ({parts})")
    record = {
        "dlq_path": dlq_path,
        "rows_read": rows_read,
        "parse_errors": int(totals["parse_errors"]),
        "valid_rows": int(totals["valid"]),
        "rejected_rows": counts["rejected_rows"],
        "rejected_duplicates": rejected_duplicates,
        "dlq_inserted": counts["dlq_inserted"],
        "dlq_rows_before": counts["dlq_rows_before"],
        "dlq_rows_after": counts["dlq_rows_after"],
        "dlq_missing": counts["dlq_missing"],
        "bronze_duplicate_rows": rows_read - int(totals["distinct_refs"]),
        "by_code": by_code,
        "by_brand": by_brand,
    }
    return record, problems


class DlqGateJob(SparkJob):
    """Gate one or more Bronze tables into their DLQ tables."""

    name = "dlq_gate"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--namespace", default=None, help="optional single path segment")
        parser.add_argument(
            "--table",
            action="append",
            default=None,
            choices=sorted(RULES_BY_TABLE),
            help="Bronze table name (repeatable; default: raw_customers and raw_clickstream)",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        tables: list[str] = list(dict.fromkeys(args.table or DEFAULT_TABLES))
        sources = {t: resolve(ctx.settings, CATALOG_TABLES[t], args.namespace) for t in tables}
        for source in sources.values():  # fail before writing anything
            if not table_exists(spark, source):
                if source.name is not None:
                    raise RuntimeError(
                        f"Bronze table {source.name} is not registered in the catalog; "
                        "run the register_tables job (or the stream never ran for it)"
                    )
                raise RuntimeError(f"no Bronze table at {source.location}; stream never ran for it")
        for table, source in sources.items():  # a named read must hit the configured location
            verify_location(spark, CATALOG_TABLES[table], source)

        report: dict[str, Any] = {}
        for table in tables:
            dlq_spec = CATALOG_TABLES[DLQ_TABLE_BY_BRONZE[table]]
            dlq = resolve(ctx.settings, dlq_spec, args.namespace)
            record, problems = _gate_table(
                spark,
                table=table,
                bronze=sources[table],
                dlq_path=dlq.location,
                run_id=ctx.run_id,
            )
            if dlq.name is not None:  # the DLQ table now exists; make it visible in the catalog
                record["dlq_catalog"] = register_table(spark, dlq_spec, dlq)
            report[table] = record
            _log.info("dlq_gate.table", table=table, **record)
            if problems:
                raise ReconciliationError(f"bronze.{table}: " + "; ".join(problems))
        return {"tables": report}


JOB = DlqGateJob
