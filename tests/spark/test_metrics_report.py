"""``metrics_report`` job (W5.D30) on tmp Delta tables, from the design.

``SparkSettings.layer_uri`` is patched to point at ``tmp_path`` so the job runs path-addressed
(namespace ``t``) without S3. ``ops.pipeline_metrics`` is filled through the real writers
(``record_batch`` / ``merge_rows``); Bronze tables carry only ``brand_id``, ``_ingest_ts`` and,
for Kafka-sourced ones, ``_kafka_ts``. The report is written with ``--output-uri`` to a tmp folder
and read back. Pure rules (rates, NULLs, sorting, markdown) are unit-tested in
``tests/unit/ops/test_report.py``. Runs inside the Spark image.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest


pytest.importorskip("pyspark")

from lakehouse.common.jobs import JobContext  # noqa: E402
from lakehouse.common.spark import SparkSettings  # noqa: E402
from lakehouse.ops.jobs.metrics_report import JOB  # noqa: E402
from lakehouse.ops.metrics import SCOPE_RECONCILE, batch_row, merge_rows, record_batch  # noqa: E402


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession

A, B = "brand_alpha", "brand_beta"
NS = "t"
RUN = "metrics_report_run1"
OLD = datetime(2026, 11, 13, 10, 0, 0)
NEW = datetime(2026, 11, 14, 10, 0, 0)


@pytest.fixture
def root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    def layer_uri(_self: SparkSettings, layer: str, *parts: str) -> str:
        return (tmp_path / layer / "/".join(parts)).as_posix()

    monkeypatch.setattr(SparkSettings, "layer_uri", layer_uri)
    return tmp_path


def _metrics_path(root: Path) -> str:
    return (root / "ops" / NS / "pipeline_metrics").as_posix()


def _reconcile(brand: str, run_id: str, at: datetime, **over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "job": "reconcile",
        "source_table": "raw_orders",
        "scope": SCOPE_RECONCILE,
        "brand_id": brand,
        "run_id": run_id,
        "rows_read": 10,
        "reread_rows": 0,
        "parse_errors": 0,
        "bronze": 10,
        "silver_accepted": 8,
        "dlq": 2,
        "duplicates_removed": 0,
        "stale_cdc_noop": None,
        "late_beyond_window": 0,
        "balanced": True,
        "recorded_at": at,
    }
    return {**base, **over}


def _batch(batch_id: int, **counts: int) -> dict[str, Any]:
    full = {"rows_read": 10, "reread_rows": 0, "parse_excluded": 0, "silver_accepted": 8}
    full |= {"dlq_excluded": 2, **counts}
    return batch_row(
        job="silver_orders",
        source_table="raw_orders",
        query_id="q1",
        batch_id=batch_id,
        run_id="r-batch",
        counts=full,
        recorded_at=NEW,
    )


def _bronze(spark: SparkSession, root: Path, name: str, rows: list[tuple[Any, ...]], schema: str):
    path = root / "bronze" / NS / name
    path.parent.mkdir(parents=True, exist_ok=True)
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").save(
        path.as_posix()
    )


def _run(spark: SparkSession, root: Path, *argv: str) -> tuple[dict[str, Any], dict[str, Any]]:
    parser = argparse.ArgumentParser()
    job = JOB()
    job.add_arguments(parser)
    args = parser.parse_args(
        ["--namespace", NS, "--output-uri", (root / "reports").as_posix(), *argv]
    )
    ctx = JobContext("metrics_report", RUN, SparkSettings(master="local[2]"))
    result = dict(job.run(spark, args, ctx))
    report = json.loads((root / "reports" / RUN / "report.json").read_text(encoding="utf-8"))
    return result, report


def test_report_uses_the_latest_reconcile_row_and_sums_batches(spark: SparkSession, root: Path):
    path = _metrics_path(root)
    merge_rows(spark, path, [_reconcile(A, "r-old", OLD, dlq=9, balanced=False)], SCOPE_RECONCILE)
    merge_rows(
        spark,
        path,
        [_reconcile(A, "r-new", NEW), _reconcile(B, "r-new", NEW, dlq=0, balanced=False)],
        SCOPE_RECONCILE,
    )
    record_batch(spark, path, _batch(0))
    record_batch(spark, path, _batch(1, silver_accepted=7, dlq_excluded=3))
    _bronze(spark, root, "raw_orders", [(A, NEW)], "brand_id string, _ingest_ts timestamp")

    result, report = _run(spark, root, "--table", "raw_orders")

    by_brand = {r["brand_id"]: r for r in report["rows"]}
    assert (by_brand[A]["dlq"], by_brand[A]["reconcile"]) == (2, "OK")  # r-new, not r-old
    assert by_brand[A]["dlq_rate"] == pytest.approx(0.2)
    assert (by_brand[B]["dlq"], by_brand[B]["reconcile"]) == (0, "MISMATCH")
    assert by_brand[B]["dlq_rate"] == 0
    (totals,) = report["batch_totals"]
    assert (totals["source_table"], totals["batches"], totals["bronze"]) == ("raw_orders", 2, 20)
    assert (totals["silver_accepted"], totals["dlq"]) == (15, 5)
    assert result["rows"] == 2
    assert result["reconcile_mismatches"] == 1
    assert result["report_uri"].endswith(f"/reports/{RUN}")
    assert (root / "reports" / RUN / "report.md").read_text(encoding="utf-8").startswith("# Ops")


def test_freshness_and_lag_only_where_a_kafka_timestamp_exists(spark: SparkSession, root: Path):
    now = datetime.now(UTC).replace(tzinfo=None)
    ingest = now - timedelta(minutes=30)
    path = _metrics_path(root)
    merge_rows(spark, path, [_reconcile(A, "r1", NEW)], SCOPE_RECONCILE)
    _bronze(
        spark,
        root,
        "raw_clickstream",
        [(A, ingest, ingest - timedelta(minutes=4))],
        "brand_id string, _ingest_ts timestamp, _kafka_ts timestamp",
    )
    _bronze(spark, root, "raw_orders", [(A, ingest)], "brand_id string, _ingest_ts timestamp")

    _, report = _run(spark, root, "--table", "raw_clickstream", "--table", "raw_orders")

    rows = {r["source_table"]: r for r in report["rows"]}
    assert 29.0 <= rows["raw_clickstream"]["freshness_min"] <= 40.0
    assert rows["raw_clickstream"]["ingest_lag_min"] == 4.0
    assert rows["raw_orders"]["ingest_lag_min"] is None  # no _kafka_ts column: not measurable
    assert rows["raw_clickstream"]["reconcile"] == "n/a"  # Bronze only, no reconcile row


def test_missing_bronze_table_is_skipped_and_missing_metrics_fail(spark: SparkSession, root: Path):
    with pytest.raises(RuntimeError, match="no ops.pipeline_metrics"):
        _run(spark, root, "--table", "raw_orders")

    merge_rows(spark, _metrics_path(root), [_reconcile(A, "r1", NEW)], SCOPE_RECONCILE)
    _, report = _run(spark, root, "--table", "raw_products")  # no Bronze table: warn and go on
    (row,) = report["rows"]
    assert row["freshness_min"] is None
    assert row["reconcile"] == "OK"
