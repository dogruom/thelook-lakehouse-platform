"""``reconcile`` job (W3.D18, D-082) on tmp Delta tables, from the design algorithm.

``SparkSettings.layer_uri`` is patched to point at ``tmp_path`` so the job runs path-addressed
(namespace ``t``) without S3. Source tables carry only the columns the algorithm names:
Bronze ``brand_id``/``_source_ref``/``_parse_error``, DLQ ``brand_id``/``_bronze_ref``/
``_replay_status``, Silver ``brand_id``. Data-derived totals: bronze = distinct parseable
``_source_ref``; ``reread_rows`` = rows - distinct refs; ``dlq`` = distinct non-REPLAYED DLQ refs.
Failing checks make ``run`` raise (exit 1 of the framework). Runs inside the Spark image.
"""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytest


pytest.importorskip("pyspark")

from lakehouse.common.jobs import JobContext  # noqa: E402
from lakehouse.common.spark import SparkSettings  # noqa: E402
from lakehouse.ops.metrics import record_batch  # noqa: E402
from lakehouse.quality.jobs.reconcile import JOB  # noqa: E402


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession

A, B = "brand_alpha", "brand_beta"
NS = "t"
BRONZE = "brand_id string, _source_ref string, _parse_error string"
DLQ = "brand_id string, _bronze_ref string, _replay_status string"
SILVER = "brand_id string, order_id string"


@pytest.fixture
def root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    def layer_uri(_self: SparkSettings, layer: str, *parts: str) -> str:
        return (tmp_path / layer / "/".join(parts)).as_posix()

    monkeypatch.setattr(SparkSettings, "layer_uri", layer_uri)
    return tmp_path


def _write(spark: SparkSession, path: Path, rows: list[tuple[Any, ...]], schema: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").save(
        path.as_posix()
    )


def _tables(
    spark: SparkSession, root: Path, *, orders_silver: int = 3, dlq_status: str = "PENDING"
):
    """Orders: 5 parseable refs for alpha (+1 re-read copy, +1 parse error): 3 accepted, 1 DLQ, 1 dup."""
    refs = [(A, f"f#L{i}", None) for i in range(1, 6)]
    _write(
        spark,
        root / "bronze" / NS / "raw_orders",
        [*refs, refs[0], (A, "f#L9", "bad_json")],
        BRONZE,
    )
    _write(spark, root / "dead_letter" / NS / "dlq_orders", [(A, "f#L4", dlq_status)], DLQ)
    _write(
        spark,
        root / "silver" / NS / "orders",
        [(A, f"o{i}") for i in range(orders_silver)],
        SILVER,
    )
    for name in ("raw_customers", "raw_clickstream"):
        _write(spark, root / "bronze" / NS / name, [], BRONZE)
    for name in ("dlq_customers", "dlq_clickstream"):
        _write(spark, root / "dead_letter" / NS / name, [], DLQ)
    _write(spark, root / "silver" / NS / "customers", [], "brand_id string, customer_id string")
    _write(spark, root / "silver" / NS / "clickstream", [], "brand_id string, event_id string")


def _batch(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "job": "silver_orders",
        "source_table": "raw_orders",
        "query_id": "q",
        "batch_id": 0,
        "run_id": "r",
        "scope": "batch",
        "brand_id": None,
        "rows_read": 7,
        "reread_rows": 1,
        "parse_errors": 1,
        "bronze": 5,
        "silver_accepted": 3,
        "dlq": 1,
        "duplicates_removed": 1,
        "stale_cdc_noop": 0,
        "late_beyond_window": 0,
        "balanced": True,
        "recorded_at": datetime(2026, 11, 11, 12, 0, 0),
    }
    return {**base, **over}


def _run(spark: SparkSession, root: Path) -> Any:
    parser = argparse.ArgumentParser()
    JOB().add_arguments(parser)
    args = parser.parse_args(["--namespace", NS])
    ctx = JobContext("reconcile", "run1", SparkSettings(master="local[2]"))
    return JOB().run(spark, args, ctx)


def _metrics_path(root: Path) -> str:
    return (root / "ops" / NS / "pipeline_metrics").as_posix()


def _reconcile_rows(spark: SparkSession, root: Path) -> list[Any]:
    df = spark.read.format("delta").load(_metrics_path(root))
    return [r for r in df.collect() if r["scope"] == "reconcile"]


def test_consistent_batches_and_data_reconcile_and_write_brand_totals(
    spark: SparkSession, root: Path
) -> None:
    _tables(spark, root)
    record_batch(spark, _metrics_path(root), _batch())
    _run(spark, root)
    rows = [r for r in _reconcile_rows(spark, root) if r["source_table"] == "raw_orders"]
    by_brand = {r["brand_id"]: r for r in rows}
    assert set(by_brand) == {A}
    r = by_brand[A]
    assert (r["bronze"], r["parse_errors"], r["reread_rows"]) == (5, 1, 1)
    assert (r["silver_accepted"], r["dlq"]) == (3, 1)


def test_replayed_dlq_rows_count_as_silver_accepted_not_dlq(
    spark: SparkSession, root: Path
) -> None:
    _tables(spark, root, orders_silver=4, dlq_status="REPLAYED")
    record_batch(spark, _metrics_path(root), _batch(silver_accepted=4, dlq=0))
    _run(spark, root)
    r = next(x for x in _reconcile_rows(spark, root) if x["source_table"] == "raw_orders")
    assert (r["dlq"], r["silver_accepted"]) == (0, 4)


def test_rerun_of_reconcile_does_not_duplicate_its_rows(spark: SparkSession, root: Path) -> None:
    _tables(spark, root)
    record_batch(spark, _metrics_path(root), _batch())
    _run(spark, root)
    first = len(_reconcile_rows(spark, root))
    _run(spark, root)
    assert len(_reconcile_rows(spark, root)) == first


# ---- negative: each check detects its violation -----------------------------------------------


def test_unbalanced_batch_row_fails_the_job(spark: SparkSession, root: Path) -> None:
    _tables(spark, root)
    record_batch(spark, _metrics_path(root), _batch(silver_accepted=4, balanced=False))
    with pytest.raises(Exception):  # noqa: B017  (the error class is not fixed by the design)
        _run(spark, root)


def test_batch_terms_that_disagree_with_the_data_fail_the_job(
    spark: SparkSession, root: Path
) -> None:
    _tables(spark, root, orders_silver=2)  # Silver lost a row the batch row says it accepted
    record_batch(spark, _metrics_path(root), _batch())
    with pytest.raises(Exception):  # noqa: B017
        _run(spark, root)


def test_balanced_but_wrong_batch_row_is_still_caught_against_the_data(
    spark: SparkSession, root: Path
) -> None:
    _tables(spark, root)
    # internally consistent (4 + 1 = 5) but the data says 3 accepted + 1 dlq + 1 duplicate
    record_batch(spark, _metrics_path(root), _batch(silver_accepted=4, duplicates_removed=0))
    with pytest.raises(Exception):  # noqa: B017
        _run(spark, root)
