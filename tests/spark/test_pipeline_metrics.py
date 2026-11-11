"""``ops.pipeline_metrics`` writer and ``content_hash`` (W3.D18, D-081/D-084), from the design.

``record_batch(spark, path, row)`` MERGEs one wide row on ``(job, query_id, batch_id)``: a
re-executed batch overwrites (last attempt wins). Batch rows have ``scope='batch'`` and a NULL
``brand_id``. ``content_hash(df, exclude=("_updated_at",))`` is order independent, NULL aware and
sensitive to every non-excluded column. Runs inside the Spark image.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytest


pytest.importorskip("pyspark")

from lakehouse.common.hashing import content_hash  # noqa: E402
from lakehouse.ops.metrics import record_batch  # noqa: E402


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession

NOW = datetime(2026, 11, 11, 12, 0, 0)


def _row(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "job": "silver_orders",
        "source_table": "raw_orders",
        "query_id": "q-1",
        "batch_id": 0,
        "run_id": "run-1",
        "scope": "batch",
        "brand_id": None,
        "rows_read": 10,
        "reread_rows": 0,
        "parse_errors": 0,
        "bronze": 10,
        "silver_accepted": 6,
        "dlq": 2,
        "duplicates_removed": 2,
        "stale_cdc_noop": 0,
        "late_beyond_window": 0,
        "balanced": True,
        "recorded_at": NOW,
    }
    return {**base, **over}


def _rows(spark: SparkSession, path: Path) -> list[Any]:
    return spark.read.format("delta").load(str(path)).collect()


def test_first_record_creates_the_table_with_one_batch_row(spark: SparkSession, tmp_path: Path):
    path = tmp_path / "m1"
    record_batch(spark, str(path), _row())
    rows = _rows(spark, path)
    assert len(rows) == 1
    r = rows[0]
    assert (r["job"], r["scope"], r["brand_id"], r["batch_id"]) == (
        "silver_orders",
        "batch",
        None,
        0,
    )
    assert r["balanced"] is True and r["bronze"] == 10


def test_re_executed_batch_overwrites_instead_of_adding_a_row(spark: SparkSession, tmp_path: Path):
    path = tmp_path / "m2"
    record_batch(spark, str(path), _row(silver_accepted=6, dlq=2, run_id="first"))
    record_batch(spark, str(path), _row(silver_accepted=5, dlq=3, run_id="second"))
    rows = _rows(spark, path)
    assert len(rows) == 1
    assert (rows[0]["silver_accepted"], rows[0]["dlq"], rows[0]["run_id"]) == (5, 3, "second")


def test_other_batch_job_or_query_id_adds_rows(spark: SparkSession, tmp_path: Path):
    path = tmp_path / "m3"
    record_batch(spark, str(path), _row())
    record_batch(spark, str(path), _row(batch_id=1))
    record_batch(spark, str(path), _row(job="silver_customers", source_table="raw_customers"))
    record_batch(spark, str(path), _row(query_id="q-2"))  # new lineage, same batch_id
    keys = {(r["job"], r["query_id"], r["batch_id"]) for r in _rows(spark, path)}
    assert keys == {
        ("silver_orders", "q-1", 0),
        ("silver_orders", "q-1", 1),
        ("silver_customers", "q-1", 0),
        ("silver_orders", "q-2", 0),
    }


# ---- content_hash -----------------------------------------------------------------------------


def _df(spark: SparkSession, rows: list[tuple[Any, ...]]):
    return spark.createDataFrame(
        rows, "brand_id string, order_id string, amount string, _updated_at string"
    )


BASE = [("a", "o1", "1.00", "t1"), ("b", "o1", "2.00", None)]


def test_hash_is_stable_and_independent_of_row_and_column_order(spark: SparkSession):
    df = _df(spark, BASE)
    assert content_hash(df) == content_hash(_df(spark, list(reversed(BASE))))
    assert content_hash(df) == content_hash(
        df.select("amount", "order_id", "brand_id", "_updated_at")
    )


def test_hash_ignores_updated_at_by_default(spark: SparkSession):
    changed = [("a", "o1", "1.00", "other"), ("b", "o1", "2.00", "x")]
    assert content_hash(_df(spark, BASE)) == content_hash(_df(spark, changed))


def test_hash_detects_changed_missing_extra_and_cross_brand_rows(spark: SparkSession):
    ref = content_hash(_df(spark, BASE))
    assert content_hash(_df(spark, [("a", "o1", "1.01", "t1"), BASE[1]])) != ref  # value
    assert content_hash(_df(spark, BASE[:1])) != ref  # missing
    assert content_hash(_df(spark, [*BASE, BASE[0]])) != ref  # duplicate row
    swapped = [("b", "o1", "1.00", "t1"), ("a", "o1", "2.00", None)]  # brand moved
    assert content_hash(_df(spark, swapped)) != ref


def test_hash_distinguishes_null_from_empty_string(spark: SparkSession):
    a = _df(spark, [("a", "o1", None, "t")])
    b = _df(spark, [("a", "o1", "", "t")])
    assert content_hash(a) != content_hash(b)


def test_exclude_parameter_replaces_the_default_set(spark: SparkSession):
    changed = [("a", "o1", "9.99", "t1"), BASE[1]]
    assert content_hash(_df(spark, BASE), exclude=("amount", "_updated_at")) == content_hash(
        _df(spark, changed), exclude=("amount", "_updated_at")
    )
    assert content_hash(_df(spark, BASE), exclude=()) != content_hash(
        _df(spark, [("a", "o1", "1.00", "zz"), BASE[1]]), exclude=()
    )
