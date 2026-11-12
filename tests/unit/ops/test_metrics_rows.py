"""``ops.pipeline_metrics`` row building, merge guards and the batch recorder (offline)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from lakehouse.common.spark import SparkSettings
from lakehouse.ops import metrics
from lakehouse.ops.metrics import (
    FIELDS,
    SCOPE_BATCH,
    SCOPE_RECONCILE,
    BatchRecorder,
    batch_row,
    is_balanced,
    merge_rows,
    metrics_path,
    metrics_schema,
    record_batch,
)
from tests.unit.quality import fake_spark


_AT = datetime(2026, 1, 2, 3, 4, 5)


def _counts(**over: int) -> dict[str, int]:
    base = {
        "rows_read": 10,
        "reread_rows": 2,
        "parse_excluded": 1,
        "silver_accepted": 4,
        "dlq_excluded": 1,
        "duplicates_removed": 2,
    }
    base.update(over)
    return base


def _row(**over: Any) -> dict[str, Any]:
    kw: dict[str, Any] = {
        "job": "silver_orders",
        "source_table": "raw_orders",
        "query_id": "q1",
        "batch_id": 7,
        "run_id": "r1",
        "counts": _counts(),
        "recorded_at": _AT,
    }
    kw.update(over)
    return batch_row(**kw)


class _Spark:
    def __init__(self) -> None:
        self.created: list[Any] = []

    def createDataFrame(self, data: Any, schema: Any) -> Any:
        self.created.append((data, schema))
        return fake_spark.Perm("df")


def test_batch_row_derives_bronze_from_rows_minus_reread_minus_parse() -> None:
    row = _row()
    assert row["bronze"] == 7
    assert row["parse_errors"] == 1
    assert row["reread_rows"] == 2
    assert row["scope"] == SCOPE_BATCH
    assert row["brand_id"] is None
    assert row["recorded_at"] == _AT


def test_batch_row_balanced_when_terms_sum_to_bronze() -> None:
    assert _row()["balanced"] is True  # 4 + 1 + 2 == 7


def test_batch_row_unbalanced_when_a_term_is_missing() -> None:
    assert _row(counts=_counts(silver_accepted=3))["balanced"] is False


def test_fk_orphans_count_as_dlq_for_orders() -> None:
    row = _row(counts=_counts(fk_orphans=2, silver_accepted=2))
    assert row["dlq"] == 3
    assert row["balanced"] is True


def test_batch_row_defaults_recorded_at_to_naive_utc_now() -> None:
    row = _row(recorded_at=None)
    assert isinstance(row["recorded_at"], datetime)
    assert row["recorded_at"].tzinfo is None


def test_is_balanced_treats_none_as_zero_and_detects_violation() -> None:
    zero = dict.fromkeys(metrics.TERMS)
    assert is_balanced({"bronze": None, **zero}) is True
    assert is_balanced({"bronze": 1, **zero}) is False


def test_batch_row_has_exactly_the_table_fields() -> None:
    assert set(_row()) == set(FIELDS)


def test_metrics_path_uses_namespace_segment() -> None:
    settings = SparkSettings()
    assert metrics_path(settings, None).endswith("ops/pipeline_metrics")
    assert metrics_path(settings, "it_1").endswith("ops/it_1/pipeline_metrics")


def test_merge_rows_rejects_unknown_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_spark.install(monkeypatch, delta_table=object)
    with pytest.raises(ValueError, match="unknown scope"):
        merge_rows(_Spark(), "p", [_row()], "nope")  # type: ignore[arg-type]


def test_merge_rows_rejects_rows_of_another_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_spark.install(monkeypatch, delta_table=object)
    with pytest.raises(ValueError, match="every row must have scope"):
        merge_rows(_Spark(), "p", [_row()], SCOPE_RECONCILE)  # type: ignore[arg-type]


def test_merge_rows_with_no_rows_touches_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_spark.install(monkeypatch, delta_table=object)
    spark = _Spark()
    merge_rows(spark, "p", [], SCOPE_BATCH)  # type: ignore[arg-type]
    assert spark.created == []


def test_merge_rows_builds_source_in_field_order(monkeypatch: pytest.MonkeyPatch) -> None:
    class Delta:
        @staticmethod
        def isDeltaTable(_s: Any, _p: str) -> bool:
            return True

        @staticmethod
        def forPath(_s: Any, _p: str) -> Any:
            return fake_spark.Perm("table")

    fake_spark.install(monkeypatch, delta_table=Delta)
    spark = _Spark()
    row = _row()
    merge_rows(spark, "p", [row], SCOPE_BATCH)  # type: ignore[arg-type]
    data, _schema = spark.created[0]
    assert data == [tuple(row.get(f) for f in FIELDS)]


def test_merge_rows_retries_concurrent_errors_then_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"n": 0}

    class Delta:
        @staticmethod
        def isDeltaTable(_s: Any, _p: str) -> bool:
            calls["n"] += 1
            raise RuntimeError("ConcurrentAppendException")

    fake_spark.install(monkeypatch, delta_table=Delta)
    monkeypatch.setattr(metrics.time, "sleep", lambda _s: None)
    with pytest.raises(RuntimeError, match="Concurrent"):
        merge_rows(_Spark(), "p", [_row()], SCOPE_BATCH)  # type: ignore[arg-type]
    assert calls["n"] == 3


def test_merge_rows_does_not_retry_other_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    class Delta:
        @staticmethod
        def isDeltaTable(_s: Any, _p: str) -> bool:
            calls["n"] += 1
            raise RuntimeError("disk full")

    fake_spark.install(monkeypatch, delta_table=Delta)
    with pytest.raises(RuntimeError, match="disk full"):
        merge_rows(_Spark(), "p", [_row()], SCOPE_BATCH)  # type: ignore[arg-type]
    assert calls["n"] == 1


def test_metrics_schema_builds_for_every_column(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_spark.install(monkeypatch)
    assert metrics_schema() is not None


def test_record_batch_merges_with_batch_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[Any] = []
    monkeypatch.setattr(metrics, "merge_rows", lambda *a: seen.append(a))
    record_batch("spark", "p", {"x": 1})  # type: ignore[arg-type]
    assert seen == [("spark", "p", [{"x": 1}], SCOPE_BATCH)]


def test_recorder_writes_row_with_bound_query_id(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[Any] = []
    monkeypatch.setattr(metrics, "record_batch", lambda _s, p, row: seen.append((p, row)))
    rec = BatchRecorder("s", "path", job="silver_orders", source_table="raw_orders", run_id="r1")  # type: ignore[arg-type]
    rec.bind_query("q-9")
    rec.record(4, _counts())
    path, row = seen[0]
    assert path == "path"
    assert row["query_id"] == "q-9"
    assert row["batch_id"] == 4
    assert row["run_id"] == "r1"
    assert row["balanced"] is True


def test_recorder_fails_when_query_id_is_never_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(metrics, "_QUERY_ID_WAIT_S", 0.0)
    rec = BatchRecorder("s", "p", job="j", source_table="t", run_id="r")  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="never bound"):
        rec.record(1, _counts())
