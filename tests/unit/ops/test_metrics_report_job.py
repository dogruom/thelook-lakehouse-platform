"""metrics_report job orchestration on the host (W5.D30.1, V-24).

The Spark reads are replaced by plain lists; the Spark behaviour itself is covered by
``tests/spark/test_metrics_report.py`` in the container. This file checks the wiring: missing
metrics table fails, mismatches are counted (the report never fails on them), and the report files
go to ``<base>/<run_id>/`` through the Hadoop FileSystem calls.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from lakehouse.common.jobs import JobContext
from lakehouse.common.spark import SparkSettings
from lakehouse.ops.jobs import metrics_report


RUN_ID = "metrics_report_20261114T120000Z_0123abcd"


def _ctx() -> JobContext:
    return JobContext(job_name="metrics_report", run_id=RUN_ID, settings=SparkSettings())


def _args(**over: Any) -> argparse.Namespace:
    return argparse.Namespace(**{"namespace": None, "table": None, "output_uri": None, **over})


def _reconcile(brand: str, balanced: bool) -> dict[str, Any]:
    return {
        "scope": "reconcile",
        "source_table": "raw_orders",
        "brand_id": brand,
        "run_id": "r1",
        "recorded_at": datetime(2026, 11, 14, 11, 0, 0),
        "balanced": balanced,
        "bronze": 10,
        "silver_accepted": 9,
        "dlq": 1,
        "duplicates_removed": 0,
        "stale_cdc_noop": None,
        "late_beyond_window": None,
        "parse_errors": 0,
    }


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    seen: dict[str, Any] = {}

    def fake_freshness(_spark: Any, _ctx: Any, tables: list[str], _ns: str | None) -> list[Any]:
        seen["tables"] = tables
        return []

    monkeypatch.setattr(metrics_report, "delta_exists_at_location", lambda *_: True)
    monkeypatch.setattr(
        metrics_report,
        "_reconcile_rows",
        lambda *_: [_reconcile("brand_a", True), _reconcile("brand_b", False)],
    )
    monkeypatch.setattr(metrics_report, "_batch_sums", lambda *_: [])
    monkeypatch.setattr(metrics_report, "_freshness", fake_freshness)
    return seen


def test_missing_metrics_table_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(metrics_report, "delta_exists_at_location", lambda *_: False)
    with pytest.raises(RuntimeError, match="no ops.pipeline_metrics"):
        metrics_report.MetricsReportJob().run(MagicMock(), _args(), _ctx())


def test_mismatch_is_counted_not_raised(patched: dict[str, Any]) -> None:
    result = metrics_report.MetricsReportJob().run(
        MagicMock(), _args(table=["raw_orders", "raw_orders"]), _ctx()
    )
    assert result["rows"] == 2
    assert result["reconcile_mismatches"] == 1
    assert result["report_uri"] is None
    assert patched["tables"] == ["raw_orders"]  # de-duplicated, order kept


def test_default_tables_when_none_given(patched: dict[str, Any]) -> None:
    metrics_report.MetricsReportJob().run(MagicMock(), _args(), _ctx())
    assert patched["tables"] == list(metrics_report.DEFAULT_TABLES)


def test_report_files_written_under_run_id(patched: dict[str, Any]) -> None:
    spark = MagicMock()
    path_cls = spark.sparkContext._jvm.org.apache.hadoop.fs.Path
    fs = path_cls.return_value.getFileSystem.return_value
    written: dict[str, bytes] = {}

    def create(_path: Any, _overwrite: bool) -> MagicMock:
        name = path_cls.call_args.args[0].rsplit("/", 1)[1]
        stream = MagicMock()
        stream.write.side_effect = lambda data, *_: written.__setitem__(name, bytes(data))
        return stream

    fs.create.side_effect = create
    result = metrics_report.MetricsReportJob().run(
        spark, _args(output_uri="s3://bucket/ops/reports"), _ctx()
    )
    assert result["report_uri"] == f"s3://bucket/ops/reports/{RUN_ID}"
    fs.mkdirs.assert_called_once()
    assert set(written) == {"report.md", "report.json"}
    assert json.loads(written["report.json"].decode("utf-8"))
    assert "brand_b" in written["report.md"].decode("utf-8")
