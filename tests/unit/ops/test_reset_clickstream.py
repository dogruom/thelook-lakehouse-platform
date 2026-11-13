"""reset_clickstream (D-097): confirmation gate, exact target list, dry run deletes nothing."""

from __future__ import annotations

import argparse
from typing import Any

import pytest

from lakehouse.common.jobs import JobContext
from lakehouse.common.spark import SparkSettings
from lakehouse.ops.jobs import reset_clickstream
from lakehouse.ops.jobs.reset_clickstream import JOB


RUN_ID = "reset_clickstream_20261113T000000Z_abcdef01"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("LAKEHOUSE_ENV", "LAKEHOUSE_BUCKET", "LAKEHOUSE_CATALOG", "S3_ENDPOINT"):
        monkeypatch.delenv(key, raising=False)


def _args(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    JOB().add_arguments(parser)
    return parser.parse_args(list(argv))


def _ctx() -> JobContext:
    return JobContext("reset_clickstream", RUN_ID, SparkSettings(catalog="hive"))


@pytest.fixture
def fake_fs(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[str]]:
    calls: dict[str, list[str]] = {"count": [], "delete": []}

    def count(_spark: Any, uri: str) -> int:
        calls["count"].append(uri)
        return 0 if uri.endswith("pipeline_metrics") else 3

    def delete(_spark: Any, uri: str) -> bool:
        calls["delete"].append(uri)
        return True

    monkeypatch.setattr(reset_clickstream, "_file_count", count)
    monkeypatch.setattr(reset_clickstream, "_delete", delete)
    return calls


class _FakeSpark:
    def __init__(self) -> None:
        self.sql_calls: list[str] = []

    def sql(self, statement: str) -> None:
        self.sql_calls.append(statement)


def test_job_name_matches_module() -> None:
    assert JOB.name == "reset_clickstream"


def test_apply_without_confirm_is_refused(fake_fs: dict[str, list[str]]) -> None:
    with pytest.raises(RuntimeError, match="--confirm clickstream"):
        JOB().run(_FakeSpark(), _args("--apply"), _ctx())  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="--confirm clickstream"):
        JOB().run(_FakeSpark(), _args("--apply", "--confirm", "yes"), _ctx())  # type: ignore[arg-type]
    assert fake_fs["delete"] == []


def test_namespace_is_refused(fake_fs: dict[str, list[str]]) -> None:
    args = _args()
    args.namespace = "it_x"
    with pytest.raises(RuntimeError, match="shared catalog"):
        JOB().run(_FakeSpark(), args, _ctx())  # type: ignore[arg-type]


def test_dry_run_reports_only_clickstream_targets_and_deletes_nothing(
    fake_fs: dict[str, list[str]],
) -> None:
    spark = _FakeSpark()
    report = JOB().run(spark, _args(), _ctx())  # type: ignore[arg-type]
    locations = sorted(t["location"] for t in report["targets"].values())
    assert locations == [
        "s3a://lakehouse/bronze/raw_clickstream",
        "s3a://lakehouse/checkpoints/bronze_stream/raw_clickstream",
        "s3a://lakehouse/checkpoints/silver_clickstream/clickstream",
        "s3a://lakehouse/dead_letter/dlq_clickstream",
        "s3a://lakehouse/silver/clickstream",
    ]
    assert all("customer" not in loc and "order" not in loc for loc in locations)
    assert report["apply"] is False
    assert fake_fs["delete"] == []
    assert spark.sql_calls == []


def test_apply_drops_catalog_entries_and_deletes_exactly_the_targets(
    fake_fs: dict[str, list[str]],
) -> None:
    spark = _FakeSpark()
    report = JOB().run(spark, _args("--apply", "--confirm", "clickstream"), _ctx())  # type: ignore[arg-type]
    assert spark.sql_calls == [
        "DROP TABLE IF EXISTS bronze.raw_clickstream",
        "DROP TABLE IF EXISTS dead_letter.dlq_clickstream",
        "DROP TABLE IF EXISTS silver.clickstream",
    ]
    assert sorted(fake_fs["delete"]) == sorted(t["location"] for t in report["targets"].values())
    assert report["metrics_rows"] == 0
