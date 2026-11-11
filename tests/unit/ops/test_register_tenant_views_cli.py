"""register_tenant_views job: CLI and refusal rules with a recording fake (offline, W3.D18)."""

from __future__ import annotations

import argparse
from typing import Any

import pytest

from lakehouse.common.jobs import JobContext
from lakehouse.common.spark import SparkSettings
from lakehouse.ops.jobs.register_tenant_views import JOB


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("LAKEHOUSE_ENV", "LAKEHOUSE_BUCKET", "LAKEHOUSE_CATALOG", "S3_ENDPOINT"):
        monkeypatch.delenv(key, raising=False)


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="register_tenant_views")
    JOB().add_arguments(parser)
    return parser.parse_args(list(argv))


class _Result:
    def collect(self) -> list[Any]:
        return []

    def first(self) -> Any:
        return None


class _Catalog:
    def tableExists(self, _name: str) -> bool:
        return True


class RecordingSpark:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.catalog = _Catalog()

    def sql(self, statement: str) -> _Result:
        self.statements.append(statement)
        return _Result()


def test_job_name_matches_module() -> None:
    assert JOB.name == "register_tenant_views"


def test_brand_is_repeatable_and_validated_later_by_the_sql_builder() -> None:
    assert list(_parse("--brand", "brand_alpha", "--brand", "brand_beta").brand) == [
        "brand_alpha",
        "brand_beta",
    ]


def test_in_memory_catalog_is_refused_before_any_sql() -> None:
    spark = RecordingSpark()
    ctx = JobContext("register_tenant_views", "run1", SparkSettings(catalog="in_memory"))
    with pytest.raises(RuntimeError):
        JOB().run(spark, _parse("--brand", "brand_alpha"), ctx)  # type: ignore[arg-type]
    assert spark.statements == []


def test_namespaced_runs_are_refused_before_any_sql() -> None:
    spark = RecordingSpark()
    ctx = JobContext("register_tenant_views", "run1", SparkSettings(catalog="hive"))
    args = _parse("--brand", "brand_alpha")
    args.namespace = "it_12345678"
    with pytest.raises(RuntimeError):
        JOB().run(spark, args, ctx)  # type: ignore[arg-type]
    assert spark.statements == []


def test_hostile_brand_never_reaches_spark_sql() -> None:
    spark = RecordingSpark()
    ctx = JobContext("register_tenant_views", "run1", SparkSettings(catalog="hive"))
    with pytest.raises(ValueError):
        JOB().run(spark, _parse("--brand", "x' OR '1'='1"), ctx)  # type: ignore[arg-type]
    assert not any("OR '1'='1" in s for s in spark.statements)


def test_each_view_filters_only_its_own_brand() -> None:
    spark = RecordingSpark()
    ctx = JobContext("register_tenant_views", "run1", SparkSettings(catalog="hive"))
    JOB().run(spark, _parse("--brand", "brand_alpha", "--brand", "brand_beta"), ctx)  # type: ignore[arg-type]
    views = [s for s in spark.statements if "VIEW" in s]
    assert views, "no CREATE VIEW statement was issued"
    for sql in views:
        assert sql.count("brand_id =") == 1
        other = "brand_beta" if "v_" in sql and "__brand_alpha" in sql else "brand_alpha"
        assert other not in sql
