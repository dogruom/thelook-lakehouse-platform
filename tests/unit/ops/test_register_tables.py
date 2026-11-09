"""register_tables job: argument parsing, in-memory refusal and per-table status (fakes, offline)."""

from __future__ import annotations

import argparse
from typing import Any

import pytest

from lakehouse.common.jobs import JobContext
from lakehouse.common.spark import SparkSettings
from lakehouse.common.tables import CATALOG_TABLES, CatalogLocationMismatchError
from lakehouse.ops.jobs import register_tables
from lakehouse.ops.jobs.register_tables import JOB


RUN_ID = "register_tables_20261110T000000Z_abcdef01"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("LAKEHOUSE_ENV", "LAKEHOUSE_BUCKET", "LAKEHOUSE_CATALOG", "S3_ENDPOINT"):
        monkeypatch.delenv(key, raising=False)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    JOB().add_arguments(parser)
    return parser


def _ctx(**kwargs: Any) -> JobContext:
    return JobContext("register_tables", RUN_ID, SparkSettings(**kwargs))


def _run(tables: list[str] | None, **settings: Any) -> Any:
    """Run the job with no Spark session: every Spark-touching helper is monkeypatched."""
    return JOB().run(None, argparse.Namespace(table=tables), _ctx(**settings))  # type: ignore[arg-type]


def test_job_name_matches_module() -> None:
    assert JOB.name == "register_tables"


def test_table_defaults_to_none_meaning_all() -> None:
    assert _parser().parse_args([]).table is None


def test_table_is_repeatable_and_restricted_to_catalog_tables() -> None:
    args = _parser().parse_args(["--table", "raw_customers", "--table", "dlq_customers"])
    assert args.table == ["raw_customers", "dlq_customers"]
    with pytest.raises(SystemExit):
        _parser().parse_args(["--table", "nope"])


def test_in_memory_catalog_is_refused_before_touching_spark() -> None:
    with pytest.raises(RuntimeError, match="in_memory"):
        _run(None)


def test_reports_missing_registered_and_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    present = {"s3a://lakehouse/bronze/raw_customers", "s3a://lakehouse/bronze/raw_clickstream"}
    registered: list[str] = []
    monkeypatch.setattr(
        register_tables, "delta_exists_at_location", lambda _s, t: t.location in present
    )

    def fake_register(_spark: Any, spec: Any, _table: Any) -> str:
        registered.append(spec.qualified_name)
        return "registered" if spec.name == "raw_customers" else "exists"

    monkeypatch.setattr(register_tables, "register_table", fake_register)
    tables = _run(None, catalog="hive")["tables"]
    assert set(tables) == set(CATALOG_TABLES)
    assert tables["raw_customers"] == {
        "name": "bronze.raw_customers",
        "location": "s3a://lakehouse/bronze/raw_customers",
        "status": "registered",
    }
    assert tables["raw_clickstream"]["status"] == "exists"
    assert tables["dlq_customers"]["status"] == "missing"
    assert sorted(registered) == ["bronze.raw_clickstream", "bronze.raw_customers"]


def test_table_selection_limits_the_work(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(register_tables, "delta_exists_at_location", lambda _s, _t: False)
    result = _run(["dlq_customers", "dlq_customers"], catalog="hive")
    assert list(result["tables"]) == ["dlq_customers"]


def test_location_mismatch_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(register_tables, "delta_exists_at_location", lambda _s, _t: True)

    def boom(*_args: Any) -> str:
        raise CatalogLocationMismatchError("x")

    monkeypatch.setattr(register_tables, "register_table", boom)
    with pytest.raises(CatalogLocationMismatchError):
        _run(["raw_customers"], catalog="hive")
