"""Offline tests of the Spark-facing helpers in ``lakehouse.common.tables`` with a fake session.

The real behaviour against a catalog is proven in ``tests/spark/test_tables.py`` (Spark image);
these tests pin the SQL text, the quoting and the branching without PySpark.
"""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from lakehouse.common.tables import (
    CatalogLocationMismatchError,
    ResolvedTable,
    TableSpec,
    delta_exists_at_location,
    read_delta,
    register_table,
    table_exists,
    verify_location,
)


SPEC = TableSpec("bronze", "bronze", "raw_customers")
LOCATION = "s3a://lakehouse/bronze/raw_customers"
NAMED = ResolvedTable("bronze.raw_customers", LOCATION)
PATH_ONLY = ResolvedTable(None, LOCATION)


class _Frame:
    def __init__(self, location: str | None) -> None:
        self._location = location

    def select(self, _column: str) -> _Frame:
        return self

    def first(self) -> dict[str, str] | None:
        return None if self._location is None else {"location": self._location}


class _Reader:
    def __init__(self, log: list[str]) -> None:
        self._log = log

    def table(self, name: str) -> str:
        self._log.append(f"table:{name}")
        return "df-by-name"

    def format(self, fmt: str) -> _Reader:
        self._log.append(f"format:{fmt}")
        return self

    def load(self, location: str) -> str:
        self._log.append(f"load:{location}")
        return "df-by-path"


class FakeSpark:
    """Records ``spark.sql`` statements; the catalog knows the tables in ``registered``."""

    def __init__(self, registered: dict[str, str] | None = None) -> None:
        self.registered = dict(registered or {})
        self.statements: list[str] = []
        self.reads: list[str] = []
        self.read = _Reader(self.reads)
        self.catalog = types.SimpleNamespace(tableExists=lambda name: name in self.registered)

    def sql(self, statement: str) -> _Frame:
        self.statements.append(statement)
        if statement.startswith("DESCRIBE DETAIL"):
            name = statement.removeprefix("DESCRIBE DETAIL ").replace("`", "")
            return _Frame(self.registered.get(name))
        return _Frame(None)


@pytest.fixture
def delta_paths(monkeypatch: pytest.MonkeyPatch) -> set[str]:
    """Fake ``delta.tables.DeltaTable.isDeltaTable``: True for locations in the returned set."""
    paths: set[str] = set()
    delta_table = types.SimpleNamespace(isDeltaTable=lambda _spark, location: location in paths)
    module = types.ModuleType("delta.tables")
    module.DeltaTable = delta_table  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "delta", types.ModuleType("delta"))
    monkeypatch.setitem(sys.modules, "delta.tables", module)
    return paths


def test_read_delta_uses_the_name_when_there_is_one() -> None:
    spark: Any = FakeSpark()
    assert read_delta(spark, NAMED) == "df-by-name"
    assert read_delta(spark, PATH_ONLY) == "df-by-path"
    assert spark.reads == ["table:bronze.raw_customers", "format:delta", f"load:{LOCATION}"]


def test_table_exists_asks_the_catalog_for_named_and_delta_for_paths(
    delta_paths: set[str],
) -> None:
    spark: Any = FakeSpark(registered={"bronze.raw_customers": LOCATION})
    assert table_exists(spark, NAMED)
    assert not table_exists(spark, PATH_ONLY)  # catalog entry is irrelevant for path runs
    delta_paths.add(LOCATION)
    assert table_exists(spark, PATH_ONLY)
    assert delta_exists_at_location(spark, NAMED)


def test_register_creates_database_at_layer_root_then_external_table() -> None:
    spark: Any = FakeSpark()
    assert register_table(spark, SPEC, NAMED) == "registered"
    assert spark.statements == [
        "CREATE DATABASE IF NOT EXISTS `bronze` LOCATION 's3a://lakehouse/bronze'",
        f"CREATE TABLE IF NOT EXISTS `bronze`.`raw_customers` USING DELTA LOCATION '{LOCATION}'",
    ]


def test_register_existing_table_at_same_location_is_exists_without_create() -> None:
    spark: Any = FakeSpark(registered={"bronze.raw_customers": LOCATION + "/"})
    assert register_table(spark, SPEC, NAMED) == "exists"
    assert not any(s.startswith("CREATE TABLE") for s in spark.statements)


def test_register_existing_table_elsewhere_is_never_re_pointed() -> None:
    spark: Any = FakeSpark(registered={"bronze.raw_customers": "s3a://other/raw_customers"})
    with pytest.raises(CatalogLocationMismatchError, match="re-point"):
        register_table(spark, SPEC, NAMED)
    assert not any(s.startswith("CREATE TABLE") for s in spark.statements)


@pytest.mark.parametrize(
    ("table", "message"),
    [
        (PATH_ONLY, "name is None"),
        (ResolvedTable("bronze.raw_customers", "s3a://lakehouse/bronze/other"), "does not end"),
        (ResolvedTable("bronze.raw_customers", "s3a://lake'house/bronze/raw_customers"), "quote"),
        (ResolvedTable("bronze.raw_customers", "s3a://lake\\house/bronze/raw_customers"), "quote"),
    ],
)
def test_register_rejects_unusable_input_before_any_sql(table: ResolvedTable, message: str) -> None:
    spark: Any = FakeSpark()
    with pytest.raises(ValueError, match=message):
        register_table(spark, SPEC, table)
    assert spark.statements == []


def test_verify_location_passes_for_the_configured_location() -> None:
    spark: Any = FakeSpark(registered={"bronze.raw_customers": LOCATION})
    verify_location(spark, SPEC, NAMED)


def test_verify_location_fails_for_a_table_registered_elsewhere() -> None:
    spark: Any = FakeSpark(registered={"bronze.raw_customers": "s3a://lakehouse/x/raw_customers"})
    with pytest.raises(CatalogLocationMismatchError, match="refusing to use"):
        verify_location(spark, SPEC, NAMED)


def test_verify_location_is_a_no_op_for_path_runs() -> None:
    spark: Any = FakeSpark()
    verify_location(spark, SPEC, PATH_ONLY)
    assert spark.statements == []
