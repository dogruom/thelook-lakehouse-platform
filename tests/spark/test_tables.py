"""Catalog registration and by-name reads (in-memory catalog; runs inside the Spark image)."""

from __future__ import annotations

from typing import TYPE_CHECKING

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


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession


def _make(spark: SparkSession, tmp_path: Path, db: str) -> tuple[TableSpec, ResolvedTable]:
    """A Delta table with 3 rows at <tmp>/<db>/t, addressed as ``<db>.t``."""
    spec = TableSpec("bronze", db, "t")
    location = f"file:{(tmp_path / db / 't').as_posix()}"
    spark.range(3).write.format("delta").save(location)
    return spec, ResolvedTable(spec.qualified_name, location)


def test_register_then_rerun_then_read_by_name(spark: SparkSession, tmp_path: Path) -> None:
    spec, table = _make(spark, tmp_path, "reg_a")
    assert delta_exists_at_location(spark, table)
    assert not table_exists(spark, table)  # data exists, catalog does not know it yet

    assert register_table(spark, spec, table) == "registered"
    assert table_exists(spark, table)
    assert register_table(spark, spec, table) == "exists"
    assert sorted(r["id"] for r in read_delta(spark, table).collect()) == [0, 1, 2]


def test_trailing_slash_on_the_expected_location_is_ignored(
    spark: SparkSession, tmp_path: Path
) -> None:
    spec, table = _make(spark, tmp_path, "reg_b")
    assert register_table(spark, spec, table) == "registered"
    slashed = ResolvedTable(table.name, table.location + "/")
    assert register_table(spark, spec, slashed) == "exists"


def test_location_mismatch_is_never_re_pointed(spark: SparkSession, tmp_path: Path) -> None:
    spec, table = _make(spark, tmp_path, "reg_c")
    assert register_table(spark, spec, table) == "registered"
    other = ResolvedTable(table.name, f"file:{(tmp_path / 'reg_c' / 'elsewhere' / 't').as_posix()}")
    with pytest.raises(CatalogLocationMismatchError, match="re-point"):
        register_table(spark, spec, other)
    assert sorted(r["id"] for r in read_delta(spark, table).collect()) == [0, 1, 2]


def test_table_exists_is_false_for_an_unknown_database(spark: SparkSession) -> None:
    assert not table_exists(spark, ResolvedTable("no_such_db_xyz.t", "file:/nowhere/t"))


def test_path_addressed_table_reads_by_location(spark: SparkSession, tmp_path: Path) -> None:
    _, table = _make(spark, tmp_path, "reg_d")
    path_only = ResolvedTable(None, table.location)
    assert table_exists(spark, path_only)
    assert read_delta(spark, path_only).count() == 3
    assert not table_exists(spark, ResolvedTable(None, f"file:{(tmp_path / 'none').as_posix()}"))


def test_register_requires_a_catalog_name(spark: SparkSession, tmp_path: Path) -> None:
    spec, table = _make(spark, tmp_path, "reg_e")
    with pytest.raises(ValueError, match="catalog-addressed"):
        register_table(spark, spec, ResolvedTable(None, table.location))


def test_register_rejects_a_location_not_ending_in_the_table_name(spark: SparkSession) -> None:
    spec = TableSpec("bronze", "reg_f", "t")
    with pytest.raises(ValueError, match="does not end with"):
        register_table(spark, spec, ResolvedTable("reg_f.t", "file:/tmp/reg_f/other"))


def test_register_rejects_a_quote_in_the_location(spark: SparkSession) -> None:
    spec = TableSpec("bronze", "reg_g", "t")
    with pytest.raises(ValueError, match="quote"):
        register_table(spark, spec, ResolvedTable("reg_g.t", "file:/tmp/it's/t"))


def test_verify_location_accepts_the_registered_location(
    spark: SparkSession, tmp_path: Path
) -> None:
    spec, table = _make(spark, tmp_path, "ver_a")
    register_table(spark, spec, table)
    verify_location(spark, spec, table)  # no error


def test_verify_location_rejects_a_table_registered_elsewhere(
    spark: SparkSession, tmp_path: Path
) -> None:
    spec, table = _make(spark, tmp_path, "ver_b")
    register_table(spark, spec, table)
    expected_elsewhere = ResolvedTable(
        table.name, f"file:{(tmp_path / 'ver_b' / 'configured' / 't').as_posix()}"
    )
    with pytest.raises(CatalogLocationMismatchError, match="refusing to use"):
        verify_location(spark, spec, expected_elsewhere)


def test_verify_location_is_a_no_op_for_path_addressed_tables(spark: SparkSession) -> None:
    spec = TableSpec("bronze", "ver_c", "t")
    verify_location(spark, spec, ResolvedTable(None, "file:/does/not/exist/t"))  # no catalog call
