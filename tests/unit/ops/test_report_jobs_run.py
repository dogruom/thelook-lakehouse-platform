"""bronze_report / dlq_report run(): fail-fast on a missing table, report shape, namespace scoping.

Spark is replaced by a stub whose every aggregate answers 1, so the assertions are about wiring and
report structure (which table is read, where, and what keys the report carries), not about counts.
"""

from __future__ import annotations

import argparse
from typing import Any

import pytest

from lakehouse.common.jobs import JobContext
from lakehouse.common.spark import SparkSettings
from lakehouse.ops.jobs import bronze_report, dlq_report
from tests.unit.quality import fake_spark


EVENT_ID = "123e4567-e89b-42d3-a456-426614174000"


class Row(dict[str, Any]):
    def __missing__(self, key: str) -> Any:
        return {
            "_rejection_codes": ["some_code"],
            "event_id": EVENT_ID,
            "_bronze_ref": "topic/0/7",
        }.get(key, 1)


class Df:
    """Every transformation returns itself; every aggregate answers 1."""

    def __init__(self) -> None:
        self.selected: list[tuple[Any, ...]] = []

    def _self(self, *_a: Any, **_k: Any) -> Df:
        return self

    agg = where = groupBy = _self

    def select(self, *cols: Any) -> Df:
        self.selected.append(cols)
        return self

    def first(self) -> Row | None:
        return Row()

    def collect(self) -> list[Row]:
        return [Row()]


class Env:
    def __init__(self) -> None:
        self.exists = True
        self.read: list[tuple[str | None, str]] = []


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> Env:
    e = Env()
    fake_spark.install(monkeypatch)
    for mod in (bronze_report, dlq_report):
        monkeypatch.setattr(mod, "table_exists", lambda _s, _t: e.exists)
        monkeypatch.setattr(
            mod, "read_delta", lambda _s, t: e.read.append((t.name, t.location)) or Df()
        )
    return e


def _ctx(catalog: str = "hive") -> JobContext:
    return JobContext("report", "run1", SparkSettings(catalog=catalog))  # type: ignore[arg-type]


def _args(job: Any, *argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    job().add_arguments(parser)
    return parser.parse_args(list(argv))


# ---- bronze_report ------------------------------------------------------------------------------


def test_bronze_report_covers_both_default_tables_by_catalog_name(env: Env) -> None:
    out = bronze_report.BronzeReportJob().run(None, _args(bronze_report.BronzeReportJob), _ctx())  # type: ignore[arg-type]
    assert env.read == [
        ("bronze.raw_customers", "s3a://lakehouse/bronze/raw_customers"),
        ("bronze.raw_clickstream", "s3a://lakehouse/bronze/raw_clickstream"),
    ]
    rep = out["tables"]["raw_customers"]
    assert set(rep) == {
        "path",
        "rows",
        "distinct_source_refs",
        "parse_errors",
        "null_counts",
        "rows_by_brand",
        "distinct_batch_ids",
        "distinct_lineages",
        "offsets_by_partition",
    }
    assert set(rep["null_counts"]) == set(bronze_report.NULL_COUNT_COLUMNS)
    assert set(rep["offsets_by_partition"]["1"]) == {"min", "max", "rows", "distinct"}


def test_bronze_report_namespace_reads_by_path_and_dedupes_tables(env: Env) -> None:
    job = bronze_report.BronzeReportJob
    out = job().run(
        None,  # type: ignore[arg-type]
        _args(job, "--namespace", "ns1", "--table", "raw_customers", "--table", "raw_customers"),
        _ctx(),
    )
    assert env.read == [(None, "s3a://lakehouse/bronze/ns1/raw_customers")]
    assert list(out["tables"]) == ["raw_customers"]


@pytest.mark.parametrize(("argv", "hint"), [((), True), (("--namespace", "ns1"), False)])
def test_bronze_report_missing_table_fails_with_register_hint_only_when_named(
    env: Env, argv: tuple[str, ...], hint: bool
) -> None:
    env.exists = False
    job = bronze_report.BronzeReportJob
    with pytest.raises(RuntimeError, match="no Bronze table at") as exc:
        job().run(None, _args(job, *argv), _ctx())  # type: ignore[arg-type]
    assert ("run register_tables" in str(exc.value)) is hint


def test_opt_int_keeps_none_and_converts_text() -> None:
    assert bronze_report._opt_int(None) is None
    assert bronze_report._opt_int("12") == 12
    assert bronze_report._opt_int(7) == 7


# ---- dlq_report ---------------------------------------------------------------------------------


def test_dlq_report_covers_default_dlq_tables_and_report_keys(env: Env) -> None:
    job = dlq_report.DlqReportJob
    out = job().run(None, _args(job), _ctx())  # type: ignore[arg-type]
    assert [name for name, _ in env.read] == [
        "dead_letter.dlq_customers",
        "dead_letter.dlq_clickstream",
    ]
    assert set(out["tables"]["dlq_customers"]) == {
        "path",
        "rows",
        "distinct_dlq_ids",
        "by_code",
        "by_category",
        "by_brand",
        "replay_status",
    }


def test_dlq_report_namespace_is_path_addressed(env: Env) -> None:
    job = dlq_report.DlqReportJob
    job().run(None, _args(job, "--namespace", "ns1", "--table", "dlq_clickstream"), _ctx())  # type: ignore[arg-type]
    assert env.read == [(None, "s3a://lakehouse/dead_letter/ns1/dlq_clickstream")]


def test_dlq_report_missing_table_fails_before_reading(env: Env) -> None:
    env.exists = False
    job = dlq_report.DlqReportJob
    with pytest.raises(RuntimeError, match="the gate never ran for it; run register_tables"):
        job().run(None, _args(job), _ctx())  # type: ignore[arg-type]
    assert env.read == []


def test_dlq_report_list_refs_does_not_change_the_report(env: Env) -> None:
    job = dlq_report.DlqReportJob
    plain = job().run(None, _args(job), _ctx())  # type: ignore[arg-type]
    listed = job().run(None, _args(job, "--list-refs"), _ctx())  # type: ignore[arg-type]
    assert plain == listed


def test_dlq_report_only_gate_tables_are_selectable() -> None:
    job = dlq_report.DlqReportJob
    with pytest.raises(SystemExit):
        _args(job, "--table", "dlq_unknown")
