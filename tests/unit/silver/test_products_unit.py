"""silver_products on the host without PySpark: counters, equation check, CLI, registry."""

from __future__ import annotations

import argparse
import dataclasses
from importlib import import_module
from typing import Any

import pytest

from lakehouse.common.jobs import JobContext, resolve_job
from lakehouse.common.spark import SparkSettings
from lakehouse.silver.customers import ReconciliationError
from lakehouse.silver.products import OUTCOMES, ProductCounts, check_equation


MODULE = "lakehouse.silver.jobs.silver_products"
COUNTS = ProductCounts(
    bronze_rows=10,
    parse_excluded=1,
    dlq_excluded=2,
    superseded=3,
    accepted=4,
    inserted=3,
    updated=1,
)


def _job() -> Any:
    return import_module(MODULE).JOB


def test_job_resolves_by_name() -> None:
    assert resolve_job("silver_products") is _job()
    assert _job().name == "silver_products"


def test_cli_accepts_only_namespace() -> None:
    parser = argparse.ArgumentParser(prog="silver_products")
    _job()().add_arguments(parser)
    assert parser.parse_args([]).namespace is None
    assert parser.parse_args(["--namespace", "itest_ns"]).namespace == "itest_ns"
    with pytest.raises(SystemExit):
        parser.parse_args(["--max-files-per-trigger", "5"])


def test_counts_are_frozen_and_as_dict_lists_every_counter() -> None:
    assert {f.name for f in dataclasses.fields(ProductCounts)} == set(COUNTS.as_dict())
    with pytest.raises(dataclasses.FrozenInstanceError):
        COUNTS.accepted = 0  # type: ignore[misc]


def test_metrics_terms_balance_the_pipeline_equation() -> None:
    metrics = COUNTS.as_metrics()
    assert metrics["rows_read"] == 10
    bronze = metrics["rows_read"] - metrics["reread_rows"] - metrics["parse_excluded"]
    assert (
        bronze
        == metrics["silver_accepted"] + metrics["dlq_excluded"] + metrics["duplicates_removed"]
    )


def test_equation_holds_and_fails_loudly() -> None:
    check_equation(10, 1, 2, 3, 4)
    with pytest.raises(ReconciliationError, match="bronze_rows=11"):
        check_equation(11, 1, 2, 3, 4)


def test_outcomes_are_the_contracted_names() -> None:
    assert OUTCOMES == ("parse_excluded", "dlq_excluded", "superseded", "accepted")


# ---- job run(), registration and main (fakes, no Spark) -------------------------------------------

HIVE = SparkSettings(catalog="hive")


class _Read:
    def format(self, _fmt: str) -> _Read:
        return self

    def load(self, path: str) -> str:
        return f"df:{path}"


class _Spark:
    read = _Read()


class _Env:
    def __init__(self) -> None:
        self.events: list[Any] = []
        self.bronze_exists = True


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> _Env:
    from lakehouse.silver.jobs import silver_products as job

    e = _Env()
    monkeypatch.setattr(
        job,
        "delta_exists_at_location",
        lambda _s, t: e.events.append(("exists", t.location)) or e.bronze_exists,
    )

    def fake_run(_spark: Any, bronze: Any, target: Any) -> ProductCounts:
        e.events.append(("run", bronze, target.location))
        return COUNTS

    monkeypatch.setattr(job, "run_products", fake_run)

    class FakeRecorder:
        def __init__(self, _spark: Any, path: str, **kw: Any) -> None:
            e.events.append(("recorder", path, kw["job"], kw["source_table"], kw["run_id"]))

        def bind_query(self, query_id: str) -> None:
            e.events.append(("bind", query_id))

        def record(self, batch_id: int, counts: Any) -> None:
            e.events.append(("record", batch_id, dict(counts)))

    monkeypatch.setattr(job, "BatchRecorder", FakeRecorder)
    monkeypatch.setattr(
        job, "register_silver_table", lambda _s, _st, ns: e.events.append(("register", ns)) or {}
    )
    return e


def _run(namespace: str | None = None) -> Any:
    job = _job()()
    argv = ["--namespace", namespace] if namespace else []
    parser = argparse.ArgumentParser()
    job.add_arguments(parser)
    return job.run(_Spark(), parser.parse_args(argv), JobContext("silver_products", "run1", HIVE))


def test_run_merges_records_one_metrics_row_and_registers_last(env: _Env) -> None:
    out = _run()
    kinds = [e[0] for e in env.events]
    assert kinds == ["exists", "run", "recorder", "bind", "record", "register"]
    assert env.events[1] == (
        "run",
        "df:s3a://lakehouse/bronze/raw_products",
        "s3a://lakehouse/silver/products",
    )
    assert env.events[3] == ("bind", "run1")
    assert env.events[4][1:] == (0, COUNTS.as_metrics())
    assert out["counts"] == COUNTS.as_dict()
    assert out["path"] == "s3a://lakehouse/silver/products"
    assert "catalog" not in out


def test_run_namespace_scopes_paths(env: _Env) -> None:
    _run("ns1")
    assert "/ns1/" in env.events[1][1] and "/ns1/" in env.events[1][2]
    assert env.events[-1] == ("register", "ns1")


def test_run_fails_before_writing_without_bronze(env: _Env) -> None:
    env.bronze_exists = False
    with pytest.raises(RuntimeError, match="bronze_batch never ran"):
        _run()
    assert [e[0] for e in env.events] == ["exists"]


def test_register_silver_table_by_catalog_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    from lakehouse.silver.jobs import silver_products as job

    monkeypatch.setattr(job, "delta_exists_at_location", lambda _s, _t: True)
    monkeypatch.setattr(job, "register_table", lambda _s, spec, _t: f"reg:{spec.qualified_name}")
    assert job.register_silver_table(None, HIVE, None) == {"products": "reg:silver.products"}  # type: ignore[arg-type]
    assert job.register_silver_table(None, HIVE, "ns1") == {}  # type: ignore[arg-type]
    assert job.register_silver_table(None, SparkSettings(catalog="in_memory"), None) == {}  # type: ignore[arg-type]
    monkeypatch.setattr(job, "delta_exists_at_location", lambda _s, _t: False)
    assert job.register_silver_table(None, HIVE, None) == {}  # type: ignore[arg-type]


def test_register_failure_says_data_is_committed(monkeypatch: pytest.MonkeyPatch) -> None:
    from lakehouse.silver.jobs import silver_products as job
    from lakehouse.silver.jobs.silver_customers import CatalogRegistrationError

    def boom(*_a: Any) -> str:
        raise RuntimeError("metastore down")

    monkeypatch.setattr(job, "delta_exists_at_location", lambda _s, _t: True)
    monkeypatch.setattr(job, "register_table", boom)
    with pytest.raises(CatalogRegistrationError, match="Silver data is committed"):
        job.register_silver_table(None, HIVE, None)  # type: ignore[arg-type]


def test_main_delegates_to_the_shared_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    from lakehouse.silver.jobs import silver_products as job

    seen: list[list[str]] = []
    monkeypatch.setattr(job, "run_job", lambda argv: seen.append(argv) or 7)
    assert job.main(["--namespace", "ns1"]) == 7
    assert seen == [["silver_products", "--namespace", "ns1"]]
