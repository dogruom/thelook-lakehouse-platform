"""dlq_gate job: CLI, fail-fast checks, per-table loop, catalog registration (stubbed Spark)."""

from __future__ import annotations

import argparse
from typing import Any

import pytest

from lakehouse.common.jobs import JobContext
from lakehouse.common.spark import SparkSettings
from lakehouse.quality.jobs import dlq_gate
from lakehouse.quality.jobs.dlq_gate import DEFAULT_TABLES, DlqGateJob, ReconciliationError
from tests.unit.quality import fake_spark
from tests.unit.quality.fake_spark import Perm


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="dlq_gate")
    DlqGateJob().add_arguments(parser)
    return parser.parse_args(list(argv))


def _ctx(catalog: str = "hive") -> JobContext:
    return JobContext("dlq_gate", "run1", SparkSettings(catalog=catalog))  # type: ignore[arg-type]


def test_defaults_gate_customers_and_clickstream_only() -> None:
    assert DEFAULT_TABLES == ("raw_customers", "raw_clickstream")
    args = _parse()
    assert (args.table, args.namespace) == (None, None)


def test_table_is_repeatable_and_restricted_to_known_bronze_tables() -> None:
    assert _parse("--table", "raw_orders", "--table", "raw_products").table == [
        "raw_orders",
        "raw_products",
    ]
    with pytest.raises(SystemExit):
        _parse("--table", "raw_nothing")


class Env:
    def __init__(self) -> None:
        self.exists: dict[str, bool] = {}
        self.verified: list[str] = []
        self.gated: list[dict[str, Any]] = []
        self.registered: list[str] = []
        self.problems: dict[str, list[str]] = {}


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> Env:
    e = Env()
    monkeypatch.setattr(dlq_gate, "table_exists", lambda _s, t: e.exists.get(t.location, True))
    monkeypatch.setattr(
        dlq_gate, "verify_location", lambda _s, spec, _t: e.verified.append(spec.name)
    )

    def gate(_spark: Any, *, table: str, bronze: Any, dlq_path: str, run_id: str) -> Any:
        e.gated.append({"table": table, "bronze": bronze, "dlq_path": dlq_path, "run_id": run_id})
        return {"rows_read": 1}, e.problems.get(table, [])

    monkeypatch.setattr(dlq_gate, "_gate_table", gate)
    monkeypatch.setattr(
        dlq_gate,
        "register_table",
        lambda _s, spec, _r: e.registered.append(spec.name) or f"registered:{spec.name}",
    )
    return e


def test_default_run_gates_both_tables_into_their_dlq_and_registers_dlq(env: Env) -> None:
    out = DlqGateJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert [g["table"] for g in env.gated] == ["raw_customers", "raw_clickstream"]
    assert env.gated[0]["dlq_path"] == "s3a://lakehouse/dead_letter/dlq_customers"
    assert env.gated[1]["dlq_path"] == "s3a://lakehouse/dead_letter/dlq_clickstream"
    assert env.gated[0]["run_id"] == "run1"
    assert env.verified == ["raw_customers", "raw_clickstream"]
    assert env.registered == ["dlq_customers", "dlq_clickstream"]
    assert out["tables"]["raw_customers"]["dlq_catalog"] == ("registered:dlq_customers")


def test_named_tables_are_deduplicated_and_keep_their_order(env: Env) -> None:
    DlqGateJob().run(
        None,  # type: ignore[arg-type]
        _parse("--table", "raw_orders", "--table", "raw_products", "--table", "raw_orders"),
        _ctx(),
    )
    assert [g["table"] for g in env.gated] == ["raw_orders", "raw_products"]
    assert env.gated[0]["dlq_path"].endswith("dead_letter/dlq_orders")


def test_namespaced_run_is_path_addressed_and_not_registered(env: Env) -> None:
    out = DlqGateJob().run(None, _parse("--namespace", "ns1"), _ctx())  # type: ignore[arg-type]
    assert "/ns1/" in env.gated[0]["dlq_path"]
    assert env.gated[0]["bronze"].name is None
    assert env.registered == []
    assert "dlq_catalog" not in out["tables"]["raw_customers"]


def test_in_memory_catalog_is_not_registered(env: Env) -> None:
    DlqGateJob().run(None, _parse(), _ctx("in_memory"))  # type: ignore[arg-type]
    assert env.registered == []


def test_unregistered_bronze_table_fails_before_anything_is_written(env: Env) -> None:
    env.exists["s3a://lakehouse/bronze/raw_clickstream"] = False
    with pytest.raises(RuntimeError, match="not registered in the catalog"):
        DlqGateJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert env.gated == []
    assert env.verified == []


def test_missing_path_addressed_bronze_table_names_the_location(env: Env) -> None:
    env.exists["s3a://lakehouse/bronze/ns1/raw_customers"] = False
    with pytest.raises(RuntimeError, match="no Bronze table at s3a://lakehouse/bronze/ns1"):
        DlqGateJob().run(None, _parse("--namespace", "ns1"), _ctx())  # type: ignore[arg-type]
    assert env.gated == []


def test_failed_check_raises_after_registering_that_table_and_skips_the_rest(env: Env) -> None:
    env.problems["raw_customers"] = ["1 rejected rows are not in the DLQ table"]
    with pytest.raises(ReconciliationError, match=r"bronze\.raw_customers: 1 rejected"):
        DlqGateJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert [g["table"] for g in env.gated] == ["raw_customers"]
    assert env.registered == ["dlq_customers"]


def test_all_problems_of_a_table_are_joined_in_the_error(env: Env) -> None:
    env.problems["raw_customers"] = ["a", "b"]
    with pytest.raises(ReconciliationError, match="a; b"):
        DlqGateJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]


def test_reconciliation_error_is_a_runtime_error() -> None:
    assert issubclass(ReconciliationError, RuntimeError)


# ---- _write_and_check ---------------------------------------------------------------------------


class Rejected(Perm):
    def __init__(self, n: int, missing: int) -> None:
        super().__init__("rejected")
        self.n = n
        self.missing = missing
        self.wrote_empty = False

    def count(self) -> int:  # type: ignore[override]
        return self.n

    def limit(self, _n: int) -> Rejected:
        return self

    @property
    def write(self) -> Perm:  # type: ignore[override]
        self.wrote_empty = True
        return Perm("w")

    def join(self, *_a: Any, **_k: Any) -> Perm:
        return Perm("missing")

    def eqNullSafe(self, _o: Any) -> Perm:
        return Perm("eq")

    def __getitem__(self, _k: Any) -> Perm:
        return Perm("col")


class Frame:
    """Counts the DLQ table: ``before`` first, ``after`` on later loads; also the missing count."""

    def __init__(self, counts: list[int]) -> None:
        self.counts = counts
        self.i = 0

    def count(self) -> int:
        value = self.counts[min(self.i, len(self.counts) - 1)]
        self.i += 1
        return value

    def __getitem__(self, _k: Any) -> Perm:
        return Perm("col")


class SparkStub:
    def __init__(self, before: int, after: int) -> None:
        self.frames = [Frame([before]), Frame([after])]
        self.loaded = 0

    @property
    def read(self) -> SparkStub:
        return self

    def format(self, _f: str) -> SparkStub:
        return self

    def load(self, _p: str) -> Frame:
        frame = self.frames[min(self.loaded, 1)]
        self.loaded += 1
        return frame


class _Delta:
    exists = True

    @classmethod
    def isDeltaTable(cls, _s: Any, _p: str) -> bool:
        return cls.exists


def test_write_and_check_with_nothing_rejected_skips_the_merge_and_is_consistent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_spark.install(monkeypatch, delta_table=_Delta)
    merged: list[Any] = []
    monkeypatch.setattr(dlq_gate, "merge_dlq", lambda *_a: merged.append(1) or 99)
    rej = Rejected(0, 0)
    spark = SparkStub(before=5, after=5)
    # ``missing`` is the count of the anti-join; Perm.count() would not be an int, so stub join.
    rej.join = lambda *_a, **_k: Rejected(0, 0)  # type: ignore[method-assign]
    counts, problems = dlq_gate._write_and_check(spark, rej, "p")  # type: ignore[arg-type]
    assert merged == []
    assert problems == []
    assert counts == {
        "rejected_rows": 0,
        "dlq_inserted": 0,
        "dlq_rows_before": 5,
        "dlq_rows_after": 5,
        "dlq_missing": 0,
    }


def test_write_and_check_flags_missing_rows_and_inconsistent_growth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _Delta.exists = False  # first run: the empty table is created from the schema
    fake_spark.install(monkeypatch, delta_table=_Delta)
    monkeypatch.setattr(dlq_gate, "merge_dlq", lambda *_a: 3)
    rej = Rejected(3, 1)
    rej.join = lambda *_a, **_k: Rejected(1, 0)  # type: ignore[method-assign]
    spark = SparkStub(before=0, after=2)  # grew by 2 but the MERGE said 3
    counts, problems = dlq_gate._write_and_check(spark, rej, "p")  # type: ignore[arg-type]
    assert rej.wrote_empty
    assert counts["dlq_inserted"] == 3
    assert counts["dlq_missing"] == 1
    assert len(problems) == 2
    assert any("grew by 2 but the MERGE reported 3" in p for p in problems)
    assert any("1 rejected rows are not in the DLQ table" in p for p in problems)
    _Delta.exists = True
