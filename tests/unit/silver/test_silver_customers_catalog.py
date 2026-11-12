"""Post-run catalog registration of silver.customers and ensure_table guards (offline, D-060)."""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from lakehouse.common.spark import SparkSettings
from lakehouse.common.tables import CatalogLocationMismatchError
from lakehouse.silver import customers
from lakehouse.silver.jobs import silver_customers as job


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []
    monkeypatch.setattr(job, "delta_exists_at_location", lambda *_: True)

    def fake_register(_spark: Any, _spec: Any, table: Any) -> str:
        seen.append(table.name)
        return "registered"

    monkeypatch.setattr(job, "register_table", fake_register)
    return seen


def test_hive_catalog_without_namespace_registers_silver_customers(calls: list[str]) -> None:
    result = job.register_silver_table(None, SparkSettings(catalog="hive"), None)  # type: ignore[arg-type]
    assert result == {"customers": "registered"}
    assert calls == ["silver.customers"]


@pytest.mark.parametrize(
    ("settings", "namespace"),
    [(SparkSettings(catalog="hive"), "itest_ns"), (SparkSettings(catalog="in_memory"), None)],
)
def test_namespaced_or_in_memory_runs_never_register(
    calls: list[str], settings: SparkSettings, namespace: str | None
) -> None:
    assert job.register_silver_table(None, settings, namespace) == {}  # type: ignore[arg-type]
    assert calls == []


def test_missing_delta_table_is_skipped(monkeypatch: pytest.MonkeyPatch, calls: list[str]) -> None:
    monkeypatch.setattr(job, "delta_exists_at_location", lambda *_: False)
    assert job.register_silver_table(None, SparkSettings(catalog="hive"), None) == {}  # type: ignore[arg-type]
    assert calls == []


def test_registration_failure_says_data_is_committed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(job, "delta_exists_at_location", lambda *_: True)

    def boom(*_a: Any) -> str:
        raise CatalogLocationMismatchError("silver.customers is registered at 'x'")

    monkeypatch.setattr(job, "register_table", boom)
    with pytest.raises(job.CatalogRegistrationError, match="Silver data is committed") as info:
        job.register_silver_table(None, SparkSettings(catalog="hive"), None)  # type: ignore[arg-type]
    assert "silver.customers" in str(info.value)
    assert isinstance(info.value.__cause__, CatalogLocationMismatchError)


def test_existence_probe_failure_is_also_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: Any) -> bool:
        raise OSError("s3 down")

    monkeypatch.setattr(job, "delta_exists_at_location", boom)
    with pytest.raises(job.CatalogRegistrationError, match="s3 down"):
        job.register_silver_table(None, SparkSettings(catalog="hive"), None)  # type: ignore[arg-type]


# ---- ensure_table with a fake delta module ------------------------------------------------------


class _Spark:
    def __init__(self) -> None:
        self.log: list[str] = []

    def createDataFrame(self, rows: Any, schema: Any) -> Any:  # noqa: ARG002
        self.log.append("create")
        return None

    def sql(self, statement: str) -> None:
        self.log.append(statement)


def _install_fake_delta(monkeypatch: pytest.MonkeyPatch, exists: bool) -> None:
    pkg = types.ModuleType("delta")
    tables = types.ModuleType("delta.tables")
    tables.DeltaTable = type(  # type: ignore[attr-defined]
        "DeltaTable", (), {"isDeltaTable": staticmethod(lambda _s, _p: exists)}
    )
    pkg.tables = tables  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "delta", pkg)
    monkeypatch.setitem(sys.modules, "delta.tables", tables)


def test_ensure_table_is_a_noop_when_table_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_delta(monkeypatch, True)
    spark = _Spark()
    customers.ensure_table(spark, "s3a://b/silver/customers")  # type: ignore[arg-type]
    assert spark.log == []


def test_ensure_table_rejects_backtick_paths_before_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_delta(monkeypatch, False)
    spark = _Spark()
    with pytest.raises(ValueError, match="backtick"):
        customers.ensure_table(spark, "s3a://b/si`lver")  # type: ignore[arg-type]
    assert spark.log == []


def test_outcomes_order_is_the_documented_gate_order() -> None:
    assert customers.OUTCOMES == (
        "parse_excluded",
        "dlq_excluded",
        "reread",
        "duplicate",
        "stale_noop",
        "accepted",
    )
