"""Post-ingest catalog registration of bronze_stream (offline; Spark calls are faked)."""

from __future__ import annotations

from typing import Any

import pytest

from lakehouse.common.spark import SparkSettings
from lakehouse.common.tables import CatalogLocationMismatchError
from lakehouse.ingestion.jobs import bronze_stream


TABLES = ["raw_customers", "raw_clickstream"]


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []
    monkeypatch.setattr(bronze_stream, "delta_exists_at_location", lambda *_: True)

    def fake_register(_spark: Any, spec: Any, table: Any) -> str:
        seen.append(table.name)
        return "registered"

    monkeypatch.setattr(bronze_stream, "register_table", fake_register)
    return seen


def test_hive_catalog_without_namespace_registers_every_table(calls: list[str]) -> None:
    result = bronze_stream.register_bronze_tables(None, SparkSettings(catalog="hive"), TABLES, None)
    assert result == {"raw_customers": "registered", "raw_clickstream": "registered"}
    assert calls == ["bronze.raw_customers", "bronze.raw_clickstream"]


@pytest.mark.parametrize(
    ("settings", "namespace"),
    [(SparkSettings(catalog="hive"), "itest_ns"), (SparkSettings(catalog="in_memory"), None)],
)
def test_namespaced_or_in_memory_runs_never_register(
    calls: list[str], settings: SparkSettings, namespace: str | None
) -> None:
    assert bronze_stream.register_bronze_tables(None, settings, TABLES, namespace) == {}
    assert calls == []


def test_missing_delta_table_is_skipped(monkeypatch: pytest.MonkeyPatch, calls: list[str]) -> None:
    monkeypatch.setattr(bronze_stream, "delta_exists_at_location", lambda *_: False)
    assert (
        bronze_stream.register_bronze_tables(None, SparkSettings(catalog="hive"), TABLES, None)
        == {}
    )
    assert calls == []


def test_registration_failure_says_the_ingest_is_committed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bronze_stream, "delta_exists_at_location", lambda *_: True)

    def boom(*_a: Any) -> str:
        raise CatalogLocationMismatchError("bronze.raw_customers is registered at 'x'")

    monkeypatch.setattr(bronze_stream, "register_table", boom)
    with pytest.raises(bronze_stream.CatalogRegistrationError, match="ingest committed") as info:
        bronze_stream.register_bronze_tables(None, SparkSettings(catalog="hive"), TABLES, None)
    assert isinstance(info.value.__cause__, CatalogLocationMismatchError)
