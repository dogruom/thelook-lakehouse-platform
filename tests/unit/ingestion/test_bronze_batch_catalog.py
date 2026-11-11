"""Post-ingest catalog registration of bronze_batch (offline; Spark calls are faked, D-060)."""

from __future__ import annotations

from typing import Any

import pytest

from lakehouse.common.spark import SparkSettings
from lakehouse.common.tables import CatalogLocationMismatchError
from lakehouse.ingestion.batch import ORDERS, PRODUCTS
from lakehouse.ingestion.jobs import bronze_batch
from lakehouse.ingestion.jobs.bronze_stream import CatalogRegistrationError


TARGETS = (ORDERS, PRODUCTS)


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []
    monkeypatch.setattr(bronze_batch, "delta_exists_at_location", lambda *_: True)

    def fake_register(_spark: Any, spec: Any, table: Any) -> str:
        seen.append(table.name)
        return "registered"

    monkeypatch.setattr(bronze_batch, "register_table", fake_register)
    return seen


def test_hive_catalog_without_namespace_registers_both_tables(calls: list[str]) -> None:
    result = bronze_batch.register_batch_tables(None, SparkSettings(catalog="hive"), TARGETS, None)  # type: ignore[arg-type]
    assert result == {"raw_orders": "registered", "raw_products": "registered"}
    assert calls == ["bronze.raw_orders", "bronze.raw_products"]


@pytest.mark.parametrize(
    ("settings", "namespace"),
    [(SparkSettings(catalog="hive"), "itest_ns"), (SparkSettings(catalog="in_memory"), None)],
)
def test_namespaced_or_in_memory_runs_never_register(
    calls: list[str], settings: SparkSettings, namespace: str | None
) -> None:
    assert bronze_batch.register_batch_tables(None, settings, TARGETS, namespace) == {}  # type: ignore[arg-type]
    assert calls == []


def test_missing_delta_table_is_skipped(monkeypatch: pytest.MonkeyPatch, calls: list[str]) -> None:
    monkeypatch.setattr(bronze_batch, "delta_exists_at_location", lambda *_: False)
    assert (
        bronze_batch.register_batch_tables(None, SparkSettings(catalog="hive"), TARGETS, None)  # type: ignore[arg-type]
        == {}
    )
    assert calls == []


def test_only_existing_tables_are_registered(
    monkeypatch: pytest.MonkeyPatch, calls: list[str]
) -> None:
    monkeypatch.setattr(
        bronze_batch, "delta_exists_at_location", lambda _s, t: t.name == "bronze.raw_orders"
    )
    result = bronze_batch.register_batch_tables(None, SparkSettings(catalog="hive"), TARGETS, None)  # type: ignore[arg-type]
    assert result == {"raw_orders": "registered"}


def test_registration_failure_says_the_ingest_is_committed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bronze_batch, "delta_exists_at_location", lambda *_: True)

    def boom(*_a: Any) -> str:
        raise CatalogLocationMismatchError("bronze.raw_orders is registered at 'x'")

    monkeypatch.setattr(bronze_batch, "register_table", boom)
    with pytest.raises(CatalogRegistrationError, match="ingest committed") as info:
        bronze_batch.register_batch_tables(None, SparkSettings(catalog="hive"), TARGETS, None)  # type: ignore[arg-type]
    assert isinstance(info.value.__cause__, CatalogLocationMismatchError)


def test_existence_probe_failure_is_also_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: Any) -> bool:
        raise OSError("s3 down")

    monkeypatch.setattr(bronze_batch, "delta_exists_at_location", boom)
    with pytest.raises(CatalogRegistrationError, match="s3 down"):
        bronze_batch.register_batch_tables(None, SparkSettings(catalog="hive"), TARGETS, None)  # type: ignore[arg-type]
