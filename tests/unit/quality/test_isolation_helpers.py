"""Tenant isolation helpers against a tiny DataFrame stand-in (offline, no Spark)."""

from __future__ import annotations

from typing import Any

import pytest

from lakehouse.quality.isolation import (
    CrossTenantLeakError,
    assert_no_cross_tenant_rows,
    brand_counts,
)


class _Grouped:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def count(self) -> _Grouped:
        return self

    def collect(self) -> list[dict[str, Any]]:
        return self._rows


class _GroupDF:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def groupBy(self, col: str) -> _Grouped:
        assert col == "brand_id"
        return _Grouped(self._rows)


class _FilterDF:
    def __init__(self, leaked: int) -> None:
        self.leaked = leaked
        self.expr = ""

    def filter(self, expr: str) -> _FilterDF:
        self.expr = expr
        return self

    def count(self) -> int:
        return self.leaked


def test_brand_counts_maps_null_brand_to_marker_and_keeps_tenants_apart() -> None:
    rows = [
        {"brand_id": "b1", "count": 2},
        {"brand_id": "b2", "count": 5},
        {"brand_id": None, "count": 1},
    ]
    assert brand_counts(_GroupDF(rows)) == {"b1": 2, "b2": 5, "<null>": 1}  # type: ignore[arg-type]


def test_no_leak_passes_and_uses_null_safe_comparison() -> None:
    df = _FilterDF(0)
    assert_no_cross_tenant_rows(df, "b1", "b2")  # type: ignore[arg-type]
    assert "<=>" in df.expr


def test_leaked_rows_raise_with_count_and_pair() -> None:
    with pytest.raises(CrossTenantLeakError, match="3 joined rows"):
        assert_no_cross_tenant_rows(_FilterDF(3), "b1", "b2")  # type: ignore[arg-type]
