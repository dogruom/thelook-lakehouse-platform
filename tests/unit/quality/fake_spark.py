"""A permissive stand-in for PySpark so expression builders can run on a host without it.

``Perm`` accepts any attribute access, call, operator or subscript and returns another ``Perm``.
It proves that building an expression raises no Python error; it does not evaluate Spark semantics
(the Spark suite does that).
"""

from __future__ import annotations

import sys
import types
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    import pytest


class Perm:
    def __init__(self, label: str = "perm") -> None:
        self._label = label

    def __getattr__(self, name: str) -> Perm:
        if name.startswith("__"):
            raise AttributeError(name)
        return Perm(f"{self._label}.{name}")

    def __call__(self, *_a: Any, **_k: Any) -> Perm:
        return Perm(f"{self._label}()")

    def __getitem__(self, _k: Any) -> Perm:
        return Perm(f"{self._label}[]")

    def __repr__(self) -> str:
        return f"<Perm {self._label}>"


def _binary(_self: Perm, _other: Any = None) -> Perm:
    return Perm("op")


for _op in (
    "add radd sub rsub mul rmul truediv rtruediv mod rmod pow rpow and rand or ror xor rxor "
    "lt le gt ge neg invert"
).split():
    setattr(Perm, f"__{_op}__", _binary)


def install(monkeypatch: pytest.MonkeyPatch, delta_table: Any | None = None) -> None:
    """Register fake ``pyspark``, ``pyspark.sql(.functions/.types)`` and ``delta.tables``."""
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = Perm("F")
    types_mod = Perm("T")
    sql.functions = functions  # type: ignore[attr-defined]
    sql.types = types_mod  # type: ignore[attr-defined]
    pyspark.sql = sql  # type: ignore[attr-defined]
    pyspark.StorageLevel = Perm("StorageLevel")  # type: ignore[attr-defined]
    for name, mod in (
        ("pyspark", pyspark),
        ("pyspark.sql", sql),
        ("pyspark.sql.functions", functions),
        ("pyspark.sql.types", types_mod),
    ):
        monkeypatch.setitem(sys.modules, name, mod)
    if delta_table is not None:
        delta = types.ModuleType("delta")
        tables = types.ModuleType("delta.tables")
        tables.DeltaTable = delta_table  # type: ignore[attr-defined]
        delta.tables = tables  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "delta", delta)
        monkeypatch.setitem(sys.modules, "delta.tables", tables)
