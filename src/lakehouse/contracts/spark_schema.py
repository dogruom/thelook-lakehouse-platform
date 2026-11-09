"""Spark ``StructType`` JSON derived from the contract models (pure Python, no PySpark).

The struct describes the **wire form** (``build_schema``: aliases, serialisation mode), so
Bronze/Silver can parse the Kafka payload with ``from_json`` using exactly what producers emit.
The result is plain JSON and is turned into a ``StructType`` by
``lakehouse.common.spark.struct_type_for``; this module stays importable on hosts without PySpark.

Design notes:

* Money fields stay ``"string"`` on purpose. They are decimal strings on the wire
  (``^[0-9]{1,8}\\.[0-9]{2}$``) and the typed ``DECIMAL(10,2)`` cast is a Silver step (W3).
  A plain ``cast("decimal(10,2)")`` does **not** enforce "exactly two decimals": it rounds
  ``12.505`` to ``12.51``, trims whitespace, accepts ``1e2`` and returns null on overflow or
  garbage. The Silver quality gate must therefore validate the money pattern *before* casting.
* ``from_json`` makes every parsed field nullable regardless of the schema, so required-ness is
  enforced by the quality gate, not by the ``StructType``. ``nullable`` here documents the
  contract; it is not a runtime guarantee.
* Patterns, lengths and ranges are contract-validation concerns (Pydantic / quality gate) and are
  deliberately not part of the Spark schema.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lakehouse.contracts.models import CONTRACT_MODELS
from lakehouse.contracts.schema import build_schema


if TYPE_CHECKING:
    from pydantic import BaseModel


_MAX_DEPTH = 16
_SCALAR_TYPES = {
    "string": "string",
    "integer": "long",
    "boolean": "boolean",
    "number": "double",
}


def spark_schema_json(model: type[BaseModel]) -> dict[str, Any]:
    """Spark StructType JSON of ``model``'s wire form (a Pydantic model class)."""
    schema = build_schema(model)
    spark_type, _ = _convert(schema, schema.get("$defs", {}), model.__name__, 0)
    if not isinstance(spark_type, dict) or spark_type.get("type") != "struct":
        raise ValueError(f"{model.__name__}: top-level schema is not an object")
    return spark_type


def spark_schema_json_for(name: str) -> dict[str, Any]:
    """Spark StructType JSON by ``CONTRACT_MODELS`` key; unknown names raise ``KeyError``."""
    return spark_schema_json(CONTRACT_MODELS[name])


def _convert(
    node: dict[str, Any], defs: dict[str, Any], path: str, depth: int
) -> tuple[str | dict[str, Any], bool]:
    """Return ``(spark type, nullable)`` for one JSON Schema node."""
    if depth > _MAX_DEPTH:
        raise ValueError(f"{path}: schema nesting deeper than {_MAX_DEPTH}")

    ref = node.get("$ref")
    if ref is not None:
        prefix = "#/$defs/"
        if not isinstance(ref, str) or not ref.startswith(prefix) or ref[len(prefix) :] not in defs:
            raise ValueError(f"{path}: unsupported or unresolved $ref {ref!r}")
        return _convert(defs[ref[len(prefix) :]], defs, path, depth + 1)

    if "anyOf" in node:
        branches = node["anyOf"]
        non_null = [b for b in branches if b.get("type") != "null"]
        if len(non_null) != 1:
            raise ValueError(f"{path}: anyOf must be one type plus null, got {len(non_null)} types")
        spark_type, nullable = _convert(non_null[0], defs, path, depth + 1)
        return spark_type, nullable or len(non_null) != len(branches)

    json_type = node.get("type")
    if json_type is None:
        json_type = _infer_type(node, path)
    if not isinstance(json_type, str):
        raise ValueError(f"{path}: unsupported type {json_type!r}")

    if json_type in _SCALAR_TYPES:
        return _SCALAR_TYPES[json_type], False
    if json_type == "array":
        items = node.get("items")
        if not isinstance(items, dict):
            raise ValueError(f"{path}: array without a single items schema")
        element, contains_null = _convert(items, defs, f"{path}[]", depth + 1)
        return {"type": "array", "elementType": element, "containsNull": contains_null}, False
    if json_type == "object":
        return _struct(node, defs, path, depth), False
    raise ValueError(f"{path}: unsupported JSON type {json_type!r}")


def _infer_type(node: dict[str, Any], path: str) -> str:
    """``enum``/``const`` without ``type``: infer from the Python value (bool before int)."""
    values = node.get("enum") or ([node["const"]] if "const" in node else [])
    kinds = {_value_type(v, path) for v in values}
    if len(kinds) != 1:
        raise ValueError(f"{path}: cannot determine a single type (no 'type', enum/const={values})")
    return kinds.pop()


def _value_type(value: Any, path: str) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, str):
        return "string"
    raise ValueError(f"{path}: unsupported enum/const value {value!r}")


def _struct(node: dict[str, Any], defs: dict[str, Any], path: str, depth: int) -> dict[str, Any]:
    properties = node.get("properties")
    if not isinstance(properties, dict) or not properties:
        raise ValueError(f"{path}: object without properties")
    required = set(node.get("required", []))
    fields: list[dict[str, Any]] = []
    for name, sub in properties.items():
        spark_type, nullable = _convert(sub, defs, f"{path}.{name}", depth + 1)
        fields.append(
            {
                "name": name,
                "type": spark_type,
                "nullable": nullable or name not in required,
                "metadata": {},
            }
        )
    return {"type": "struct", "fields": fields}
