"""JSON Schema generation and compatibility classification for the contract models."""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from lakehouse.contracts.models import CONTRACT_MODELS


if TYPE_CHECKING:
    from pydantic import BaseModel


SCHEMA_DIR = Path(__file__).parent / "schemas"


def build_schema(model: type[BaseModel]) -> dict[str, Any]:
    """JSON Schema of the wire form (aliases, string money)."""
    return model.model_json_schema(by_alias=True, mode="serialization")


def export_schemas(out_dir: Path) -> list[Path]:
    """Write ``<name>.json`` snapshots (sorted keys, LF) for every contract model."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for name, model in CONTRACT_MODELS.items():
        path = out_dir / f"{name}.json"
        text = json.dumps(build_schema(model), sort_keys=True, indent=2) + "\n"
        path.write_text(text, encoding="utf-8", newline="\n")
        written.append(path)
    return written


class ChangeKind(Enum):
    NONE = "none"
    ADDITIVE = "additive"
    BREAKING = "breaking"


_KEYWORDS = (
    "format",
    "pattern",
    "enum",
    "const",
    "minItems",
    "maxItems",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
)
_MAX_DEPTH = 16


class _Facts(NamedTuple):
    types: frozenset[str]
    keywords: dict[str, frozenset[str]]
    properties: frozenset[str]
    required: frozenset[str]


def _leaves(
    nodes: list[dict[str, Any]], defs: dict[str, Any], depth: int = 0
) -> list[dict[str, Any]]:
    """Resolve local ``$ref`` and flatten ``anyOf`` into non-composite branch nodes."""
    out: list[dict[str, Any]] = []
    for node in nodes:
        ref = node.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/$defs/") and depth < _MAX_DEPTH:
            target = defs.get(ref.removeprefix("#/$defs/"), {})
            out.extend(_leaves([target], defs, depth + 1))
        elif "anyOf" in node and depth < _MAX_DEPTH:
            out.extend(_leaves(list(node["anyOf"]), defs, depth + 1))
        else:
            out.append(node)
    return out


def _collect(
    nodes: list[dict[str, Any]],
    defs: dict[str, Any],
    path: str,
    facts: dict[str, _Facts],
    depth: int = 0,
) -> None:
    if depth > _MAX_DEPTH:
        return
    leaves = _leaves(nodes, defs)
    types: set[str] = set()
    keywords: dict[str, set[str]] = {}
    required: set[str] = set()
    children: dict[str, list[dict[str, Any]]] = {}
    items: list[dict[str, Any]] = []
    for leaf in leaves:
        leaf_type = leaf.get("type")
        if isinstance(leaf_type, list):
            types.update(str(x) for x in leaf_type)
        elif leaf_type is not None:
            types.add(str(leaf_type))
        for key in _KEYWORDS:
            if key in leaf:
                values = leaf[key] if key == "enum" else [leaf[key]]
                keywords.setdefault(key, set()).update(
                    json.dumps(v, sort_keys=True) for v in values
                )
        required.update(leaf.get("required", []))
        for name, sub in leaf.get("properties", {}).items():
            children.setdefault(name, []).append(sub)
        if isinstance(leaf.get("items"), dict):
            items.append(leaf["items"])
    facts[path] = _Facts(
        frozenset(types),
        {k: frozenset(v) for k, v in keywords.items()},
        frozenset(children),
        frozenset(required),
    )
    for name, subs in children.items():
        _collect(subs, defs, f"{path}.{name}", facts, depth + 1)
    if items:
        _collect(items, defs, f"{path}[]", facts, depth + 1)


def _facts(schema: dict[str, Any]) -> dict[str, _Facts]:
    facts: dict[str, _Facts] = {}
    _collect([schema], schema.get("$defs", {}), "$", facts)
    return facts


def classify_change(old: dict[str, Any], new: dict[str, Any]) -> ChangeKind:
    """Classify a JSON Schema change as NONE, ADDITIVE or BREAKING.

    Scope: the schema is walked recursively through ``properties``, array ``items``, local
    ``$ref`` (``#/$defs/...``) and direct ``anyOf`` (flattened, so ``X | None`` is read as the
    union of its branches). At every path these are compared: ``type``, ``format``, ``pattern``,
    ``enum``, ``const``, ``minItems``/``maxItems``, ``minLength``/``maxLength``,
    ``minimum``/``maximum`` and ``exclusiveMinimum``/``exclusiveMaximum``, plus the property
    set and the ``required`` set (order irrelevant). Anything else (``allOf``/``oneOf``,
    ``default``, ``description``, ``title``, ``additionalProperties``) is ignored.

    BREAKING: a property or array ``items`` removed; any compared keyword changed in either
    direction; a newly required property; optional -> required.
    ADDITIVE: a new optional property at any depth; required -> optional. Otherwise NONE.
    """
    old_facts = _facts(old)
    new_facts = _facts(new)
    additive = False
    for path, before in old_facts.items():
        after = new_facts.get(path)
        if after is None:  # property / items removed
            return ChangeKind.BREAKING
        if before.types != after.types or before.keywords != after.keywords:
            return ChangeKind.BREAKING
        if after.required - before.required:
            return ChangeKind.BREAKING
        additive = additive or bool(before.required - after.required)
    additive = additive or bool(set(new_facts) - set(old_facts))
    return ChangeKind.ADDITIVE if additive else ChangeKind.NONE
