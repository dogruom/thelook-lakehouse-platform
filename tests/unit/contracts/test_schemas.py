"""JSON Schema snapshots and schema-change classification."""

from __future__ import annotations

import copy
import json
from importlib import resources
from typing import TYPE_CHECKING, Any

import pytest

from lakehouse.contracts import (
    CONTRACT_MODELS,
    ChangeKind,
    CustomerCdcEvent,
    build_schema,
    classify_change,
    export_schemas,
)


if TYPE_CHECKING:
    from pathlib import Path


def _committed(name: str) -> dict[str, Any]:
    ref = resources.files("lakehouse.contracts").joinpath("schemas", f"{name}.json")
    return json.loads(ref.read_text(encoding="utf-8"))


def test_contract_models_registry_has_exactly_the_four_contracts() -> None:
    assert set(CONTRACT_MODELS) == {
        "customer_cdc.v1",
        "clickstream_event.v1",
        "order_event.v1",
        "product.v1",
    }


@pytest.mark.parametrize("name", sorted(CONTRACT_MODELS))
def test_committed_schema_matches_model(name: str) -> None:
    assert build_schema(CONTRACT_MODELS[name]) == _committed(name)


def test_snapshot_comparison_can_fail() -> None:
    """Negative: schemas of different contracts are not interchangeable."""
    assert build_schema(CONTRACT_MODELS["customer_cdc.v1"]) != _committed("clickstream_event.v1")


def test_customer_schema_uses_aliases_not_field_names() -> None:
    props = build_schema(CustomerCdcEvent)["properties"]

    assert {"_cdc_action", "_cdc_sequence", "_cdc_ts"} <= props.keys()
    assert "cdc_action" not in props


def test_export_schemas_writes_deterministic_files(tmp_path: Path) -> None:
    written = export_schemas(tmp_path)

    assert sorted(p.name for p in written) == sorted(f"{n}.json" for n in CONTRACT_MODELS)
    for path in written:
        text = path.read_text(encoding="utf-8")
        parsed = json.loads(text)
        assert text == json.dumps(parsed, sort_keys=True, indent=2) + "\n"
        assert parsed == build_schema(CONTRACT_MODELS[path.stem])


def test_export_schemas_is_idempotent(tmp_path: Path) -> None:
    first = {p.name: p.read_bytes() for p in export_schemas(tmp_path)}
    second = {p.name: p.read_bytes() for p in export_schemas(tmp_path)}

    assert first == second


# --------------------------------------------------------------------------- classify_change

OLD: dict[str, Any] = {
    "type": "object",
    "properties": {
        "a": {"type": "string"},
        "b": {"type": "integer"},
        "c": {"type": "string", "enum": ["x", "y"]},
        "d": {"anyOf": [{"type": "string"}, {"type": "null"}]},
    },
    "required": ["a"],
}


def _new(mutate: Any) -> dict[str, Any]:
    schema = copy.deepcopy(OLD)
    mutate(schema)
    return schema


def test_identical_schemas_are_none() -> None:
    assert classify_change(OLD, copy.deepcopy(OLD)) is ChangeKind.NONE


def test_new_optional_property_is_additive() -> None:
    new = _new(lambda sc: sc["properties"].update(e={"type": "string"}))

    assert classify_change(OLD, new) is ChangeKind.ADDITIVE


def test_removed_property_is_breaking() -> None:
    new = _new(lambda sc: sc["properties"].pop("b"))

    assert classify_change(OLD, new) is ChangeKind.BREAKING


def test_changed_property_type_is_breaking() -> None:
    new = _new(lambda sc: sc["properties"]["b"].update(type="string"))

    assert classify_change(OLD, new) is ChangeKind.BREAKING


def test_changed_anyof_type_is_breaking() -> None:
    new = _new(
        lambda sc: sc["properties"].update(d={"anyOf": [{"type": "integer"}, {"type": "null"}]})
    )

    assert classify_change(OLD, new) is ChangeKind.BREAKING


@pytest.mark.parametrize("values", [["x", "y", "z"], ["x"], ["x", "q"]])
def test_any_enum_change_is_breaking(values: list[str]) -> None:
    new = _new(lambda sc: sc["properties"]["c"].update(enum=values))

    assert classify_change(OLD, new) is ChangeKind.BREAKING


def test_new_required_property_is_breaking() -> None:
    def mutate(sc: dict[str, Any]) -> None:
        sc["properties"]["e"] = {"type": "string"}
        sc["required"].append("e")

    assert classify_change(OLD, _new(mutate)) is ChangeKind.BREAKING


def test_optional_to_required_is_breaking() -> None:
    new = _new(lambda sc: sc["required"].append("b"))

    assert classify_change(OLD, new) is ChangeKind.BREAKING


def test_real_schema_unchanged_is_none() -> None:
    schema = build_schema(CustomerCdcEvent)

    assert classify_change(schema, copy.deepcopy(schema)) is ChangeKind.NONE


def test_removing_a_field_from_real_schema_is_breaking() -> None:
    old = build_schema(CustomerCdcEvent)
    new = copy.deepcopy(old)
    del new["properties"]["city"]

    assert classify_change(old, new) is ChangeKind.BREAKING


def test_adding_optional_field_to_real_schema_is_additive() -> None:
    old = build_schema(CustomerCdcEvent)
    new = copy.deepcopy(old)
    new["properties"]["marketing_opt_in"] = {"type": "boolean"}

    assert classify_change(old, new) is ChangeKind.ADDITIVE


# --------------------------------------------------------------------------- real order schema


@pytest.fixture
def order_schema() -> dict[str, Any]:
    return copy.deepcopy(_committed("order_event.v1"))


def test_real_order_schema_unchanged_is_none(order_schema: dict[str, Any]) -> None:
    assert classify_change(_committed("order_event.v1"), order_schema) is ChangeKind.NONE


def test_new_required_field_in_order_item_def_is_breaking(order_schema: dict[str, Any]) -> None:
    item = order_schema["$defs"]["OrderItem"]
    item["properties"]["discount_code"] = {"type": "string"}
    item["required"].append("discount_code")

    assert classify_change(_committed("order_event.v1"), order_schema) is ChangeKind.BREAKING


def test_removed_order_item_quantity_is_breaking(order_schema: dict[str, Any]) -> None:
    item = order_schema["$defs"]["OrderItem"]
    del item["properties"]["quantity"]
    item["required"].remove("quantity")

    assert classify_change(_committed("order_event.v1"), order_schema) is ChangeKind.BREAKING


def test_order_timestamp_format_change_is_breaking(order_schema: dict[str, Any]) -> None:
    order_schema["properties"]["order_timestamp"]["format"] = "date"

    assert classify_change(_committed("order_event.v1"), order_schema) is ChangeKind.BREAKING


def test_pattern_change_is_breaking(order_schema: dict[str, Any]) -> None:
    order_schema["properties"]["brand_id"]["pattern"] = "^brand_[a-z]{1,40}$"

    assert classify_change(_committed("order_event.v1"), order_schema) is ChangeKind.BREAKING


def test_items_max_items_change_is_breaking(order_schema: dict[str, Any]) -> None:
    order_schema["properties"]["items"]["maxItems"] = 10

    assert classify_change(_committed("order_event.v1"), order_schema) is ChangeKind.BREAKING


def test_optional_nested_property_in_order_item_is_additive(order_schema: dict[str, Any]) -> None:
    order_schema["$defs"]["OrderItem"]["properties"]["gift_wrap"] = {"type": "boolean"}

    assert classify_change(_committed("order_event.v1"), order_schema) is ChangeKind.ADDITIVE


def test_money_field_pattern_change_is_breaking(order_schema: dict[str, Any]) -> None:
    base = _committed("order_event.v1")
    order_schema["properties"]["gross_amount"]["pattern"] = r"^\d{1,8}\.\d{2}$"
    assert classify_change(base, order_schema) is ChangeKind.BREAKING

    changed = copy.deepcopy(order_schema)
    changed["properties"]["gross_amount"]["pattern"] = r"^\d{1,7}\.\d{2}$"
    assert classify_change(order_schema, changed) is ChangeKind.BREAKING
