"""Spark StructType JSON derived from the wire-form JSON Schema (pure Python, no pyspark)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from lakehouse.contracts.models import CONTRACT_MODELS, CustomerCdcEvent, OrderEvent, Product
from lakehouse.contracts.schema import build_schema
from lakehouse.contracts.spark_schema import spark_schema_json, spark_schema_json_for


def _fields(struct: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {f["name"]: f for f in struct["fields"]}


@pytest.mark.parametrize("name", sorted(CONTRACT_MODELS))
def test_every_contract_yields_a_struct_in_wire_property_order(name: str) -> None:
    """Field names/order must follow the wire schema so from_json column order is stable."""
    struct = spark_schema_json_for(name)
    assert struct["type"] == "struct"
    props = list(build_schema(CONTRACT_MODELS[name])["properties"])
    assert [f["name"] for f in struct["fields"]] == props
    for f in struct["fields"]:
        assert set(f) >= {"name", "type", "nullable", "metadata"}
        assert f["metadata"] == {}


def test_customer_cdc_uses_wire_aliases_and_exact_types() -> None:
    """CDC metadata keeps its underscore wire names; sequence is long; required stays non-null."""
    f = _fields(spark_schema_json(CustomerCdcEvent))
    assert f["_cdc_sequence"]["type"] == "long"
    assert f["_cdc_action"]["type"] == "string"
    assert "cdc_sequence" not in f
    assert f["phone"]["type"] == "string" and f["phone"]["nullable"] is True
    assert f["event_id"]["type"] == "string" and f["event_id"]["nullable"] is False
    assert f["brand_id"]["nullable"] is False
    assert f["schema_version"]["type"] == "long"
    assert f["signup_date"]["type"] == "string"


def test_order_items_are_array_of_struct_and_money_is_string() -> None:
    """Money stays string on the wire; items become array<struct> without null elements."""
    f = _fields(spark_schema_json(OrderEvent))
    items = f["items"]["type"]
    assert items["type"] == "array" and items["containsNull"] is False
    elem = items["elementType"]
    assert elem["type"] == "struct"
    ef = _fields(elem)
    assert ef["quantity"]["type"] == "long"
    assert ef["unit_price"]["type"] == "string" and ef["line_total"]["type"] == "string"
    assert ef["brand_id"]["nullable"] is False
    for money in ("gross_amount", "discount_amount", "net_amount"):
        assert f[money]["type"] == "string"
    assert f["sales_channel"]["nullable"] is True


def test_product_is_active_boolean_and_optional_cost_nullable() -> None:
    f = _fields(spark_schema_json(Product))
    assert f["is_active"]["type"] == "boolean"
    assert f["cost_price"]["type"] == "string" and f["cost_price"]["nullable"] is True


def test_unknown_contract_name_raises_key_error() -> None:
    with pytest.raises(KeyError):
        spark_schema_json_for("nope.v9")


def test_union_of_two_non_null_types_is_rejected_naming_the_field() -> None:
    """Silent guessing of ambiguous types would corrupt Bronze parsing; fail loudly."""

    class Bad(BaseModel):
        mixed: int | str

    with pytest.raises(ValueError, match="mixed"):
        spark_schema_json(Bad)


def test_schema_without_type_is_rejected() -> None:
    class Untyped(BaseModel):
        anything: Any

    with pytest.raises(ValueError, match="anything"):
        spark_schema_json(Untyped)
