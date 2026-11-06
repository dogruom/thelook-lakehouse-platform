"""Alias/wire behaviour and Kafka key/header helpers (raw-dict based, so defects can be keyed)."""

from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from lakehouse.contracts import (
    CONTRACT_MODELS,
    CustomerCdcEvent,
    cdc_headers,
    cdc_key,
    clickstream_headers,
    clickstream_key,
    safe_errors,
    to_wire,
)

from . import _samples as s


# --------------------------------------------------------------------------- wire / alias


def test_to_wire_emits_underscore_prefixed_cdc_keys() -> None:
    wire = to_wire(CustomerCdcEvent.model_validate(s.customer()))

    assert {"_cdc_action", "_cdc_sequence", "_cdc_ts"} <= wire.keys()
    assert not {"cdc_action", "cdc_sequence", "cdc_ts"} & wire.keys()
    assert wire["_cdc_action"] == "I"
    assert wire["_cdc_sequence"] == 1


@pytest.mark.parametrize(
    ("alias", "plain"),
    [("_cdc_action", "cdc_action"), ("_cdc_sequence", "cdc_sequence"), ("_cdc_ts", "cdc_ts")],
)
def test_unprefixed_cdc_field_name_is_rejected_as_input(alias: str, plain: str) -> None:
    payload = s.customer()
    payload[plain] = payload.pop(alias)

    with pytest.raises(ValidationError) as excinfo:
        CustomerCdcEvent.model_validate(payload)
    locs = {str(part) for item in safe_errors(excinfo.value) for part in item["loc"]}
    assert alias in locs or plain in locs


def test_both_alias_and_plain_name_together_are_rejected() -> None:
    payload = {**s.customer(), "cdc_action": "U"}

    with pytest.raises(ValidationError) as excinfo:
        CustomerCdcEvent.model_validate(payload)
    locs = {str(part) for item in safe_errors(excinfo.value) for part in item["loc"]}
    assert "cdc_action" in locs


@pytest.mark.parametrize("name", list(CONTRACT_MODELS))
def test_round_trip_to_wire_then_validate_is_equal(name: str) -> None:
    model_cls: type[BaseModel] = CONTRACT_MODELS[name]
    original = model_cls.model_validate(s.SAMPLES[name]())

    restored = model_cls.model_validate(to_wire(original))

    assert restored == original


@pytest.mark.parametrize("name", list(CONTRACT_MODELS))
def test_wire_form_is_plain_json_serialisable(name: str) -> None:
    model = CONTRACT_MODELS[name].model_validate(s.SAMPLES[name]())

    assert json.loads(json.dumps(to_wire(model))) == to_wire(model)


def test_wire_money_is_two_decimal_string_and_dates_are_strings() -> None:
    wire = to_wire(CONTRACT_MODELS["order_event.v1"].model_validate(s.order()))

    assert wire["gross_amount"] == "25.00"
    assert wire["items"][0]["unit_price"] == "12.50"
    assert isinstance(wire["order_timestamp"], str)


def test_wire_customer_date_fields_are_strings() -> None:
    wire = to_wire(CustomerCdcEvent.model_validate(s.customer()))

    assert wire["signup_date"] == "2026-02-01"
    assert isinstance(wire["produced_at"], str)


def test_round_trip_detects_a_mutated_value() -> None:
    """Negative: the equality used above is able to fail."""
    original = CustomerCdcEvent.model_validate(s.customer())
    wire = to_wire(original)
    wire["_cdc_sequence"] = 2

    assert CustomerCdcEvent.model_validate(wire) != original


# --------------------------------------------------------------------------- Kafka helpers


def test_cdc_key_is_brand_and_customer() -> None:
    assert cdc_key(s.customer()) == f"brand_alpha:{s.UUID_B}"


def test_cdc_key_differs_across_brands_for_same_customer() -> None:
    other = {**s.customer(), "brand_id": "brand_beta"}

    assert cdc_key(other) != cdc_key(s.customer())


def test_clickstream_key_is_brand_and_session() -> None:
    assert clickstream_key(s.click()) == f"brand_beta:{s.UUID_C}"


@pytest.mark.parametrize("variant", ["null", "missing"])
def test_clickstream_key_without_session_is_brand_with_empty_suffix(variant: str) -> None:
    payload = s.click()
    if variant == "null":
        payload["session_id"] = None
    else:
        del payload["session_id"]

    assert clickstream_key(payload) == "brand_beta:"


def test_cdc_headers_are_utf8_bytes_with_customer_cdc_event_type() -> None:
    assert cdc_headers(s.customer()) == [
        ("brand_id", b"brand_alpha"),
        ("schema_version", b"1"),
        ("event_type", b"customer_cdc"),
    ]


def test_clickstream_headers_use_payload_event_type() -> None:
    assert clickstream_headers(s.click()) == [
        ("brand_id", b"brand_beta"),
        ("schema_version", b"1"),
        ("event_type", b"page_view"),
    ]


def test_headers_work_on_defective_raw_payloads() -> None:
    """Defect events bypass the models; helpers must still key them."""
    bad = {**s.click(), "session_id": None, "event_type": "not_an_event", "page_url": 5}

    assert clickstream_key(bad) == "brand_beta:"
    assert dict(clickstream_headers(bad))["event_type"] == b"not_an_event"


@pytest.mark.parametrize("func", [cdc_headers, clickstream_headers])
def test_headers_without_brand_id_raise_value_error(func: Any) -> None:
    payload = s.customer() if func is cdc_headers else s.click()
    del payload["brand_id"]

    with pytest.raises(ValueError):  # - message is not part of the contract
        func(payload)
