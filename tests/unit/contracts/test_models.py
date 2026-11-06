"""Constraint tests for the contract models, derived from DATA_CONTRACTS.md 2.0-2.5 and the W1.D3 card.

Every constraint has a valid case and at least one negative case that must fail.
"""

from __future__ import annotations

import copy
from decimal import Decimal
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from lakehouse.contracts import (
    CdcAction,
    ClickEventType,
    ClickstreamEvent,
    Currency,
    CustomerCdcEvent,
    DeviceCategory,
    LoyaltyTier,
    OrderEvent,
    OrderItem,
    OrderStatus,
    PaymentMethod,
    Product,
    ProductCategory,
    SalesChannel,
    safe_errors,
)

from . import _samples as s


MODEL_FOR = {
    "customer": (CustomerCdcEvent, s.customer),
    "click": (ClickstreamEvent, s.click),
    "item": (OrderItem, s.item),
    "order": (OrderEvent, s.order),
    "product": (Product, s.product),
}


def _case(kind: str, **changes: Any) -> tuple[type[BaseModel], dict[str, Any]]:
    model, factory = MODEL_FOR[kind]
    return model, {**factory(), **changes}


def _locs(exc: ValidationError) -> set[str]:
    return {str(part) for item in safe_errors(exc) for part in item["loc"]}


def _rejects(kind: str, *, model_level: bool = False, **changes: Any) -> None:
    """Assert rejection; unless model_level, the error loc must name the first changed field."""
    model, payload = _case(kind, **changes)
    with pytest.raises(ValidationError) as excinfo:
        model.model_validate(payload)
    assert safe_errors(excinfo.value), "no errors reported"
    if not model_level:
        expected = next(iter(changes))
        assert expected in _locs(excinfo.value), (expected, _locs(excinfo.value))


def _accepts(kind: str, **changes: Any) -> BaseModel:
    model, payload = _case(kind, **changes)
    return model.model_validate(payload)


# --------------------------------------------------------------------------- valid fixtures


@pytest.mark.parametrize("kind", list(MODEL_FOR))
def test_valid_fixture_is_accepted(kind: str) -> None:
    assert _accepts(kind) is not None


@pytest.mark.parametrize("kind", list(MODEL_FOR))
def test_unknown_field_is_rejected(kind: str) -> None:
    _rejects(kind, surprise="x")


@pytest.mark.parametrize("kind", list(MODEL_FOR))
def test_models_are_frozen(kind: str) -> None:
    model = _accepts(kind)
    with pytest.raises(ValidationError):
        model.brand_id = "brand_other"  # type: ignore[attr-defined]


def test_nested_order_item_unknown_field_is_rejected() -> None:
    payload = s.order()
    payload["items"][0]["surprise"] = 1
    with pytest.raises(ValidationError) as excinfo:
        OrderEvent.model_validate(payload)
    assert "surprise" in _locs(excinfo.value)


# --------------------------------------------------------------------------- enums

ENUM_VALUES = [
    (CdcAction, {"I", "U", "D"}),
    (LoyaltyTier, {"Bronze", "Silver", "Gold", "Platinum"}),
    (ClickEventType, {"page_view", "search", "add_to_cart", "checkout", "purchase"}),
    (DeviceCategory, {"mobile", "desktop", "tablet"}),
    (OrderStatus, {"pending", "completed", "cancelled", "returned"}),
    (PaymentMethod, {"credit_card", "paypal", "sepa_debit", "apple_pay"}),
    (Currency, {"EUR", "USD", "TRY"}),
    (SalesChannel, {"web", "app", "store"}),
    (ProductCategory, {"Apparel", "Electronics", "Home", "Beauty"}),
]


@pytest.mark.parametrize(("enum_cls", "expected"), ENUM_VALUES)
def test_enum_members_match_the_contract(enum_cls: Any, expected: set[str]) -> None:
    assert {member.value for member in enum_cls} == expected


# --------------------------------------------------------------------------- patterns / enums / lengths

BAD_BRANDS = ["Brand_x", "brand_", "alpha", "brand_UP", "brand-x", "brand_" + "a" * 41, ""]
BAD_UUIDS = [
    "not-a-uuid",
    "",
    "AAAAAAAA-1111-4111-8111-111111111111",  # uppercase hex is rejected
    "11111111-1111-1111-8111-111111111111",  # version nibble 1, not 4
    "11111111-1111-4111-c111-111111111111",  # variant nibble c
    s.UUID_A + "0",
]


@pytest.mark.parametrize("kind", ["customer", "click", "item", "order", "product"])
@pytest.mark.parametrize("brand", BAD_BRANDS)
def test_bad_brand_id_is_rejected(kind: str, brand: str) -> None:
    _rejects(kind, brand_id=brand)


@pytest.mark.parametrize("brand", ["brand_a", "brand_" + "a" * 40, "brand_x_1"])
def test_brand_id_boundaries_are_accepted(brand: str) -> None:
    _accepts("product", brand_id=brand)


@pytest.mark.parametrize(
    ("kind", "field"),
    [
        ("customer", "event_id"),
        ("customer", "customer_id"),
        ("click", "event_id"),
        ("click", "session_id"),
        ("click", "customer_id"),
        ("item", "item_id"),
        ("item", "order_id"),
        ("order", "order_id"),
        ("order", "customer_id"),
    ],
)
@pytest.mark.parametrize("bad", BAD_UUIDS)
def test_bad_uuid_is_rejected(kind: str, field: str, bad: str) -> None:
    _rejects(kind, **{field: bad})


@pytest.mark.parametrize("name", ["J", "J" * 50])
def test_name_length_boundaries_are_accepted(name: str) -> None:
    _accepts("customer", first_name=name, last_name=name)


@pytest.mark.parametrize("field", ["first_name", "last_name"])
@pytest.mark.parametrize("bad", ["", "J" * 51])
def test_name_length_violation_is_rejected(field: str, bad: str) -> None:
    _rejects("customer", **{field: bad})


@pytest.mark.parametrize("bad", ["user@", "missing-at.com", "", "a b@c.de", "@x.de", "a@b", None])
def test_invalid_email_is_rejected(bad: str | None) -> None:
    _rejects("customer", email=bad)


@pytest.mark.parametrize("phone", [None, "+4915112345678", "+12", "+" + "1" * 15])
def test_valid_or_null_phone_is_accepted(phone: str | None) -> None:
    _accepts("customer", phone=phone)


@pytest.mark.parametrize("bad", ["0049123", "+0123", "+1", "+" + "1" * 16, "12345", "+49 151", ""])
def test_non_e164_phone_is_rejected(bad: str) -> None:
    _rejects("customer", phone=bad)


def test_null_city_is_accepted() -> None:
    _accepts("customer", city=None)


@pytest.mark.parametrize("bad", ["de", "DEU", "D", "1A", "", None])
def test_bad_country_is_rejected(bad: str | None) -> None:
    _rejects("customer", country=bad)


@pytest.mark.parametrize(
    ("kind", "field", "bad"),
    [
        ("customer", "loyalty_tier", "Diamond"),
        ("customer", "_cdc_action", "X"),
        ("customer", "_cdc_action", "i"),
        ("click", "event_type", "click"),
        ("click", "device_category", "watch"),
        ("order", "order_status", "shipped"),
        ("order", "payment_method", "cash"),
        ("order", "currency", "GBP"),
        ("order", "sales_channel", "kiosk"),
        ("product", "category", "Toys"),
    ],
)
def test_value_outside_enum_is_rejected(kind: str, field: str, bad: str) -> None:
    _rejects(kind, **{field: bad})


@pytest.mark.parametrize("bad", [0, -1, None, "abc"])
def test_cdc_sequence_below_one_is_rejected(bad: object) -> None:
    _rejects("customer", _cdc_sequence=bad)


def test_cdc_sequence_one_is_accepted() -> None:
    _accepts("customer", _cdc_sequence=1)


@pytest.mark.parametrize("field", ["produced_at", "_cdc_ts"])
def test_naive_customer_timestamp_is_rejected(field: str) -> None:
    _rejects("customer", **{field: "2026-03-01T10:00:00"})


@pytest.mark.parametrize("field", ["produced_at", "event_timestamp"])
def test_naive_click_timestamp_is_rejected(field: str) -> None:
    _rejects("click", **{field: "2026-03-01T10:00:00"})


def test_naive_order_timestamp_is_rejected() -> None:
    _rejects("order", order_timestamp="2026-03-01T09:00:00")


def test_invalid_signup_date_is_rejected() -> None:
    _rejects("customer", signup_date="31/12/2025")


@pytest.mark.parametrize("version", [0, 2, "v1", None])
@pytest.mark.parametrize("kind", ["customer", "click", "order", "product"])
def test_schema_version_must_be_literal_one(kind: str, version: object) -> None:
    _rejects(kind, schema_version=version)


@pytest.mark.parametrize(
    ("kind", "field"),
    [
        ("customer", "event_id"),
        ("customer", "brand_id"),
        ("customer", "customer_id"),
        ("customer", "email"),
        ("customer", "country"),
        ("customer", "signup_date"),
        ("customer", "_cdc_action"),
        ("customer", "_cdc_sequence"),
        ("customer", "_cdc_ts"),
        ("customer", "produced_at"),
        ("click", "session_id"),
        ("click", "event_type"),
        ("click", "brand_id"),
        ("order", "order_id"),
        ("order", "brand_id"),
        ("order", "items"),
        ("order", "gross_amount"),
        ("order", "net_amount"),
        ("item", "brand_id"),
        ("item", "line_total"),
        ("product", "brand_id"),
        ("product", "retail_price"),
    ],
)
def test_missing_required_field_is_rejected(kind: str, field: str) -> None:
    model, payload = _case(kind)
    del payload[field]
    with pytest.raises(ValidationError) as excinfo:
        model.model_validate(payload)
    assert field in _locs(excinfo.value)


# --------------------------------------------------------------------------- cross-field rules


def test_signup_date_on_produced_date_is_accepted() -> None:
    _accepts("customer", signup_date="2026-03-01")


def test_signup_date_after_produced_date_is_rejected() -> None:
    _rejects("customer", model_level=True, signup_date="2026-03-02")


def test_clickstream_null_session_is_rejected_by_model() -> None:
    _rejects("click", session_id=None)


def test_clickstream_anonymous_user_and_null_url_are_accepted() -> None:
    _accepts("click", customer_id=None, page_url=None)


def test_net_amount_must_equal_gross_minus_discount() -> None:
    _rejects("order", model_level=True, net_amount="20.01")


def test_zero_discount_is_accepted() -> None:
    _accepts("order", discount_amount="0.00", net_amount="25.00")


@pytest.mark.parametrize(
    ("field", "bad"),
    [("gross_amount", "0.00"), ("gross_amount", "-25.00"), ("discount_amount", "-0.01")],
)
def test_order_amount_sign_violation_is_rejected(field: str, bad: str) -> None:
    _rejects("order", **{field: bad})


def test_order_without_sales_channel_defaults_to_none() -> None:
    payload = s.order()
    del payload["sales_channel"]
    assert OrderEvent.model_validate(payload).sales_channel is None


def _items(payload: dict[str, Any], count: int) -> list[dict[str, Any]]:
    items = [copy.deepcopy(payload["items"][0]) for _ in range(count)]
    for index, entry in enumerate(items):
        entry["item_id"] = f"aaaaaaaa-aaaa-4aaa-8aaa-{index:012d}"
    return items


@pytest.mark.parametrize("count", [0, 6])
def test_item_count_outside_one_to_five_is_rejected(count: int) -> None:
    payload = s.order()
    payload["items"] = _items(payload, count)
    with pytest.raises(ValidationError) as excinfo:
        OrderEvent.model_validate(payload)
    assert "items" in _locs(excinfo.value)


@pytest.mark.parametrize("count", [1, 5])
def test_item_count_boundaries_are_accepted(count: int) -> None:
    payload = s.order()
    payload["items"] = _items(payload, count)
    assert len(OrderEvent.model_validate(payload).items) == count


def test_item_with_foreign_brand_is_rejected() -> None:
    payload = s.order()
    payload["items"][0]["brand_id"] = "brand_beta"
    with pytest.raises(ValidationError) as excinfo:
        OrderEvent.model_validate(payload)
    assert safe_errors(excinfo.value)


def test_item_with_foreign_order_id_is_rejected() -> None:
    payload = s.order()
    payload["items"][0]["order_id"] = s.UUID_A
    with pytest.raises(ValidationError) as excinfo:
        OrderEvent.model_validate(payload)
    assert safe_errors(excinfo.value)


def test_line_total_must_equal_quantity_times_unit_price() -> None:
    _rejects("item", model_level=True, line_total="25.01")


@pytest.mark.parametrize("quantity", [0, -1])
def test_non_positive_quantity_is_rejected(quantity: int) -> None:
    _rejects("item", quantity=quantity, line_total="0.00")


@pytest.mark.parametrize("price", ["0.00", "-1.00"])
def test_non_positive_unit_price_is_rejected(price: str) -> None:
    _rejects("item", unit_price=price, line_total="0.00")


@pytest.mark.parametrize("bad", ["PRD-1234", "prd-12345", "PRD-123456", "PRD-ABCDE", "12345", ""])
def test_bad_product_id_is_rejected(bad: str) -> None:
    _rejects("item", product_id=bad)
    _rejects("product", product_id=bad)


# --------------------------------------------------------------------------- product


@pytest.mark.parametrize("name", ["P", "P" * 255])
def test_product_name_boundaries_are_accepted(name: str) -> None:
    _accepts("product", product_name=name)


@pytest.mark.parametrize("bad", ["", "P" * 256])
def test_product_name_length_violation_is_rejected(bad: str) -> None:
    _rejects("product", product_name=bad)


@pytest.mark.parametrize("field", ["retail_price", "cost_price"])
@pytest.mark.parametrize("bad", ["0.00", "-1.00"])
def test_non_positive_price_is_rejected(field: str, bad: str) -> None:
    _rejects("product", **{field: bad})


def test_null_cost_price_is_accepted() -> None:
    product = _accepts("product", cost_price=None)
    assert product.cost_price is None  # type: ignore[attr-defined]


def test_is_active_defaults_to_true() -> None:
    payload = s.product()
    del payload["is_active"]
    assert Product.model_validate(payload).is_active is True


# --------------------------------------------------------------------------- Money


def test_float_money_is_rejected() -> None:
    _rejects("order", gross_amount=25.0, net_amount=20.0, discount_amount=5.0)
    _rejects("product", retail_price=99.9)


@pytest.mark.parametrize("value", ["99.90", Decimal("99.90"), 100])
def test_str_decimal_and_int_money_are_accepted(value: object) -> None:
    product = _accepts("product", retail_price=value)
    assert product.retail_price == Decimal(str(value))  # type: ignore[attr-defined]


@pytest.mark.parametrize("bad", ["12.505", "123456789.00", "abc", "NaN", "Infinity"])
def test_money_precision_and_scale_violations_are_rejected(bad: str) -> None:
    _rejects("product", retail_price=bad)


def test_money_max_digits_boundary_is_accepted() -> None:
    _accepts("product", retail_price="99999999.99")


def test_money_serialises_as_two_decimal_json_string() -> None:
    wire = _accepts("product", retail_price=10, cost_price="12.5").model_dump(mode="json")
    assert wire["retail_price"] == "10.00"
    assert wire["cost_price"] == "12.50"


# --------------------------------------------------------------------------- privacy


def test_validation_error_text_does_not_leak_pii_input() -> None:
    email = "leaky.person@"
    first = "Zyxwv" * 12  # 60 chars -> length violation
    _, payload = _case("customer", email=email, first_name=first, last_name=first)
    with pytest.raises(ValidationError) as excinfo:
        CustomerCdcEvent.model_validate(payload)
    text = str(excinfo.value) + repr(excinfo.value)
    assert "leaky.person" not in text
    assert "Zyxwv" not in text
    assert excinfo.value.error_count() >= 3  # the errors really happened


# --------------------------------------------------------------------------- safe_errors


def _customer_error(**changes: Any) -> ValidationError:
    with pytest.raises(ValidationError) as excinfo:
        CustomerCdcEvent.model_validate({**s.customer(), **changes})
    return excinfo.value


def test_safe_errors_items_have_exactly_loc_and_type() -> None:
    errors = safe_errors(_customer_error(email="maria.schmidt@", first_name="Maximilian" * 6))

    assert errors
    assert all(set(item) == {"loc", "type"} for item in errors)


def test_safe_errors_never_contain_the_rejected_values() -> None:
    exc = _customer_error(email="maria.schmidt@", first_name="Maximilian" * 6, last_name="")

    rendered = " ".join(str(item) for item in safe_errors(exc))
    assert "maria" not in rendered
    assert "schmidt" not in rendered
    assert "Maximilian" not in rendered
    assert {"email", "first_name", "last_name"} <= _locs(exc)


def test_safe_errors_model_level_error_is_clean() -> None:
    exc = _customer_error(signup_date="2026-03-02")

    errors = safe_errors(exc)
    rendered = " ".join(str(item) for item in errors)
    assert errors
    assert all(set(item) == {"loc", "type"} for item in errors)
    assert "jane.doe" not in rendered
    assert "Jane" not in rendered
    assert "2026-03-02" not in rendered
