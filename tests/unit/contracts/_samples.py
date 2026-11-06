"""Valid wire-format sample payloads (fresh copies on every call) for the contract tests."""

from __future__ import annotations

from typing import Any


UUID_A = "11111111-1111-4111-8111-111111111111"
UUID_B = "22222222-2222-4222-9222-222222222222"
UUID_C = "33333333-3333-4333-a333-333333333333"
UUID_D = "44444444-4444-4444-b444-444444444444"
UUID_E = "55555555-5555-4555-8555-555555555555"


def customer() -> dict[str, Any]:
    return {
        "event_id": UUID_A,
        "brand_id": "brand_alpha",
        "schema_version": 1,
        "produced_at": "2026-03-01T10:00:00Z",
        "customer_id": UUID_B,
        "first_name": "Jane",
        "last_name": "Doe",
        "email": "jane.doe@example.com",
        "phone": "+4915112345678",
        "city": "Berlin",
        "country": "DE",
        "loyalty_tier": "Gold",
        "signup_date": "2026-02-01",
        "_cdc_action": "I",
        "_cdc_sequence": 1,
        "_cdc_ts": "2026-03-01T09:59:59Z",
    }


def click() -> dict[str, Any]:
    return {
        "event_id": UUID_A,
        "brand_id": "brand_beta",
        "schema_version": 1,
        "produced_at": "2026-03-01T10:00:00Z",
        "session_id": UUID_C,
        "customer_id": UUID_B,
        "event_type": "page_view",
        "page_url": "/products/PRD-00001",
        "device_category": "mobile",
        "event_timestamp": "2026-03-01T09:59:00Z",
    }


def item() -> dict[str, Any]:
    return {
        "item_id": UUID_D,
        "order_id": UUID_E,
        "brand_id": "brand_alpha",
        "product_id": "PRD-00001",
        "quantity": 2,
        "unit_price": "12.50",
        "line_total": "25.00",
    }


def order() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "order_id": UUID_E,
        "brand_id": "brand_alpha",
        "customer_id": UUID_B,
        "order_timestamp": "2026-03-01T09:00:00Z",
        "order_status": "completed",
        "payment_method": "credit_card",
        "currency": "EUR",
        "gross_amount": "25.00",
        "discount_amount": "5.00",
        "net_amount": "20.00",
        "items": [item()],
        "sales_channel": "web",
    }


def product() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "product_id": "PRD-00001",
        "brand_id": "brand_alpha",
        "product_name": "Trail Jacket",
        "category": "Apparel",
        "retail_price": "99.90",
        "cost_price": "40.00",
        "is_active": True,
    }


SAMPLES = {
    "customer_cdc.v1": customer,
    "clickstream_event.v1": click,
    "order_event.v1": order,
    "product.v1": product,
}
