"""Versioned event and table contracts shared by producers and consumers (ADR-0009).

Must not import any component package (enforced by import-linter).
"""

from lakehouse.contracts.enums import (
    CdcAction,
    ClickEventType,
    Currency,
    DeviceCategory,
    LoyaltyTier,
    OrderStatus,
    PaymentMethod,
    ProductCategory,
    SalesChannel,
)
from lakehouse.contracts.errors import safe_errors
from lakehouse.contracts.kafka import (
    cdc_headers,
    cdc_key,
    clickstream_headers,
    clickstream_key,
)
from lakehouse.contracts.models import (
    BRAND_ID_PATTERN,
    CONTRACT_MODELS,
    ClickstreamEvent,
    CustomerCdcEvent,
    EventEnvelope,
    Money,
    OrderEvent,
    OrderItem,
    Product,
    to_wire,
)
from lakehouse.contracts.schema import ChangeKind, build_schema, classify_change, export_schemas


__all__ = [
    "BRAND_ID_PATTERN",
    "CONTRACT_MODELS",
    "CdcAction",
    "ChangeKind",
    "ClickEventType",
    "ClickstreamEvent",
    "Currency",
    "CustomerCdcEvent",
    "DeviceCategory",
    "EventEnvelope",
    "LoyaltyTier",
    "Money",
    "OrderEvent",
    "OrderItem",
    "OrderStatus",
    "PaymentMethod",
    "Product",
    "ProductCategory",
    "SalesChannel",
    "build_schema",
    "cdc_headers",
    "cdc_key",
    "classify_change",
    "clickstream_headers",
    "clickstream_key",
    "export_schemas",
    "safe_errors",
    "to_wire",
]
