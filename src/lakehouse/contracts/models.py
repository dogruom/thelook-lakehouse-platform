"""Pydantic v2 contract models (DATA_CONTRACTS.md §2.0-2.5).

Models contain no ``uuid4()`` / ``now()`` defaults: identifiers and timestamps are supplied by the
(seeded) generator so that the same seed yields identical output. Regexes use ``[0-9]`` instead
of ``\\d`` because the Rust regex engine behind pydantic treats ``\\d`` as Unicode-aware.
"""

from __future__ import annotations

from datetime import UTC, date
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    StringConstraints,
    WithJsonSchema,
    model_validator,
)

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


BRAND_ID_PATTERN = r"^brand_[a-z0-9_]{1,40}$"
UUID_V4_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
PRODUCT_ID_PATTERN = r"^PRD-[0-9]{5}$"
PHONE_E164_PATTERN = r"^\+[1-9][0-9]{1,14}$"
COUNTRY_PATTERN = r"^[A-Z]{2}$"
MONEY_PATTERN = r"^[0-9]{1,8}\.[0-9]{2}$"
# Pragmatic local@domain.tld check, not full RFC 5322.
EMAIL_PATTERN = (
    r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+"
    r"@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$"
)

BrandId = Annotated[str, StringConstraints(pattern=BRAND_ID_PATTERN)]
UuidStr = Annotated[str, StringConstraints(pattern=UUID_V4_PATTERN)]


def _reject_float(value: Any) -> Any:
    """Money must never be built from binary floats (rounding errors)."""
    if isinstance(value, float | bool):
        raise ValueError("money must be a Decimal, int or str, not float/bool")
    return value


def _money_to_str(value: Decimal) -> str:
    return f"{value:.2f}"


Money = Annotated[
    Decimal,
    BeforeValidator(_reject_float),
    Field(max_digits=10, decimal_places=2),
    PlainSerializer(_money_to_str, return_type=str, when_used="json"),
    WithJsonSchema({"type": "string", "pattern": MONEY_PATTERN}, mode="serialization"),
]


class ContractModel(BaseModel):
    """Common configuration of every contract model."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class EventEnvelope(ContractModel):
    """Fields shared by all Kafka events (DATA_CONTRACTS.md §2.0)."""

    event_id: UuidStr
    brand_id: BrandId
    schema_version: Literal[1]
    produced_at: AwareDatetime


class CustomerCdcEvent(EventEnvelope):
    """``customer_cdc`` v1. Wire names of CDC metadata carry a leading underscore."""

    customer_id: UuidStr
    first_name: str = Field(min_length=1, max_length=50)
    last_name: str = Field(min_length=1, max_length=50)
    email: str = Field(pattern=EMAIL_PATTERN, max_length=254)
    phone: str | None = Field(default=None, pattern=PHONE_E164_PATTERN)
    city: str | None = None
    country: str = Field(pattern=COUNTRY_PATTERN)
    loyalty_tier: LoyaltyTier
    signup_date: date
    cdc_action: CdcAction = Field(alias="_cdc_action")
    cdc_sequence: int = Field(ge=1, alias="_cdc_sequence")
    cdc_ts: AwareDatetime = Field(alias="_cdc_ts")

    @model_validator(mode="after")
    def _signup_not_after_production(self) -> CustomerCdcEvent:
        if self.signup_date > self.produced_at.astimezone(UTC).date():
            raise ValueError("signup_date must not be after produced_at")
        return self


class ClickstreamEvent(EventEnvelope):
    """``clickstream_event`` v1."""

    session_id: UuidStr
    customer_id: UuidStr | None = None
    event_type: ClickEventType
    page_url: str | None = None
    device_category: DeviceCategory
    event_timestamp: AwareDatetime


class OrderItem(ContractModel):
    """Embedded order line item (DATA_CONTRACTS.md §2.3)."""

    item_id: UuidStr
    order_id: UuidStr
    brand_id: BrandId
    product_id: str = Field(pattern=PRODUCT_ID_PATTERN)
    quantity: int = Field(gt=0)
    unit_price: Annotated[Money, Field(gt=0)]
    line_total: Money

    @model_validator(mode="after")
    def _line_total_matches(self) -> OrderItem:
        if self.line_total != self.quantity * self.unit_price:
            raise ValueError("line_total must equal quantity * unit_price")
        return self


class OrderEvent(ContractModel):
    """``order_event`` v1 (file-landed; no Kafka envelope)."""

    schema_version: Literal[1]
    order_id: UuidStr
    brand_id: BrandId
    customer_id: UuidStr
    order_timestamp: AwareDatetime
    order_status: OrderStatus
    payment_method: PaymentMethod
    currency: Currency
    gross_amount: Annotated[Money, Field(gt=0)]
    discount_amount: Annotated[Money, Field(ge=0)]
    net_amount: Money
    items: list[OrderItem] = Field(min_length=1, max_length=5)
    sales_channel: SalesChannel | None = None

    @model_validator(mode="after")
    def _amounts_and_items_consistent(self) -> OrderEvent:
        if self.net_amount != self.gross_amount - self.discount_amount:
            raise ValueError("net_amount must equal gross_amount - discount_amount")
        for item in self.items:
            if item.brand_id != self.brand_id or item.order_id != self.order_id:
                raise ValueError("every item must carry the parent brand_id and order_id")
        return self


class Product(ContractModel):
    """``product`` v1 (static catalog seed)."""

    schema_version: Literal[1]
    product_id: str = Field(pattern=PRODUCT_ID_PATTERN)
    brand_id: BrandId
    product_name: str = Field(min_length=1, max_length=255)
    category: ProductCategory
    retail_price: Annotated[Money, Field(gt=0)]
    cost_price: Annotated[Money, Field(gt=0)] | None = None
    is_active: bool = True


CONTRACT_MODELS: dict[str, type[BaseModel]] = {
    "customer_cdc.v1": CustomerCdcEvent,
    "clickstream_event.v1": ClickstreamEvent,
    "order_event.v1": OrderEvent,
    "product.v1": Product,
}


def to_wire(model: BaseModel) -> dict[str, Any]:
    """Serialise a contract model to its JSON wire form (aliases, string money)."""
    return model.model_dump(mode="json", by_alias=True)
