"""Enumerations shared by the event and table contracts (DATA_CONTRACTS.md §2).

Member names equal their wire values on purpose (the contract lists them verbatim).
"""

from __future__ import annotations

from enum import StrEnum


class CdcAction(StrEnum):
    I = "I"  # noqa: E741 - wire value of the CDC insert operation
    U = "U"
    D = "D"


class LoyaltyTier(StrEnum):
    Bronze = "Bronze"
    Silver = "Silver"
    Gold = "Gold"
    Platinum = "Platinum"


class ClickEventType(StrEnum):
    page_view = "page_view"
    search = "search"
    add_to_cart = "add_to_cart"
    checkout = "checkout"
    purchase = "purchase"


class DeviceCategory(StrEnum):
    mobile = "mobile"
    desktop = "desktop"
    tablet = "tablet"


class OrderStatus(StrEnum):
    pending = "pending"
    completed = "completed"
    cancelled = "cancelled"
    returned = "returned"


class PaymentMethod(StrEnum):
    credit_card = "credit_card"
    paypal = "paypal"
    sepa_debit = "sepa_debit"
    apple_pay = "apple_pay"


class Currency(StrEnum):
    EUR = "EUR"
    USD = "USD"
    TRY = "TRY"


class SalesChannel(StrEnum):
    web = "web"
    app = "app"
    store = "store"


class ProductCategory(StrEnum):
    Apparel = "Apparel"
    Electronics = "Electronics"
    Home = "Home"
    Beauty = "Beauty"
