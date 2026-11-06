"""Kafka key and header helpers (DATA_CONTRACTS.md §2.0).

They operate on raw payload mappings, not models, because injected-defect events bypass the models.
``brand_id`` is a header for routing only; partitioning is driven by the key.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Mapping


Header = tuple[str, bytes]


def _require(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if value is None or str(value) == "":
        raise ValueError(f"payload has no {field}; refusing to build Kafka key/headers")
    return str(value)


def cdc_key(payload: Mapping[str, Any]) -> str:
    """Message key ``{brand_id}:{customer_id}`` of a ``customer_cdc`` event."""
    return f"{_require(payload, 'brand_id')}:{_require(payload, 'customer_id')}"


def clickstream_key(payload: Mapping[str, Any]) -> str:
    """Message key ``{brand_id}:{session_id}``; a null/missing session yields ``{brand_id}:``.

    Such events are kept in Bronze and rejected to the DLQ downstream.
    """
    session_id = payload.get("session_id")
    return f"{_require(payload, 'brand_id')}:{'' if session_id is None else session_id}"


def _headers(payload: Mapping[str, Any], event_type: str) -> list[Header]:
    values = [
        ("brand_id", _require(payload, "brand_id")),
        ("schema_version", _require(payload, "schema_version")),
        ("event_type", event_type),
    ]
    return [(name, value.encode("utf-8")) for name, value in values]


def cdc_headers(payload: Mapping[str, Any]) -> list[Header]:
    """Headers ``brand_id``, ``schema_version``, ``event_type=customer_cdc``."""
    return _headers(payload, "customer_cdc")


def clickstream_headers(payload: Mapping[str, Any]) -> list[Header]:
    """Headers ``brand_id``, ``schema_version``, ``event_type`` (the clickstream event type)."""
    return _headers(payload, _require(payload, "event_type"))
