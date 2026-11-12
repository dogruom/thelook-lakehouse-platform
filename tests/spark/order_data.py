"""Shared builders for the W3.D17 Spark tests: landed order/product lines as Bronze rows.

Lines are built from the DATA_CONTRACTS section 2.2-2.4 wire format (strings for money and
timestamps, D-045), not from the implementation. ``bronze_files`` goes through the production
``to_bronze_rows`` of ``lakehouse.ingestion.batch`` so ``_source_ref``/``brand_id`` are realistic.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from lakehouse.ingestion.batch import BatchTarget


ALPHA, BETA = "brand_alpha", "brand_beta"
DAY = "2026-03-01"
ABSENT = object()  # drop the field from the JSON object
BASE_TS = "2026-03-01T10:00:00Z"
FUTURE_TS = "2099-01-01T10:00:00Z"


def uid(n: int) -> str:
    """Deterministic lowercase UUIDv4 string."""
    return f"{n:08x}-0000-4000-8000-000000000000"


C1, C2, C3 = uid(0xC1), uid(0xC2), uid(0xC3)
O1, O2, O3, O4, O5 = (uid(0xA0 + i) for i in range(1, 6))
P1, P2, P3 = "PRD-00001", "PRD-00002", "PRD-00003"


def item(
    n: int,
    order_id: str,
    brand: str = ALPHA,
    product: str = P1,
    quantity: int = 2,
    unit_price: str = "10.00",
    line_total: str = "20.00",
) -> dict[str, Any]:
    return {
        "item_id": uid(0x1000 + n),
        "order_id": order_id,
        "brand_id": brand,
        "product_id": product,
        "quantity": quantity,
        "unit_price": unit_price,
        "line_total": line_total,
    }


def order(
    order_id: str = O1,
    *,
    brand: str = ALPHA,
    customer: str = C1,
    n_item: int = 0,
    **over: Any,
) -> dict[str, Any]:
    """A contract-valid order (two items: 20.00 + 5.50 = 25.50, discount 2.50, net 23.00)."""
    payload: dict[str, Any] = {
        "schema_version": 1,
        "order_id": order_id,
        "brand_id": brand,
        "customer_id": customer,
        "order_timestamp": BASE_TS,
        "order_status": "completed",
        "payment_method": "credit_card",
        "currency": "EUR",
        "gross_amount": "25.50",
        "discount_amount": "2.50",
        "net_amount": "23.00",
        "items": [
            item(n_item + 1, order_id, brand, P1, 2, "10.00", "20.00"),
            item(n_item + 2, order_id, brand, P2, 1, "5.50", "5.50"),
        ],
    }
    for name, value in over.items():
        if value is ABSENT:
            payload.pop(name, None)
        else:
            payload[name] = value
    return payload


def product(product_id: str = P1, *, brand: str = ALPHA, **over: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "product_id": product_id,
        "brand_id": brand,
        "product_name": "Classic Jacket",
        "category": "Apparel",
        "retail_price": "49.90",
        "cost_price": "20.00",
        "is_active": True,
    }
    for name, value in over.items():
        if value is ABSENT:
            payload.pop(name, None)
        else:
            payload[name] = value
    return payload


def dump(payload: dict[str, Any] | str) -> str:
    """One NDJSON line; a ``str`` is passed through (malformed-line cases)."""
    return payload if isinstance(payload, str) else json.dumps(payload, separators=(",", ":"))


def order_path(brand: str = ALPHA, batch_no: int = 1, day: str = DAY) -> str:
    return f"landing/{brand}/orders/{day}/batch-{batch_no:05d}.ndjson"


def product_path(brand: str = ALPHA, day: str = DAY) -> str:
    return f"landing/{brand}/products/{day}/products.ndjson"


def bronze_files(
    spark: SparkSession,
    target: BatchTarget,
    files: dict[str, list[dict[str, Any] | str]],
    batch_id: str = "batch-1",
) -> DataFrame:
    """Bronze rows (one per line) of whole-file inputs ``{path: [line, ...]}``."""
    from lakehouse.ingestion.batch import FILE_PATH_COLUMN, to_bronze_rows

    rows = [(path, "".join(dump(line) + "\n" for line in lines)) for path, lines in files.items()]
    frame = spark.createDataFrame(rows, [FILE_PATH_COLUMN, "value"])
    return to_bronze_rows(frame, target, batch_id)


def ref(path: str, line: int) -> str:
    return f"{path}#L{line}"
