"""Log-safe tenant key shared by the gate and the reports (D-054, D-057).

A ``brand_id`` read from a payload or header is unvalidated input: a value outside the tenant-id
contract must never be logged verbatim (it may hold PII). :func:`brand_key` maps NULL to
``__null__`` and anything that does not match the contract pattern to ``__invalid__``.

PySpark is imported lazily so hosts without it can import this module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lakehouse.contracts.models import BRAND_ID_PATTERN


if TYPE_CHECKING:
    from pyspark.sql import Column

NULL_BRAND_KEY = "__null__"
INVALID_BRAND_KEY = "__invalid__"
# Java's ``$`` also matches before a trailing newline; ``\z`` is the true end of input.
BRAND_RLIKE = BRAND_ID_PATTERN.removesuffix("$") + r"\z"


def brand_key(column: str = "brand_id") -> Column:
    """Log-safe tenant key of ``column``: the value, ``__null__`` or ``__invalid__``."""
    from pyspark.sql import functions as F

    brand = F.col(column)
    return (
        F.when(brand.isNull(), F.lit(NULL_BRAND_KEY))
        .when(brand.rlike(BRAND_RLIKE), brand)
        .otherwise(F.lit(INVALID_BRAND_KEY))
    )
