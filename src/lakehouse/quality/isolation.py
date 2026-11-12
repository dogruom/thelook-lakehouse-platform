"""Tenant isolation checks (W3.D18, D-085): per-brand counts and a cross-tenant leak detector.

``assert_no_cross_tenant_rows`` inspects a *joined* frame that carries the left side's
``brand_id`` and the right side's brand as ``right_brand_id``. In a tenant-safe join (key =
``brand_id`` + business key) the two are always equal; a join on the business key alone pairs rows
of different brands whenever two tenants reuse an id, and those rows are the leak. Any row whose
two brands differ (NULL-safe) fails the check, so a leak towards a third brand is caught too;
``left_brand`` and ``right_brand`` name the pair under test in the error. Out of scope: row-level
security and IAM (partitioning and this check are not an access-control boundary).
"""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pyspark.sql import DataFrame

BRAND_COLUMN = "brand_id"
RIGHT_BRAND_COLUMN = "right_brand_id"
_NULL_BRAND = "<null>"


class CrossTenantLeakError(Exception):
    """A joined frame pairs rows of different brands."""


def brand_counts(df: DataFrame) -> dict[str, int]:
    """Rows per ``brand_id`` (a NULL brand is reported as ``<null>``)."""
    rows = df.groupBy(BRAND_COLUMN).count().collect()
    return {
        _NULL_BRAND if r[BRAND_COLUMN] is None else str(r[BRAND_COLUMN]): int(r["count"])
        for r in rows
    }


def assert_no_cross_tenant_rows(df: DataFrame, left_brand: str, right_brand: str) -> None:
    """Raise :class:`CrossTenantLeakError` when ``df`` holds a row with differing brands."""
    leaked = df.filter(f"NOT ({BRAND_COLUMN} <=> {RIGHT_BRAND_COLUMN})").count()
    if leaked:
        raise CrossTenantLeakError(
            f"{leaked} joined rows pair {BRAND_COLUMN} with a different {RIGHT_BRAND_COLUMN} "
            f"(checked pair: {left_brand!r} / {right_brand!r}); a join key lacks brand_id"
        )
