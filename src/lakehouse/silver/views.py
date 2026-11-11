"""Per-tenant Silver views (W3.D18, D-085): one filtered view per (table, brand).

``tenant_view_sql`` only builds DDL from identifiers validated against strict patterns, so no
caller-supplied text reaches the SQL (no injection). The view is a convenience for brand-scoped
readers and for the isolation check; it is not access control (no row-level security, ADR-0005).
"""

from __future__ import annotations

import re

from lakehouse.contracts.models import BRAND_ID_PATTERN


_BRAND = re.compile(BRAND_ID_PATTERN.removesuffix("$") + r"\Z")
_IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")


def _valid(label: str, value: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or not pattern.match(value):
        raise ValueError(f"invalid {label} {value!r}")
    return value


def tenant_view_sql(table: str, brand: str, *, temp: bool, source: str | None = None) -> str:
    """``CREATE OR REPLACE [TEMP] VIEW`` over ``silver.<table>`` filtered to one ``brand_id``.

    The named view is ``silver.v_<table>__<brand>``; the temp view has no database prefix.
    ``source`` overrides the source relation (a temp view name, for path-addressed runs); it is
    validated like a table name.
    """
    table = _valid("table name", table, _IDENTIFIER)
    brand = _valid("brand_id", brand, _BRAND)
    src = f"silver.{table}" if source is None else _valid("source name", source, _IDENTIFIER)
    if temp:
        head = f"CREATE OR REPLACE TEMP VIEW v_{table}__{brand}"
    else:
        head = f"CREATE OR REPLACE VIEW silver.v_{table}__{brand}"
    return f"{head} AS SELECT * FROM {src} WHERE brand_id = '{brand}'"
