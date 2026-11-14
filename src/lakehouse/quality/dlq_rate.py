"""Pure DLQ-rate logic of the ``dlq_rate_gate`` job (W4.D23.2, D-099).

``dlq_rate = dlq rows / parseable Bronze rows`` per ``(brand_id, source table)``. Both terms use the
same definitions as ``reconcile``: parseable Bronze rows are the distinct ``_source_ref`` without a
``_parse_error`` (parse failures are not contract failures and stay out of the denominator); DLQ rows
are the distinct ``_bronze_ref`` of the DLQ table that ``replay`` has not recovered
(``_replay_status`` is not ``REPLAYED``).

A rate **above** the threshold breaches; a rate equal to it passes. No parseable rows and no DLQ
rows means "no data": the rate is ``None`` and never breaches. DLQ rows without any parseable Bronze
row cannot happen in a consistent lakehouse; they count as rate ``1.0`` so the anomaly is not hidden.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

#: Log event the job emits on a breach (with ``brands``); the orchestration layer reads it from the
#: job's JSON log by name (``lakehouse.orchestration.joblog``) and never imports this module.
BREACH_EVENT = "dlq_rate_gate.breach"


class DlqRateBreachError(RuntimeError):
    """At least one brand is above the DLQ-rate threshold (message names the brands)."""


@dataclass(frozen=True)
class RateResult:
    """Outcome for one ``(brand, source table)``."""

    brand_id: str
    source_table: str
    bronze_rows: int
    dlq_rows: int
    dlq_rate: float | None
    breached: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "bronze_rows": self.bronze_rows,
            "dlq_rows": self.dlq_rows,
            "dlq_rate": None if self.dlq_rate is None else round(self.dlq_rate, 6),
            "breached": self.breached,
        }


def validate_max_rate(value: float) -> float:
    """``value`` when it is a number within ``[0, 1]``; ``ValueError`` otherwise."""
    if math.isnan(value) or not 0.0 <= value <= 1.0:
        raise ValueError(f"max DLQ rate must be within [0, 1], got {value}")
    return value


def dlq_rate(dlq_rows: int, bronze_rows: int) -> float | None:
    """``dlq_rows / bronze_rows``; ``None`` without any rows, ``1.0`` for DLQ rows without Bronze."""
    if bronze_rows <= 0:
        return None if dlq_rows <= 0 else 1.0
    return dlq_rows / bronze_rows


def evaluate_rates(
    brands: Sequence[str],
    sources: Sequence[str],
    counts: Mapping[tuple[str, str], tuple[int, int]],
    max_rate: float,
) -> list[RateResult]:
    """One result per ``(brand, source)``; ``counts[(brand, source)] = (bronze_rows, dlq_rows)``.

    A missing key means 0 rows. The order is ``brands`` then ``sources`` as given (deterministic).
    """
    validate_max_rate(max_rate)
    results: list[RateResult] = []
    for brand in brands:
        for source in sources:
            bronze, dlq = counts.get((brand, source), (0, 0))
            rate = dlq_rate(dlq, bronze)
            results.append(
                RateResult(brand, source, bronze, dlq, rate, rate is not None and rate > max_rate)
            )
    return results


def breached_brands(results: Sequence[RateResult]) -> list[str]:
    """Sorted, unique brands that have at least one breached source."""
    return sorted({r.brand_id for r in results if r.breached})


def report(results: Sequence[RateResult]) -> dict[str, dict[str, dict[str, Any]]]:
    """``{brand: {source: {bronze_rows, dlq_rows, dlq_rate, breached}}}`` (JSON-serialisable)."""
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for r in results:
        out.setdefault(r.brand_id, {})[r.source_table] = r.as_dict()
    return out


def breach_message(results: Sequence[RateResult], max_rate: float) -> str:
    """Message naming the breaching brands (and their sources); counts only, no row values."""
    parts = [
        f"{r.brand_id}/{r.source_table}={r.dlq_rate:.4f}"
        for r in results
        if r.breached and r.dlq_rate is not None
    ]
    brands = ", ".join(breached_brands(results))
    return f"DLQ rate above {max_rate:.4f} for brand(s): {brands} ({'; '.join(parts)})"
