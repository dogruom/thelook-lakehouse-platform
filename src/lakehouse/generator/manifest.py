"""Ground-truth manifest of a CDC run: the test oracle for the Silver layer (DATA_CONTRACTS §2.1.1).

``classify_outcomes`` is a pure state machine over the final emission order. It encodes the gate
order contract gate (DLQ) -> dedup -> sequence-ordered MERGE (decisions D-022...D-025).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from itertools import pairwise
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


class Outcome(StrEnum):
    ACCEPT = "ACCEPT"
    DLQ = "DLQ"
    DUPLICATE = "DUPLICATE"
    NOOP = "NOOP"


class CdcDefect(StrEnum):
    invalid_email = "invalid_email"
    null_email = "null_email"
    future_signup = "future_signup"
    out_of_order = "out_of_order"
    duplicate = "duplicate"
    cross_tenant_collision = "cross_tenant_collision"


DLQ_DEFECTS = frozenset({CdcDefect.invalid_email, CdcDefect.null_email, CdcDefect.future_signup})

# Reconciliation term of every outcome (DATA_CONTRACTS §3.2).
RECONCILIATION_TERM = {
    Outcome.ACCEPT: "silver_accepted",
    Outcome.DLQ: "dlq",
    Outcome.DUPLICATE: "duplicates_removed",
    Outcome.NOOP: "stale_cdc_noop",
}


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """One emission (a duplicate replay is its own entry and shares the ``event_id``).

    ``expected_outcome`` is ignored by ``classify_outcomes``; it only holds the result.
    """

    emission_index: int
    event_id: str
    brand_id: str
    customer_id: str
    cdc_sequence: int
    cdc_action: str
    defect: CdcDefect | None = None
    expected_outcome: Outcome = Outcome.ACCEPT


@dataclass(frozen=True, slots=True)
class Manifest:
    seed: int
    brands: int
    base_events: int
    emitted_events: int
    generator_version: str
    config_fingerprint: str
    entries: list[ManifestEntry] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    expected_final: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """JSON-ready dict (enums become their string values)."""
        return asdict(self)


def _batch_bounds(n: int, batch_ends: Sequence[int] | None) -> list[int]:
    """Exclusive batch end offsets covering ``range(n)``; ``None`` means batch size 1."""
    if batch_ends is None:
        return list(range(1, n + 1))
    bounds = list(batch_ends)
    if any(b <= 0 or b > n for b in bounds) or any(a >= b for a, b in pairwise(bounds)):
        raise ValueError("batch_ends must be strictly increasing offsets within (0, len(entries)]")
    if (bounds[-1] if bounds else 0) < n:
        bounds.append(n)
    return bounds


def classify_outcomes(
    entries: Sequence[ManifestEntry],
    valid: Sequence[bool],
    batch_ends: Sequence[int] | None = None,
) -> list[Outcome]:
    """Expected Silver outcome of every emission, in emission order.

    Per emission: (1) invalid payload -> DLQ; (2) key ``(brand_id, customer_id, cdc_sequence)``
    already seen in an earlier valid emission, in this or an earlier batch -> DUPLICATE;
    (3) otherwise the max-sequence candidate of a key within a batch is ACCEPT when its sequence
    exceeds the highest sequence applied by earlier batches, every other candidate is NOOP.
    ``batch_ends`` are exclusive batch end offsets; ``None`` evaluates one event at a time.
    """
    n = len(entries)
    if len(valid) != n:
        raise ValueError("valid must have one flag per entry")
    outcomes = [Outcome.NOOP] * n
    seen: set[tuple[str, str, int]] = set()
    applied: dict[tuple[str, str], int] = {}
    start = 0
    for end in _batch_bounds(n, batch_ends):
        best: dict[tuple[str, str], int] = {}
        for i in range(start, end):
            e = entries[i]
            if not valid[i]:
                outcomes[i] = Outcome.DLQ
                continue
            full_key = (e.brand_id, e.customer_id, e.cdc_sequence)
            if full_key in seen:
                outcomes[i] = Outcome.DUPLICATE
                continue
            seen.add(full_key)
            key = (e.brand_id, e.customer_id)
            if key not in best or e.cdc_sequence > entries[best[key]].cdc_sequence:
                best[key] = i
        for key, i in best.items():
            if entries[i].cdc_sequence > applied.get(key, 0):
                outcomes[i] = Outcome.ACCEPT
                applied[key] = entries[i].cdc_sequence
        start = end
    return outcomes


def build_counts(entries: Sequence[ManifestEntry]) -> dict[str, int]:
    """Per-defect counts plus the reconciliation terms (``bronze == emitted``)."""
    counts = {d.value: 0 for d in CdcDefect}
    for entry in entries:
        if entry.defect is not None:
            counts[entry.defect.value] += 1
    for term in RECONCILIATION_TERM.values():
        counts[term] = 0
    for entry in entries:
        counts[RECONCILIATION_TERM[entry.expected_outcome]] += 1
    counts["late_beyond_window"] = 0
    counts["emitted"] = counts["bronze"] = len(entries)
    return counts


def expected_final_rows(
    entries: Sequence[ManifestEntry],
    values: Sequence[Mapping[str, Any]],
    valid: Sequence[bool],
) -> list[dict[str, Any]]:
    """Final Silver state: per ``(brand_id, customer_id)`` the valid emission with max sequence."""
    best: dict[tuple[str, str], int] = {}
    for i, entry in enumerate(entries):
        if not valid[i]:
            continue
        key = (entry.brand_id, entry.customer_id)
        if key not in best or entry.cdc_sequence > entries[best[key]].cdc_sequence:
            best[key] = i
    rows = []
    for key in sorted(best):
        i = best[key]
        entry, value = entries[i], values[i]
        rows.append(
            {
                "brand_id": entry.brand_id,
                "customer_id": entry.customer_id,
                "cdc_sequence": entry.cdc_sequence,
                "event_id": entry.event_id,
                "cdc_action": entry.cdc_action,
                "is_deleted": entry.cdc_action == "D",
                "city": value["city"],
                "country": value["country"],
                "loyalty_tier": value["loyalty_tier"],
            }
        )
    return rows
