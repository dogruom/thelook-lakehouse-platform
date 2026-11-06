"""Independent reference implementation of the CDC outcome rule and the sequence-ordered MERGE.

Written from DATA_CONTRACTS §4.1 and the W1.D4 task card, NOT from ``lakehouse.generator``.
It deliberately shares no code with ``classify_outcomes``: it is the oracle the tests compare
the generator's manifest against.

Precedence per emission (final emission order):
  DLQ (invalid payload) -> DUPLICATE (key seen in an earlier VALID emission)
  -> NOOP (stale / lost within batch) -> ACCEPT
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from lakehouse.contracts import CustomerCdcEvent


Key = tuple[str, str, int]  # (brand_id, customer_id, cdc_sequence)

ACCEPT, DLQ, DUPLICATE, NOOP = "ACCEPT", "DLQ", "DUPLICATE", "NOOP"


def is_valid(payload: dict[str, Any]) -> bool:
    try:
        CustomerCdcEvent.model_validate(payload)
    except ValidationError:
        return False
    return True


def canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def reference_outcomes(
    events: list[tuple[str, str, int]],
    valid: list[bool],
    batch_ends: list[int] | None = None,
) -> list[str]:
    """Outcome per emission. ``batch_ends`` are exclusive end offsets; None = batch size 1."""
    n = len(events)
    ends = list(range(1, n + 1)) if batch_ends is None else list(batch_ends)
    assert ends and ends[-1] == n, "batches must cover the whole stream"
    out: list[str] = [""] * n
    seen: set[Key] = set()
    applied: dict[tuple[str, str], int] = {}
    start = 0
    for end in ends:
        candidates: dict[tuple[str, str], list[int]] = {}
        for i in range(start, end):
            brand, cust, seq = events[i]
            if not valid[i]:
                out[i] = DLQ
                continue
            if (brand, cust, seq) in seen:
                out[i] = DUPLICATE
                continue
            seen.add((brand, cust, seq))
            candidates.setdefault((brand, cust), []).append(i)
        for entity, idxs in candidates.items():
            winner = max(idxs, key=lambda i: events[i][2])
            for i in idxs:
                if i == winner and events[i][2] > applied.get(entity, 0):
                    out[i] = ACCEPT
                else:
                    out[i] = NOOP
            if events[winner][2] > applied.get(entity, 0):
                applied[entity] = events[winner][2]
        start = end
    return out


def reference_final(
    payloads: list[dict[str, Any]], *, ignore_sequence: bool = False
) -> list[dict[str, Any]]:
    """Apply valid payloads in the given order with the sequence-guarded upsert.

    ``ignore_sequence=True`` is the deliberately WRONG last-write-wins merge, used by negative tests.
    """
    target: dict[tuple[str, str], dict[str, Any]] = {}
    for p in payloads:
        if not is_valid(p):
            continue
        entity = (p["brand_id"], p["customer_id"])
        current = target.get(entity)
        if (
            not ignore_sequence
            and current is not None
            and p["_cdc_sequence"] <= current["cdc_sequence"]
        ):
            continue
        target[entity] = {
            "brand_id": p["brand_id"],
            "customer_id": p["customer_id"],
            "cdc_sequence": p["_cdc_sequence"],
            "event_id": p["event_id"],
            "cdc_action": p["_cdc_action"],
            "is_deleted": p["_cdc_action"] == "D",
            "city": p["city"],
            "country": p["country"],
            "loyalty_tier": p["loyalty_tier"],
        }
    return [target[k] for k in sorted(target)]
