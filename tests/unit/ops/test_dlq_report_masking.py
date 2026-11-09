"""dlq_report log hygiene: unvalidated payload values never reach the log as-is (D-057)."""

from __future__ import annotations

import pytest

from lakehouse.ops.jobs import dlq_report
from lakehouse.quality.jobs import dlq_gate


VALID = "0b8f8a52-3c1e-4d2a-9f4b-7c6d5e4f3a21"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "__null__"),
        (VALID, VALID),
        (VALID.upper(), "__invalid__"),
        (VALID + "\n", "__invalid__"),
        ("0b8f8a52-3c1e-1d2a-9f4b-7c6d5e4f3a21", "__invalid__"),  # version 1, not 4
        ("jane.doe@example.com", "__invalid__"),
        ("", "__invalid__"),
    ],
)
def test_event_id_is_logged_only_when_it_is_a_uuid4(value: str | None, expected: str) -> None:
    assert dlq_report._safe_event_id(value) == expected


def test_brand_placeholders_match_between_gate_and_report() -> None:
    # Duplicated on purpose (components are independent, ADR-0009); they must not drift.
    assert dlq_report.NULL_BRAND_KEY == dlq_gate.NULL_BRAND_KEY
    assert dlq_report.INVALID_BRAND_KEY == dlq_gate.INVALID_BRAND_KEY
    assert dlq_report._BRAND_RLIKE == dlq_gate._BRAND_RLIKE
