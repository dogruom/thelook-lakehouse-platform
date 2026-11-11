"""dlq_report log hygiene: unvalidated payload values never reach the log as-is (D-057)."""

from __future__ import annotations

import pytest

from lakehouse.common import brand_key
from lakehouse.ops.jobs import bronze_report, dlq_report
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


def test_brand_key_helper_is_shared_not_duplicated() -> None:
    # One definition in lakehouse.common (Q-004); the jobs import it and cannot drift.
    assert dlq_report.brand_key is brand_key.brand_key
    assert dlq_gate.brand_key is brand_key.brand_key
    assert bronze_report.brand_key is brand_key.brand_key
    assert dlq_report.NULL_BRAND_KEY == brand_key.NULL_BRAND_KEY == "__null__"
    assert dlq_report.INVALID_BRAND_KEY == brand_key.INVALID_BRAND_KEY == "__invalid__"
    assert brand_key.BRAND_RLIKE.endswith(r"\z")
