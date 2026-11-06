"""Unit tests for scripts/autonomous/limits.py (rate-limit detection and wait planning)."""

from __future__ import annotations

import dataclasses
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "autonomous"))

import limits
from limits import LimitHit, detect_limit, plan_wait


RESETS_AT = 1762430400
RESETS_DT = datetime.fromtimestamp(RESETS_AT, tz=UTC)


def _event(status: str, **info: object) -> str:
    return json.dumps({"type": "rate_limit_event", "rate_limit_info": {"status": status, **info}})


def _result(text: str, *, is_error: bool = True) -> str:
    return json.dumps({"type": "result", "is_error": is_error, "result": text})


def _hit(resets_at: datetime | None) -> LimitHit:
    return LimitHit(limit_type=None, resets_at=resets_at, source="event", message="m")


# --- structured events -------------------------------------------------------------------------


def test_verified_event_template_is_detected() -> None:
    line = (
        '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected",'
        '"resetsAt":1762430400,"rateLimitType":"five_hour"}}'
    )
    hit = detect_limit([line])
    assert hit is not None
    assert hit.source == "event"
    assert hit.limit_type == "five_hour"
    assert hit.resets_at == RESETS_DT
    assert hit.resets_at.tzinfo is not None
    assert hit.resets_at.utcoffset() == timedelta(0)


@pytest.mark.parametrize("status", ["allowed", "allowed_warning"])
def test_non_rejected_event_is_not_a_hit(status: str) -> None:
    assert detect_limit([_event(status, resetsAt=RESETS_AT, rateLimitType="five_hour")]) is None


def test_event_without_resets_at_and_type() -> None:
    hit = detect_limit([_event("rejected")])
    assert hit is not None
    assert hit.source == "event"
    assert hit.resets_at is None
    assert hit.limit_type is None


def test_last_rejected_event_wins() -> None:
    lines = [
        _event("rejected", resetsAt=RESETS_AT, rateLimitType="five_hour"),
        _event("rejected", resetsAt=RESETS_AT + 3600, rateLimitType="seven_day"),
        _event("allowed", resetsAt=RESETS_AT + 99, rateLimitType="seven_day_opus"),
    ]
    hit = detect_limit(lines)
    assert hit is not None
    assert hit.limit_type == "seven_day"
    assert hit.resets_at == RESETS_DT + timedelta(hours=1)


def test_rejected_event_beats_text_hit_regardless_of_order() -> None:
    text = _result("You've hit your weekly limit", is_error=True)
    event = _event("rejected", resetsAt=RESETS_AT, rateLimitType="five_hour")
    for lines in ([text, event], [event, text]):
        hit = detect_limit(lines)
        assert hit is not None
        assert hit.source == "event"
        assert hit.limit_type == "five_hour"


def test_hit_message_is_capped_at_500_chars() -> None:
    hit = detect_limit([_result("You've hit your 5-hour limit " + "x" * 2000)])
    assert hit is not None
    assert len(hit.message) <= 500


def test_limit_hit_is_frozen() -> None:
    hit = _hit(None)
    with pytest.raises(dataclasses.FrozenInstanceError):
        hit.message = "other"  # type: ignore[misc]


# --- text fallback -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("5-hour limit", "five_hour"),
        ("session limit", "five_hour"),
        ("weekly limit", "seven_day"),
        ("Opus limit", "seven_day_opus"),
        ("Sonnet limit", "seven_day_sonnet"),
        ("Fable limit", "seven_day_overage_included"),
        ("usage credit limit", "overage"),
        ("usage limit", None),
        ("limit", None),
    ],
)
def test_text_label_maps_to_limit_type(label: str, expected: str | None) -> None:
    hit = detect_limit([_result(f"You've hit your {label}")])
    assert hit is not None
    assert hit.source == "text"
    assert hit.limit_type == expected
    assert hit.resets_at is None


@pytest.mark.parametrize("apostrophe", ["'", chr(0x2019)])
def test_both_apostrophes_match(apostrophe: str) -> None:
    hit = detect_limit([f"You{apostrophe}ve hit your 5-hour limit"])
    assert hit is not None
    assert hit.limit_type == "five_hour"


def test_verified_human_text_templates_match() -> None:
    texts = [
        "You've hit your 5-hour limit · resets 3:20pm · progress saved",
        "You've hit your weekly limit · resets Nov 10, 9am",
        "You've hit your Opus limit · progress saved",
        "You've hit your session limit",
    ]
    expected = ["five_hour", "seven_day", "seven_day_opus", "five_hour"]
    for text, want in zip(texts, expected, strict=True):
        hit = detect_limit([_result(text)])
        assert hit is not None, text
        assert hit.limit_type == want
        assert hit.resets_at is None


def test_non_json_plain_line_matches() -> None:
    hit = detect_limit(["  You've hit your weekly limit"])
    assert hit is not None
    assert hit.limit_type == "seven_day"
    assert hit.source == "text"


def test_malformed_json_is_treated_as_plain_text() -> None:
    # Starts with "{" but is not valid JSON, and does not start with the phrase -> no match.
    assert (
        detect_limit(['{"type": "result", "is_error": true, You\'ve hit your 5-hour limit']) is None
    )


def test_malformed_json_starting_with_brace_matching_text_is_not_required() -> None:
    # The phrase must start the string; a truncated JSON blob never does.
    assert detect_limit(['{"type":"result","result":"You\'ve hit your weekly limit']) is None


def test_monthly_spend_limit_is_not_mapped_to_five_hour() -> None:
    hit = detect_limit([_result("You've hit your monthly spend limit")])
    assert hit is None or hit.limit_type is None


def test_assistant_message_content_does_not_match() -> None:
    line = json.dumps(
        {
            "type": "assistant",
            "message": {"content": [{"type": "text", "text": "You've hit your 5-hour limit"}]},
        }
    )
    assert detect_limit([line]) is None


def test_result_without_error_flag_does_not_match() -> None:
    assert detect_limit([_result("You've hit your 5-hour limit", is_error=False)]) is None


def test_phrase_not_at_start_does_not_match() -> None:
    assert detect_limit(["I think you've hit your limit"]) is None
    assert detect_limit([_result("Note: You've hit your weekly limit")]) is None


def test_blank_lines_and_empty_input_are_ignored() -> None:
    assert detect_limit([]) is None
    assert detect_limit(["", "   ", "\n"]) is None


def test_unrelated_output_yields_none() -> None:
    lines = [json.dumps({"type": "system", "subtype": "init"}), _result("done", is_error=False)]
    assert detect_limit(lines) is None


# --- plan_wait ---------------------------------------------------------------------------------

NOW = datetime(2025, 11, 6, 12, 0, 0, tzinfo=UTC)


def test_plan_wait_known_reset_adds_margin() -> None:
    hit = _hit(NOW + timedelta(seconds=600))
    assert plan_wait(hit, NOW) == 600 + 120


def test_plan_wait_custom_margin() -> None:
    hit = _hit(NOW + timedelta(seconds=600))
    assert plan_wait(hit, NOW, margin_s=10) == 610


def test_plan_wait_past_reset_returns_margin_only() -> None:
    hit = _hit(NOW - timedelta(hours=2))
    assert plan_wait(hit, NOW) == 120
    assert plan_wait(hit, NOW, margin_s=30) == 30


def test_plan_wait_unknown_reset_uses_fallback() -> None:
    assert plan_wait(_hit(None), NOW) == 900
    assert plan_wait(_hit(None), NOW, fallback_s=60) == 60


def test_plan_wait_beyond_horizon_returns_none() -> None:
    hit = _hit(NOW + timedelta(seconds=86400 + 1))
    assert plan_wait(hit, NOW) is None


def test_plan_wait_at_horizon_boundary_with_margin_counts_against_horizon() -> None:
    # (reset - now) + margin is compared with the horizon: 86400 + 120 > 86400 -> None.
    hit = _hit(NOW + timedelta(seconds=86400))
    assert plan_wait(hit, NOW) is None
    # Exactly at the horizon (margin included) is still allowed.
    hit2 = _hit(NOW + timedelta(seconds=86400 - 120))
    assert plan_wait(hit2, NOW) == 86400


def test_plan_wait_custom_horizon() -> None:
    hit = _hit(NOW + timedelta(seconds=1000))
    assert plan_wait(hit, NOW, horizon_s=500) is None
    assert plan_wait(hit, NOW, horizon_s=5000) == 1120


def test_plan_wait_returns_int() -> None:
    result = plan_wait(_hit(NOW + timedelta(seconds=5, milliseconds=300)), NOW)
    assert isinstance(result, int)


def test_module_exposes_public_api() -> None:
    for name in ("LimitHit", "detect_limit", "plan_wait"):
        assert hasattr(limits, name)
