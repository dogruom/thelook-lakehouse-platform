"""Usage-limit detection and wait planning for ``claude -p --output-format stream-json``.

Only two evidence sources are trusted:

* a ``rate_limit_event`` line whose ``rate_limit_info.status`` is ``rejected`` (structured, wins);
* the human message ``You've hit your <label> ...`` found in an error ``result`` line or in
  non-JSON output (stderr). Assistant message content is never inspected.

The ``resets ...`` text in the human message is local-formatted and is never parsed.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal


if TYPE_CHECKING:
    from collections.abc import Iterable


# Longest labels first so that the bare "limit" alternative cannot shadow a specific label.
LIMIT_TEXT: re.Pattern[str] = re.compile(
    r"^\s*You(?:'|"
    "\N{RIGHT SINGLE QUOTATION MARK}"
    r")ve hit your "
    r"(?P<label>usage credit limit|usage limit|session limit|5-hour limit|weekly limit"
    r"|Opus limit|Sonnet limit|Fable limit|limit)\b"
)

_LABEL_TO_TYPE: dict[str, str | None] = {
    "5-hour limit": "five_hour",
    "session limit": "five_hour",
    "weekly limit": "seven_day",
    "Opus limit": "seven_day_opus",
    "Sonnet limit": "seven_day_sonnet",
    "Fable limit": "seven_day_overage_included",
    "usage credit limit": "overage",
    "usage limit": None,
    "limit": None,
}

_MESSAGE_MAX = 500


@dataclass(frozen=True)
class LimitHit:
    """A detected usage-limit rejection."""

    limit_type: str | None
    resets_at: datetime | None  # timezone-aware UTC
    source: Literal["event", "text"]
    message: str  # raw evidence, truncated to 500 chars


def _to_utc(value: object) -> datetime | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    try:
        return datetime.fromtimestamp(value, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _event_hit(obj: dict[str, Any], raw: str) -> LimitHit | None:
    info = obj.get("rate_limit_info")
    if not isinstance(info, dict) or info.get("status") != "rejected":
        return None
    limit_type = info.get("rateLimitType")
    return LimitHit(
        limit_type=limit_type if isinstance(limit_type, str) else None,
        resets_at=_to_utc(info.get("resetsAt")),
        source="event",
        message=raw[:_MESSAGE_MAX],
    )


def _text_hit(text: str, raw: str) -> LimitHit | None:
    match = LIMIT_TEXT.match(text)
    if match is None:
        return None
    return LimitHit(
        limit_type=_LABEL_TO_TYPE.get(match.group("label")),
        resets_at=None,
        source="text",
        message=raw[:_MESSAGE_MAX],
    )


def _parse_json_object(stripped: str) -> dict[str, Any] | None:
    if not stripped.startswith("{"):
        return None
    try:
        parsed = json.loads(stripped)
    except ValueError:
        return None  # malformed JSON: the caller treats the line as plain text
    return parsed if isinstance(parsed, dict) else None


def _scan_line(stripped: str) -> LimitHit | None:
    """Evidence in a single non-blank line, honouring the trusted-source rules."""
    obj = _parse_json_object(stripped)
    if obj is None:
        return _text_hit(stripped, stripped)
    kind = obj.get("type")
    if kind == "rate_limit_event":
        return _event_hit(obj, stripped)
    if kind == "result" and obj.get("is_error") is True:
        result = obj.get("result")
        if isinstance(result, str):
            for result_line in result.splitlines():
                hit = _text_hit(result_line, stripped)
                if hit is not None:
                    return hit
    return None


def detect_limit(lines: Iterable[str]) -> LimitHit | None:
    """Return the usage-limit hit found in ``lines`` or ``None``.

    The last rejected ``rate_limit_event`` wins over any text hit.
    """
    event: LimitHit | None = None
    text: LimitHit | None = None
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        hit = _scan_line(stripped)
        if hit is None:
            continue
        if hit.source == "event":
            event = hit
        else:
            text = hit
    return event if event is not None else text


def plan_wait(
    hit: LimitHit,
    now: datetime,
    *,
    fallback_s: int = 900,
    margin_s: int = 120,
    horizon_s: int = 86400,
) -> int | None:
    """Seconds to sleep before retrying, or ``None`` when the reset is beyond the horizon."""
    if hit.resets_at is None:
        return fallback_s
    reference = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    remaining = (hit.resets_at - reference).total_seconds()
    wait = (math.ceil(remaining) if remaining > 0 else 0) + margin_s
    if wait > horizon_s:
        return None
    return int(wait)
