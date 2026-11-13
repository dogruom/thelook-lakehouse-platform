"""Alert payload and webhook sink for task failures and threshold breaches (W4.D23.3, D-099).

Contract of the payload (JSON object, ``schema_version`` 1):

``event`` (``"lakehouse.alert"``), ``kind`` (``"failure"`` | ``"threshold"``), ``dag_id``,
``run_id``, ``task_id``, ``brand_id`` (``null`` when the task is not about one brand), ``reason``
(sanitised, at most 300 characters), ``try_number``, ``logical_date`` (ISO 8601 UTC or ``null``)
and ``text`` (one line, also what a Slack incoming webhook displays).

No secrets and no PII: ``reason`` has e-mail addresses and URL credentials redacted and is cut;
only ids, counts and exception type names belong in it. The default sink is the task log (one
``ALERT_PAYLOAD {...}`` line); a webhook is called only when ``ALERT_WEBHOOK_URL`` is set. A sink
problem never raises: an alert must not fail the task that is already failing.

This module runs inside Airflow, where the standard ``logging`` module feeds the task log, so it
uses that instead of the Spark-side structlog setup.
"""

from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any, Final

from lakehouse.orchestration.plan import is_valid_brand_id


EVENT: Final = "lakehouse.alert"
SCHEMA_VERSION: Final = 1
KIND_FAILURE: Final = "failure"
KIND_THRESHOLD: Final = "threshold"
KINDS: Final = (KIND_FAILURE, KIND_THRESHOLD)
#: Prefix of the exception message of a DLQ-rate breach (classifies the alert as a threshold).
THRESHOLD_MARKER: Final = "dlq_rate_breach"
LOG_PREFIX: Final = "ALERT_PAYLOAD"
MAX_REASON_CHARS: Final = 300
WEBHOOK_TIMEOUT_S: Final = 5.0

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_URL_CREDENTIALS_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^/\s@]+@")
_BRAND_TOKEN_RE = re.compile(r"\bbrand_id=(brand_[a-z0-9_]{1,40})(?![a-z0-9_])")
_log = logging.getLogger("lakehouse.alert")


def sanitize_reason(text: str) -> str:
    """Single-line, redacted, truncated version of ``text``."""
    cleaned = _URL_CREDENTIALS_RE.sub(r"\1[redacted]@", text)
    cleaned = _EMAIL_RE.sub("[email]", cleaned)
    cleaned = " ".join(cleaned.split())
    if len(cleaned) > MAX_REASON_CHARS:
        cleaned = cleaned[: MAX_REASON_CHARS - 3] + "..."
    return cleaned


#: Tasks whose failure is only a roll-up of other failures that already alerted: no second alert.
ROLLUP_TASKS: Final = ("run_verdict",)


def is_alertable_task(task_id: str) -> bool:
    """False for roll-up tasks (one alert per underlying failure, not two)."""
    return task_id not in ROLLUP_TASKS


def failure_text(exception: object = None, reason: object = None, state: object = None) -> str:
    """Best available reason text of a failed task; ``"unknown"`` when the context has nothing.

    Airflow callbacks do not always carry ``exception`` (for example after a timeout or an
    externally set state); fall back to ``reason`` and then to the task state.
    """
    for candidate in (exception, reason):
        text = str(candidate).strip() if candidate is not None else ""
        if text:
            return text
    state_text = str(state).strip() if state is not None else ""
    return f"task failed (state={state_text})" if state_text else "unknown"


def classify_kind(reason: str) -> str:
    """``threshold`` when the reason starts with the breach marker, else ``failure``."""
    return KIND_THRESHOLD if reason.lstrip().startswith(THRESHOLD_MARKER) else KIND_FAILURE


def extract_brand_id(text: str) -> str | None:
    """The brand named by a ``brand_id=<id>`` token in ``text`` (first valid one), else ``None``."""
    match = _BRAND_TOKEN_RE.search(text)
    return match.group(1) if match else None


def _iso_utc(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def build_alert_payload(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    kind: str,
    reason: str,
    try_number: int,
    logical_date: datetime | str | None,
    brand_id: str | None = None,
) -> dict[str, Any]:
    """JSON-serialisable alert (see the module docstring). ``ValueError`` for an unknown ``kind``.

    A ``brand_id`` outside the tenant-id contract is dropped (``null``) rather than echoed.
    """
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}, got {kind!r}")
    brand = brand_id if brand_id is not None and is_valid_brand_id(brand_id) else None
    safe_reason = sanitize_reason(reason)
    scope = f" ({brand})" if brand else ""
    return {
        "event": EVENT,
        "schema_version": SCHEMA_VERSION,
        "kind": kind,
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "brand_id": brand,
        "reason": safe_reason,
        "try_number": int(try_number),
        "logical_date": _iso_utc(logical_date),
        "text": f"[lakehouse] {kind} in {dag_id}.{task_id}{scope}: {safe_reason}",
    }


def format_log_line(payload: dict[str, Any]) -> str:
    """The one log line of a payload: ``ALERT_PAYLOAD <compact sorted JSON>``."""
    return f"{LOG_PREFIX} {json.dumps(payload, sort_keys=True, separators=(',', ':'))}"


def parse_log_line(line: str) -> dict[str, Any] | None:
    """Inverse of :func:`format_log_line` (tolerates a log prefix before the marker)."""
    start = line.find(LOG_PREFIX + " ")
    if start < 0:
        return None
    try:
        loaded = json.loads(line[start + len(LOG_PREFIX) + 1 :])
    except json.JSONDecodeError:
        return None
    return loaded if isinstance(loaded, dict) else None


def post_webhook(payload: dict[str, Any], url: str, timeout_s: float = WEBHOOK_TIMEOUT_S) -> bool:
    """POST ``payload`` as JSON to an ``http(s)`` URL; ``True`` on a 2xx answer, never raises."""
    if not url.lower().startswith(("http://", "https://")):
        _log.warning("alert webhook ignored: URL scheme is not http(s)")
        return False
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    request = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:  # the scheme was checked above: no file:// or custom handlers
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            return bool(200 <= response.status < 300)
    except (urllib.error.URLError, OSError, ValueError) as exc:
        # Type name only: the exception text can carry the URL (and with it a Slack secret).
        _log.warning("alert webhook failed: %s", type(exc).__name__)
        return False


def emit_alert(payload: dict[str, Any], webhook_url: str | None) -> bool:
    """Log the payload line and, when ``webhook_url`` is set, POST it. Returns webhook success."""
    _log.error(format_log_line(payload))
    if not webhook_url:
        return False
    return post_webhook(payload, webhook_url)
