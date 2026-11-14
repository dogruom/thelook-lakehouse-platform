"""Read the structured log of a Spark job run (the orchestration <-> compute interface, ADR-0010).

A job communicates through its exit code and its JSON log lines (``job.started``,
``job.succeeded`` with ``metrics``, ``job.failed`` with ``error_type``). Spark's own log4j lines on
the same stream are ignored. Only counts and error type names are extracted - never row values.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Final


if TYPE_CHECKING:
    from collections.abc import Iterable

SUCCEEDED: Final = "job.succeeded"
FAILED: Final = "job.failed"
#: Event the ``dlq_rate_gate`` job logs when a brand is above the threshold.
THRESHOLD_BREACH_EVENT: Final = "dlq_rate_gate.breach"


def parse_records(lines: Iterable[str]) -> list[dict[str, Any]]:
    """JSON objects found in ``lines`` (from the first ``{`` of each line), in order."""
    records: list[dict[str, Any]] = []
    for line in lines:
        start = line.find("{")
        if start < 0:
            continue
        try:
            record = json.loads(line[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


def events(records: Iterable[dict[str, Any]], name: str) -> list[dict[str, Any]]:
    """Records whose ``event`` equals ``name``."""
    return [r for r in records if r.get("event") == name]


def job_metrics(records: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    """``metrics`` of the last ``job.succeeded`` record, or ``None`` when there is none."""
    done = events(records, SUCCEEDED)
    if not done:
        return None
    metrics = done[-1].get("metrics")
    return dict(metrics) if isinstance(metrics, dict) else {}


def breached_brands_from_log(records: Iterable[dict[str, Any]]) -> list[str]:
    """Brands named by ``dlq_rate_gate.breach`` records (sorted, unique, strings only)."""
    brands: set[str] = set()
    for record in events(records, THRESHOLD_BREACH_EVENT):
        listed = record.get("brands")
        if isinstance(listed, list):
            brands.update(b for b in listed if isinstance(b, str))
    return sorted(brands)


def failure_reason(job: str, exit_code: int, records: Iterable[dict[str, Any]]) -> str:
    """One-line failure reason: job, exit code and the logged ``error_type`` (no traceback)."""
    failed = events(records, FAILED)
    error_type = failed[-1].get("error_type") if failed else None
    suffix = f", error_type={error_type}" if isinstance(error_type, str) else ""
    return f"spark job {job} exited with code {exit_code}{suffix}"
