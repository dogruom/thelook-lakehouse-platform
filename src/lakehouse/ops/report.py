"""Pure logic of the ops metrics report (W5.D30): plain dicts in, rows and a markdown table out.

The ``metrics_report`` Spark job collects small aggregated rows from ``ops.pipeline_metrics`` and
Bronze and hands them to this module, so the rules (rates, NULL handling, sorting, formatting) are
unit-testable on the host without PySpark.

What each column means, and what it is not:

* ``bronze``, ``silver_accepted``, ``dlq``, ``duplicates_removed``, ``stale_cdc_noop``,
  ``late_beyond_window``, ``parse_errors``: the latest ``scope='reconcile'`` row per
  ``(source_table, brand_id)``. A term that the data does not determine is ``None`` (shown
  ``n/a``), never 0 (see ``lakehouse.quality.jobs.reconcile``).
* ``dlq_rate = dlq / bronze``; ``None`` when ``bronze`` is 0 or ``dlq`` is unknown.
* ``reconcile``: ``OK`` / ``MISMATCH`` from the ``balanced`` flag of that row, ``n/a`` when no
  reconcile row exists.
* ``freshness_min``: ``now - max(_ingest_ts)`` in minutes, i.e. how long ago Bronze last wrote a
  row for the key. It is staleness of the *table*, not end-to-end latency.
* ``ingest_lag_min``: ``max(_ingest_ts) - max(_kafka_ts)`` in minutes, only for Bronze tables that
  carry ``_kafka_ts`` (the Kafka-sourced ones). It is a coarse lag indicator between the newest
  Kafka message time and the newest Bronze write, **not** a consumer-group offset lag: Kafka
  offsets are not available to a Glue job, and batch-loaded Bronze tables have no ``_kafka_ts``
  (shown ``n/a``).

Timestamps are naive UTC datetimes (the Spark session runs in UTC).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

from lakehouse.ops.metrics import TERMS


if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

NA = "n/a"
KEY = ("source_table", "brand_id")
REPORT_COLUMNS = (
    "source_table",
    "brand_id",
    "bronze",
    "silver_accepted",
    "dlq",
    "dlq_rate",
    "duplicates_removed",
    "stale_cdc_noop",
    "late_beyond_window",
    "parse_errors",
    "reconcile",
    "freshness_min",
    "ingest_lag_min",
)
BATCH_COLUMNS = (
    "source_table",
    "batches",
    "unbalanced_batches",
    "bronze",
    *TERMS,
    "dlq_rate",
    "parse_errors",
)
_COUNT_TERMS = (*TERMS, "parse_errors")


def dlq_rate(dlq: int | None, bronze: int | None) -> float | None:
    """``dlq / bronze``; ``None`` when either is unknown or ``bronze`` is not positive."""
    if dlq is None or bronze is None or bronze <= 0:
        return None
    return dlq / bronze


def minutes_between(later: datetime | None, earlier: datetime | None) -> float | None:
    """``later - earlier`` in minutes (1 decimal), clipped at 0; ``None`` if either is missing."""
    if later is None or earlier is None:
        return None
    return round(max((later - earlier).total_seconds(), 0.0) / 60.0, 1)


def _opt_int(value: object) -> int | None:
    return None if value is None else int(str(value))


def _sort_key(table: str, brand: str | None) -> tuple[str, int, str]:
    return (table, 1 if brand is None else 0, brand or "")


def latest_reconcile(rows: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str | None], Any]:
    """Latest ``scope='reconcile'`` row per ``(source_table, brand_id)`` (by ``recorded_at``,
    then ``run_id``); other scopes are ignored."""
    best: dict[tuple[str, str | None], Mapping[str, Any]] = {}
    for row in rows:
        if row.get("scope") != "reconcile":
            continue
        key = (str(row["source_table"]), row.get("brand_id"))
        order = (row.get("recorded_at") or datetime.min, str(row.get("run_id") or ""))
        current = best.get(key)
        if current is None or order > (
            current.get("recorded_at") or datetime.min,
            str(current.get("run_id") or ""),
        ):
            best[key] = row
    return dict(best)


def _status(row: Mapping[str, Any] | None) -> str:
    if row is None or row.get("balanced") is None:
        return NA
    return "OK" if row["balanced"] else "MISMATCH"


def build_rows(
    reconcile_rows: Iterable[Mapping[str, Any]],
    freshness: Iterable[Mapping[str, Any]],
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    """One report row per ``(source_table, brand_id)`` seen in the metrics or in Bronze.

    ``freshness`` items: ``source_table``, ``brand_id``, ``max_ingest_ts``, optional
    ``max_kafka_ts`` (``None`` when the table has no ``_kafka_ts``). Sorted by table, then brand
    (a NULL brand last).
    """
    metrics = latest_reconcile(reconcile_rows)
    fresh = {(str(f["source_table"]), f.get("brand_id")): f for f in freshness}
    out: list[dict[str, Any]] = []
    for key in sorted(set(metrics) | set(fresh), key=lambda k: _sort_key(*k)):
        m, f = metrics.get(key), fresh.get(key)
        bronze = _opt_int(m.get("bronze")) if m else None
        dlq = _opt_int(m.get("dlq")) if m else None
        ingest = f.get("max_ingest_ts") if f else None
        row: dict[str, Any] = {"source_table": key[0], "brand_id": key[1], "bronze": bronze}
        for term in TERMS:
            row[term] = _opt_int(m.get(term)) if m else None
        row["parse_errors"] = _opt_int(m.get("parse_errors")) if m else None
        row["dlq_rate"] = dlq_rate(dlq, bronze)
        row["reconcile"] = _status(m)
        row["freshness_min"] = minutes_between(now, ingest)
        row["ingest_lag_min"] = minutes_between(ingest, f.get("max_kafka_ts") if f else None)
        out.append({c: row[c] for c in REPORT_COLUMNS})
    return out


def build_batch_totals(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Per ``source_table`` totals over the ``scope='batch'`` rows (summed; NULL counts as 0).

    Input rows carry ``source_table``, ``batches``, ``unbalanced_batches`` and the count terms
    (the Spark job pre-aggregates; several rows per table are summed again). Batch rows have no
    tenant, so these totals are per table only and are the only source of
    ``stale_cdc_noop`` / ``duplicates_removed`` for customers.
    """
    sums: dict[str, dict[str, int]] = {}
    for row in rows:
        acc = sums.setdefault(
            str(row["source_table"]),
            dict.fromkeys(("batches", "unbalanced_batches", "bronze", *_COUNT_TERMS), 0),
        )
        for name in acc:
            acc[name] += int(row.get(name) or 0)
    out: list[dict[str, Any]] = []
    for table, acc in sorted(sums.items()):
        entry: dict[str, Any] = {
            "source_table": table,
            **acc,
            "dlq_rate": dlq_rate(acc["dlq"], acc["bronze"]),
        }
        out.append({c: entry[c] for c in BATCH_COLUMNS})
    return out


def _cell(value: object) -> str:
    if value is None:
        return NA
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value).replace("|", "\\|")


def _table(columns: Sequence[str], rows: Iterable[Mapping[str, Any]]) -> list[str]:
    lines = [
        "| " + " | ".join(columns) + " |",
        "|" + "|".join(" --- " for _ in columns) + "|",
    ]
    lines += ["| " + " | ".join(_cell(r.get(c)) for c in columns) + " |" for r in rows]
    return lines


def to_markdown(
    rows: Sequence[Mapping[str, Any]],
    batch_totals: Sequence[Mapping[str, Any]],
    *,
    run_id: str,
    generated_at: datetime,
    env: str,
) -> str:
    """Markdown report: per-tenant table, per-table batch totals and the column legend."""
    lines = [
        f"# Ops metrics report `{run_id}`",
        "",
        f"Generated {generated_at:%Y-%m-%d %H:%M:%S} UTC, environment `{env}`.",
        "",
        "## Per table and tenant (latest reconcile run)",
        "",
        *_table(REPORT_COLUMNS, rows),
        "",
        "## Per table, all recorded micro-batches (no tenant split)",
        "",
        *_table(BATCH_COLUMNS, batch_totals),
        "",
        "## Reading this report",
        "",
        f"- `{NA}` = not measurable for that key (never 0). Customers' `silver_accepted`,"
        " `duplicates_removed` and `stale_cdc_noop` are not derivable from data per tenant.",
        "- `freshness_min` = now - max(`_ingest_ts`): staleness of Bronze, not end-to-end latency.",
        "- `ingest_lag_min` = max(`_ingest_ts`) - max(`_kafka_ts`), only where `_kafka_ts` exists."
        " It is not a Kafka consumer offset lag (offsets are not available on Glue).",
        "- `reconcile` `MISMATCH`: see `reconcile.mismatch` logs and"
        " `docs/runbooks/incident-triage.md`.",
        "",
    ]
    return "\n".join(lines)


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"not JSON serialisable: {type(value).__name__}")


def to_json(
    rows: Sequence[Mapping[str, Any]],
    batch_totals: Sequence[Mapping[str, Any]],
    *,
    run_id: str,
    generated_at: datetime,
    env: str,
) -> str:
    """The same content as :func:`to_markdown`, machine readable (NULL stays ``null``)."""
    document = {
        "run_id": run_id,
        "generated_at": generated_at.isoformat(),
        "env": env,
        "rows": list(rows),
        "batch_totals": list(batch_totals),
    }
    return json.dumps(document, default=_json_default, indent=2, sort_keys=True) + "\n"
