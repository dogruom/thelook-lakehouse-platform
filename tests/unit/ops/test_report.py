"""Pure ops report logic (W5.D30): rates, NULL handling, latest-row choice, sorting, formatting."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any

import pytest

from lakehouse.ops.jobs import metrics_report
from lakehouse.ops.report import (
    BATCH_COLUMNS,
    NA,
    REPORT_COLUMNS,
    build_batch_totals,
    build_rows,
    dlq_rate,
    latest_reconcile,
    minutes_between,
    to_json,
    to_markdown,
)


NOW = datetime(2026, 11, 14, 12, 0, 0)


def _rec(table: str = "raw_orders", brand: str | None = "brand_a", **over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "scope": "reconcile",
        "source_table": table,
        "brand_id": brand,
        "run_id": "r1",
        "recorded_at": datetime(2026, 11, 14, 11, 0, 0),
        "balanced": True,
        "bronze": 100,
        "silver_accepted": 90,
        "dlq": 5,
        "duplicates_removed": 4,
        "stale_cdc_noop": 0,
        "late_beyond_window": 1,
        "parse_errors": 2,
    }
    return {**base, **over}


def _fresh(table: str = "raw_orders", brand: str | None = "brand_a", **over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "source_table": table,
        "brand_id": brand,
        "max_ingest_ts": datetime(2026, 11, 14, 11, 30, 0),
    }
    return {**base, **over}


# ---- rates and minutes --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("dlq", "bronze", "expected"),
    [(5, 100, 0.05), (0, 100, 0.0), (5, 0, None), (0, 0, None), (None, 100, None), (5, None, None)],
)
def test_dlq_rate_is_none_for_zero_or_unknown_denominator(
    dlq: int | None, bronze: int | None, expected: float | None
) -> None:
    assert dlq_rate(dlq, bronze) == expected


def test_minutes_between_rounds_clips_and_handles_missing() -> None:
    assert minutes_between(NOW, datetime(2026, 11, 14, 11, 30, 0)) == 30.0
    assert minutes_between(NOW, datetime(2026, 11, 14, 11, 59, 20)) == 0.7
    assert minutes_between(datetime(2026, 11, 14, 11, 0, 0), NOW) == 0.0  # clock skew: clipped
    assert minutes_between(None, NOW) is None
    assert minutes_between(NOW, None) is None


# ---- latest reconcile row -----------------------------------------------------------------------


def test_latest_reconcile_picks_newest_row_per_key_and_ignores_batch_rows() -> None:
    old = _rec(run_id="r1", recorded_at=datetime(2026, 11, 13), dlq=9)
    new = _rec(run_id="r2", recorded_at=datetime(2026, 11, 14), dlq=1)
    batch = _rec(scope="batch", brand=None, run_id="r9", recorded_at=datetime(2026, 11, 15))
    other = _rec(brand="brand_b")
    got = latest_reconcile([new, old, batch, other])
    assert got[("raw_orders", "brand_a")]["dlq"] == 1
    assert set(got) == {("raw_orders", "brand_a"), ("raw_orders", "brand_b")}


def test_latest_reconcile_breaks_a_timestamp_tie_by_run_id() -> None:
    a = _rec(run_id="r1", dlq=1)
    b = _rec(run_id="r2", dlq=2)
    assert latest_reconcile([b, a])[("raw_orders", "brand_a")]["dlq"] == 2


# ---- build_rows ---------------------------------------------------------------------------------


def test_build_rows_computes_rate_status_freshness_and_lag() -> None:
    fresh = _fresh(max_kafka_ts=datetime(2026, 11, 14, 11, 20, 0))
    (row,) = build_rows([_rec()], [fresh], now=NOW)
    assert tuple(row) == REPORT_COLUMNS
    assert row["bronze"] == 100
    assert row["dlq_rate"] == pytest.approx(0.05)
    assert row["reconcile"] == "OK"
    assert row["freshness_min"] == 30.0
    assert row["ingest_lag_min"] == 10.0


def test_build_rows_keeps_terms_the_data_does_not_determine_as_null() -> None:
    rec = _rec("raw_customers", silver_accepted=None, duplicates_removed=None, stale_cdc_noop=None)
    (row,) = build_rows([rec], [_fresh("raw_customers")], now=NOW)
    assert row["silver_accepted"] is None
    assert row["duplicates_removed"] is None
    assert row["stale_cdc_noop"] is None
    assert row["dlq"] == 5  # a known term is not dropped
    assert row["ingest_lag_min"] is None  # no _kafka_ts for this item


def test_build_rows_zero_bronze_gives_null_rate_not_a_division_error() -> None:
    rec = _rec(bronze=0, silver_accepted=0, dlq=0, duplicates_removed=0, late_beyond_window=0)
    (row,) = build_rows([rec], [], now=NOW)
    assert row["dlq_rate"] is None
    assert row["bronze"] == 0
    assert row["freshness_min"] is None  # no Bronze freshness input for this key


def test_build_rows_reports_mismatch_and_missing_reconcile() -> None:
    rows = build_rows([_rec(balanced=False)], [_fresh(), _fresh(brand="brand_z")], now=NOW)
    by_brand = {r["brand_id"]: r for r in rows}
    assert by_brand["brand_a"]["reconcile"] == "MISMATCH"
    assert by_brand["brand_z"]["reconcile"] == NA  # Bronze rows but no reconcile row
    assert by_brand["brand_z"]["bronze"] is None
    assert by_brand["brand_z"]["dlq_rate"] is None
    assert by_brand["brand_z"]["freshness_min"] == 30.0


def test_build_rows_sorts_by_table_then_brand_with_null_brand_last() -> None:
    recs = [
        _rec("raw_orders", "brand_b"),
        _rec("raw_clickstream", None),
        _rec("raw_orders", "brand_a"),
        _rec("raw_clickstream", "brand_z"),
    ]
    keys = [(r["source_table"], r["brand_id"]) for r in build_rows(recs, [], now=NOW)]
    assert keys == [
        ("raw_clickstream", "brand_z"),
        ("raw_clickstream", None),
        ("raw_orders", "brand_a"),
        ("raw_orders", "brand_b"),
    ]


def test_build_rows_empty_inputs_give_no_rows() -> None:
    assert build_rows([], [], now=NOW) == []


# ---- batch totals -------------------------------------------------------------------------------


def test_build_batch_totals_sums_per_table_and_treats_null_as_zero() -> None:
    parts = [
        {"source_table": "raw_orders", "batches": 2, "unbalanced_batches": 1, "bronze": 10,
         "silver_accepted": 6, "dlq": 2, "duplicates_removed": 2, "stale_cdc_noop": None,
         "late_beyond_window": 0, "parse_errors": 1},
        {"source_table": "raw_orders", "batches": 1, "bronze": 10, "silver_accepted": 10},
        {"source_table": "raw_clickstream", "batches": 0},
    ]  # fmt: skip
    totals = build_batch_totals(parts)
    assert [t["source_table"] for t in totals] == ["raw_clickstream", "raw_orders"]
    clicks, orders = totals
    assert tuple(orders) == BATCH_COLUMNS
    assert (orders["batches"], orders["unbalanced_batches"], orders["bronze"]) == (3, 1, 20)
    assert orders["silver_accepted"] == 16
    assert orders["dlq_rate"] == pytest.approx(0.1)
    assert clicks["dlq_rate"] is None  # zero bronze


# ---- formatting ---------------------------------------------------------------------------------


def test_markdown_has_tables_na_for_null_and_the_honest_legend() -> None:
    rows = build_rows(
        [_rec(), _rec("raw_customers", silver_accepted=None)],
        [_fresh(max_kafka_ts=datetime(2026, 11, 14, 11, 20, 0))],
        now=NOW,
    )
    totals = build_batch_totals([{"source_table": "raw_orders", "batches": 1, "bronze": 4}])
    md = to_markdown(rows, totals, run_id="metrics_report_x", generated_at=NOW, env="aws")
    assert md.startswith("# Ops metrics report `metrics_report_x`")
    assert "environment `aws`" in md
    assert "| " + " | ".join(REPORT_COLUMNS) + " |" in md
    assert "| raw_orders | brand_a | 100 | 90 | 5 | 0.05 | 4 | 0 | 1 | 2 | OK | 30 | 10 |" in md
    customers = next(line for line in md.splitlines() if line.startswith("| raw_customers"))
    assert f"| {NA} |" in customers  # silver_accepted is NULL, not 0
    assert "not a Kafka consumer offset lag" in md
    assert "## Per table, all recorded micro-batches" in md


def test_markdown_escapes_pipes_in_cells() -> None:
    row = dict.fromkeys(REPORT_COLUMNS)
    row["source_table"] = "a|b"
    md = to_markdown([row], [], run_id="r", generated_at=NOW, env="local")
    assert "a\\|b" in md


def test_json_roundtrips_with_null_and_iso_timestamps() -> None:
    rows = build_rows([_rec(silver_accepted=None)], [_fresh()], now=NOW)
    doc = json.loads(to_json(rows, [], run_id="r1", generated_at=NOW, env="local"))
    assert doc["run_id"] == "r1"
    assert doc["generated_at"] == "2026-11-14T12:00:00"
    assert doc["rows"][0]["silver_accepted"] is None
    assert doc["rows"][0]["dlq_rate"] == pytest.approx(0.05)
    assert doc["batch_totals"] == []


# ---- job wiring (no Spark) ----------------------------------------------------------------------


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    metrics_report.MetricsReportJob().add_arguments(parser)
    return parser.parse_args(list(argv))


def test_job_is_discoverable_and_defaults_to_all_bronze_tables() -> None:
    from lakehouse.common.jobs import resolve_job

    assert resolve_job("metrics_report") is metrics_report.JOB
    args = _parse()
    assert args.table is None
    assert args.output_uri is None
    assert metrics_report.DEFAULT_TABLES == (
        "raw_customers",
        "raw_clickstream",
        "raw_orders",
        "raw_products",
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("s3://bucket/ops/reports/", "s3://bucket/ops/reports"),
        ("s3a://lakehouse/ops/reports", "s3a://lakehouse/ops/reports"),
        ("/tmp/reports", "/tmp/reports"),
    ],
)
def test_output_uri_accepts_uris_and_strips_the_trailing_slash(value: str, expected: str) -> None:
    assert _parse("--output-uri", value).output_uri == expected


@pytest.mark.parametrize("value", ["reports", "s3://b/../x", "s3://b/has space", "C:relative"])
def test_output_uri_rejects_relative_traversal_and_spaces(
    value: str, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit):
        _parse("--output-uri", value)
    capsys.readouterr()
