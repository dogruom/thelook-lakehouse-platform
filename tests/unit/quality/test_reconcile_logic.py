"""Reconciliation equation and data-derived totals: pure logic, offline (W3.D18, D-081/D-082)."""

from __future__ import annotations

from typing import Any

from lakehouse.quality.jobs.reconcile import (
    SOURCES,
    Source,
    batch_violations,
    compare_totals,
    data_rows,
)


ORDERS = Source("raw_orders", "dlq_orders", "orders")
CUSTOMERS = Source("raw_customers", "dlq_customers", None)


def _batch(**over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "job": "silver_orders",
        "source_table": "raw_orders",
        "batch_id": 3,
        "rows_read": 10,
        "reread_rows": 1,
        "parse_errors": 1,
        "bronze": 8,
        "silver_accepted": 5,
        "dlq": 1,
        "duplicates_removed": 1,
        "stale_cdc_noop": 1,
        "late_beyond_window": 0,
        "balanced": True,
    }
    row.update(over)
    return row


def test_sources_pair_each_bronze_table_with_its_dlq() -> None:
    assert {s.bronze: s.dlq for s in SOURCES} == {
        "raw_customers": "dlq_customers",
        "raw_clickstream": "dlq_clickstream",
        "raw_orders": "dlq_orders",
    }
    assert next(s for s in SOURCES if s.bronze == "raw_customers").silver is None


def test_balanced_batch_has_no_violation() -> None:
    assert batch_violations([_batch()]) == []


def test_terms_not_summing_to_bronze_is_reported() -> None:
    out = batch_violations([_batch(silver_accepted=4)])
    assert len(out) == 1
    assert "terms_do_not_sum_to_bronze" in out[0]["reasons"]
    assert out[0]["batch_id"] == 3
    assert out[0]["bronze"] == 8


def test_stored_flag_false_is_reported_even_if_terms_add_up() -> None:
    out = batch_violations([_batch(balanced=False)])
    assert out[0]["reasons"] == ["balanced_flag_not_true"]


def test_bronze_inconsistent_with_rows_minus_reread_minus_parse_is_reported() -> None:
    # terms sum to 9 == bronze, but rows_read - reread - parse is 8
    out = batch_violations([_batch(bronze=9, silver_accepted=6)])
    assert out[0]["reasons"] == ["bronze_not_rows_minus_reread_minus_parse"]


def test_null_counts_are_treated_as_zero() -> None:
    row = _batch(
        rows_read=None,
        reread_rows=None,
        parse_errors=None,
        bronze=None,
        silver_accepted=None,
        dlq=None,
        duplicates_removed=None,
        stale_cdc_noop=None,
        late_beyond_window=None,
    )
    assert batch_violations([row]) == []


def test_only_the_bad_batch_of_several_is_listed() -> None:
    out = batch_violations([_batch(batch_id=1), _batch(batch_id=2, dlq=0), _batch(batch_id=4)])
    assert [p["batch_id"] for p in out] == [2]


def test_data_rows_for_silver_backed_source_compute_remainder_and_reread() -> None:
    totals = {"b1": {"rows": 12, "bronze": 10, "parse_errors": 1, "dlq": 2, "silver": 6}}
    rows, problems = data_rows(ORDERS, totals)
    assert problems == []
    (r,) = rows
    assert r["reread_rows"] == 1  # 12 - 10 - 1
    assert r["silver_accepted"] == 6
    assert r["dlq"] == 2
    assert r["duplicates_removed"] == 2  # 10 - 6 - 2
    assert r["stale_cdc_noop"] == 0
    assert r["late_beyond_window"] == 0
    assert r["scope"] == "reconcile"
    assert r["balanced"] is True
    assert r["brand_id"] == "b1"


def test_negative_remainder_is_a_problem_and_marks_row_unbalanced() -> None:
    totals = {"b1": {"rows": 5, "bronze": 5, "parse_errors": 0, "dlq": 1, "silver": 6}}
    rows, problems = data_rows(ORDERS, totals)
    assert rows[0]["balanced"] is False
    assert problems == [
        {
            "check": "data_remainder_negative",
            "source_table": "raw_orders",
            "brand_id": "b1",
            "bronze": 5,
            "silver_accepted": 6,
            "dlq": 1,
        }
    ]


def test_customers_row_has_null_terms_that_data_does_not_determine() -> None:
    totals = {"b1": {"rows": 4, "bronze": 4, "parse_errors": 0, "dlq": 1}}
    (r,), problems = data_rows(CUSTOMERS, totals)
    assert problems == []
    assert r["silver_accepted"] is None
    assert r["duplicates_removed"] is None
    assert r["stale_cdc_noop"] is None
    assert r["late_beyond_window"] is None


def test_customers_dlq_larger_than_bronze_is_a_problem() -> None:
    totals = {"b1": {"rows": 1, "bronze": 1, "parse_errors": 0, "dlq": 2}}
    _, problems = data_rows(CUSTOMERS, totals)
    assert len(problems) == 1
    assert problems[0]["silver_accepted"] is None


def test_data_rows_are_sorted_per_brand_and_tenant_separated() -> None:
    t = {"rows": 1, "bronze": 1, "parse_errors": 0, "dlq": 0, "silver": 1}
    rows, _ = data_rows(ORDERS, {"b2": t, "b1": {**t, "silver": 0, "dlq": 1}})
    assert [r["brand_id"] for r in rows] == ["b1", "b2"]
    assert rows[0]["dlq"] == 1
    assert rows[1]["dlq"] == 0


def test_compare_totals_is_empty_when_batch_sums_match_data() -> None:
    totals = {"b1": {"rows": 10, "bronze": 8, "parse_errors": 1, "dlq": 1, "silver": 5}}
    data, _ = data_rows(ORDERS, totals)
    # data: reread = 10 - 8 - 1 = 1; remainder = 8 - 5 - 1 = 2 == dups + stale + late
    assert compare_totals(ORDERS, data, [_batch()]) == []


def test_compare_totals_detects_a_lost_bronze_event() -> None:
    totals = {"b1": {"rows": 10, "bronze": 8, "parse_errors": 1, "dlq": 1, "silver": 5}}
    data, _ = data_rows(ORDERS, totals)
    out = compare_totals(ORDERS, data, [_batch(bronze=7, silver_accepted=4)])
    terms = {m["term"]: m for m in out}
    assert terms["bronze"]["data"] == 8
    assert terms["bronze"]["batch_rows_sum"] == 7
    assert "silver_accepted+dlq" in terms


def test_compare_totals_for_customers_only_checks_base_terms_and_dlq() -> None:
    data = [{"bronze": 4, "parse_errors": 0, "reread_rows": 0, "dlq": 1}]
    batch = [_batch(bronze=4, parse_errors=0, reread_rows=0, dlq=2)]
    out = compare_totals(CUSTOMERS, data, batch)
    assert [m["term"] for m in out] == ["dlq"]
    assert out[0]["source_table"] == "raw_customers"
