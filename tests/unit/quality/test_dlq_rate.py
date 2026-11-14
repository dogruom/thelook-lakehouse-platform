"""Pure DLQ-rate logic (W4.D23.2): rate, threshold boundary, per-brand breach, report (offline)."""

from __future__ import annotations

import json

import pytest

from lakehouse.quality import dlq_rate as dr


SOURCES = ["raw_customers", "raw_orders"]


def test_rate_basic_and_no_data() -> None:
    assert dr.dlq_rate(15, 100) == 0.15
    assert dr.dlq_rate(0, 100) == 0.0
    assert dr.dlq_rate(0, 0) is None


def test_dlq_rows_without_bronze_count_as_full_rate() -> None:
    assert dr.dlq_rate(3, 0) == 1.0


@pytest.mark.parametrize("bad", [-0.01, 1.01, float("nan")])
def test_max_rate_must_be_within_unit_interval(bad: float) -> None:
    with pytest.raises(ValueError, match="within"):
        dr.validate_max_rate(bad)
    with pytest.raises(ValueError, match="within"):
        dr.evaluate_rates(["brand_a"], SOURCES, {}, bad)


def test_equal_to_threshold_passes_above_breaches() -> None:
    counts = {
        ("brand_a", "raw_customers"): (100, 15),  # exactly 0.15: passes
        ("brand_b", "raw_customers"): (100, 16),  # above: breaches
    }
    results = dr.evaluate_rates(["brand_a", "brand_b"], ["raw_customers"], counts, 0.15)
    assert [r.breached for r in results] == [False, True]
    assert dr.breached_brands(results) == ["brand_b"]


def test_per_brand_isolation_and_sorted_unique_breach_list() -> None:
    counts = {
        ("brand_gamma", "raw_customers"): (10, 9),
        ("brand_gamma", "raw_orders"): (10, 9),
        ("brand_alpha", "raw_customers"): (1000, 10),
        ("brand_beta", "raw_orders"): (10, 8),
    }
    results = dr.evaluate_rates(["brand_gamma", "brand_alpha", "brand_beta"], SOURCES, counts, 0.5)
    assert dr.breached_brands(results) == ["brand_beta", "brand_gamma"]
    assert [r.brand_id for r in results[:2]] == ["brand_gamma", "brand_gamma"]


def test_missing_counts_mean_no_data_and_never_breach() -> None:
    results = dr.evaluate_rates(["brand_a"], SOURCES, {}, 0.0)
    assert all(r.dlq_rate is None and not r.breached for r in results)


def test_zero_threshold_breaches_on_any_dlq_row() -> None:
    results = dr.evaluate_rates(
        ["brand_a"], ["raw_customers"], {("brand_a", "raw_customers"): (50, 1)}, 0.0
    )
    assert dr.breached_brands(results) == ["brand_a"]


def test_report_is_json_serialisable_and_rounded() -> None:
    results = dr.evaluate_rates(["brand_a"], SOURCES, {("brand_a", "raw_orders"): (3, 1)}, 0.5)
    rep = dr.report(results)
    assert rep["brand_a"]["raw_orders"] == {
        "bronze_rows": 3,
        "dlq_rows": 1,
        "dlq_rate": 0.333333,
        "breached": False,
    }
    assert rep["brand_a"]["raw_customers"]["dlq_rate"] is None
    json.dumps(rep)


def test_breach_message_names_brands_and_has_counts_only() -> None:
    results = dr.evaluate_rates(
        ["brand_a", "brand_b"],
        ["raw_customers"],
        {("brand_a", "raw_customers"): (10, 9), ("brand_b", "raw_customers"): (10, 1)},
        0.2,
    )
    message = dr.breach_message(results, 0.2)
    assert "brand_a" in message
    assert "brand_b" not in message
    assert "0.9000" in message
