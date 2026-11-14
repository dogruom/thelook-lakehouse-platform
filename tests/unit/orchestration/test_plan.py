"""Run plan: params validation, failure injection, job lists, trigger-rule table (offline)."""

from __future__ import annotations

import pytest

from lakehouse.orchestration import plan


def test_defaults_are_the_three_tenants_and_no_failure() -> None:
    p = plan.build_run_plan()
    assert p.brands == ("brand_alpha", "brand_beta", "brand_gamma")
    assert p.fail_brand == "none"
    assert p.max_dlq_rate == plan.DEFAULT_MAX_DLQ_RATE == 0.15


def test_fail_brand_must_be_none_or_planned() -> None:
    assert (
        plan.build_run_plan(["brand_alpha", "brand_beta"], "brand_beta").fail_brand == "brand_beta"
    )
    with pytest.raises(ValueError, match="fail_brand"):
        plan.build_run_plan(["brand_alpha"], "brand_beta")


@pytest.mark.parametrize(
    "brands",
    [[], None, "brand_alpha", ["Brand_A"], ["brand_alpha", "brand_alpha"], ["brand_a\n"], [1]],
)
def test_invalid_brands_are_rejected(brands: object) -> None:
    with pytest.raises(ValueError, match="brands"):
        plan.build_run_plan(brands)


def test_error_message_does_not_echo_the_bad_brand() -> None:
    with pytest.raises(ValueError) as err:
        plan.build_run_plan(["jane.doe@example.com"])
    assert "jane" not in str(err.value)


@pytest.mark.parametrize("value", [-0.1, 1.1, float("nan"), "0.1", True, None])
def test_invalid_max_dlq_rate(value: object) -> None:
    with pytest.raises(ValueError, match="max_dlq_rate"):
        plan.validate_max_dlq_rate(value)


@pytest.mark.parametrize("value", [0, 0.0, 0.15, 1, 1.0])
def test_valid_max_dlq_rate(value: float) -> None:
    assert plan.validate_max_dlq_rate(value) == float(value)


def test_should_inject_failure() -> None:
    assert plan.should_inject_failure("brand_beta", "brand_beta")
    assert not plan.should_inject_failure("brand_alpha", "brand_beta")
    assert not plan.should_inject_failure("brand_beta", plan.NO_FAILURE)


def test_job_order_follows_the_pipeline() -> None:
    assert plan.pipeline_job_names() == (
        "bronze_stream",
        "bronze_batch",
        "dlq_gate",
        "silver_customers",
        "silver_customers_history",
        "silver_orders",
        "silver_products",
        "silver_clickstream",
        "reconcile",
    )


def test_dlq_gate_covers_all_four_bronze_tables_and_reconcile_is_not_retried() -> None:
    tables = [a for a in plan.DLQ_GATE_JOB.args if a != "--table"]
    assert tables == ["raw_customers", "raw_clickstream", "raw_orders", "raw_products"]
    assert plan.RECONCILE_JOB.retries == 0
    assert all(j.retries >= 1 for j in plan.SILVER_JOBS)


def test_task_timeout_exceeds_the_spark_timeout() -> None:
    assert plan.task_timeout_s(900) == 900 + plan.TASK_TIMEOUT_MARGIN_S


def test_trigger_rules_are_documented_for_every_special_task() -> None:
    rules = {task: rule for task, rule, _ in plan.TRIGGER_RULES}
    assert rules["summarize_landing"] == "all_done"
    assert rules["run_verdict"] == "one_failed"
    assert rules["dbt_build"] == "all_success"
    assert all(why for _, _, why in plan.TRIGGER_RULES)


def test_job_names_resolve_to_real_job_modules() -> None:
    """Every job the DAG submits exists as ``lakehouse.<component>.jobs.<name>``."""
    from lakehouse.common.jobs import resolve_job

    for name in (*plan.pipeline_job_names(), plan.DLQ_RATE_GATE_JOB):
        assert resolve_job(name).name == name
