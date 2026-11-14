"""Static checks of ``dags/lakehouse_pipeline.py`` through ``ast`` (Airflow is not installed here).

The DAG file is never imported by unit tests; its behaviour is covered by the integration tests
that run ``airflow dags test`` in the Airflow container.
"""

from __future__ import annotations

import ast
from functools import cache
from pathlib import Path

import pytest

from lakehouse.orchestration import plan


DAG_FILE = Path(__file__).resolve().parents[3] / "dags" / "lakehouse_pipeline.py"


@cache
def _tree() -> ast.Module:
    return ast.parse(DAG_FILE.read_text(encoding="utf-8"))


def _constants() -> dict[str, object]:
    return {name: getattr(plan, name) for name in dir(plan) if name.isupper()}


def _dag_call() -> ast.Call:
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef):
            for deco in node.decorator_list:
                if isinstance(deco, ast.Call) and getattr(deco.func, "id", "") == "dag":
                    return deco
    pytest.fail("no @dag(...) decorator found")


def _kwargs(call: ast.Call) -> dict[str, ast.expr]:
    return {kw.arg: kw.value for kw in call.keywords if kw.arg}


def _literal(node: ast.expr) -> object:
    if isinstance(node, ast.Name):
        return _constants()[node.id]
    return ast.literal_eval(node)


def test_dag_id_is_lakehouse_pipeline() -> None:
    assert _literal(_kwargs(_dag_call())["dag_id"]) == "lakehouse_pipeline"


def test_one_active_run_and_no_catchup() -> None:
    kwargs = _kwargs(_dag_call())
    assert _literal(kwargs["max_active_runs"]) == 1
    assert _literal(kwargs["catchup"]) is False


def test_params_brands_fail_brand_and_max_dlq_rate_exist() -> None:
    params = _kwargs(_dag_call())["params"]
    assert isinstance(params, ast.Dict)
    assert {ast.literal_eval(k) for k in params.keys if k is not None} == {
        "brands",
        "fail_brand",
        "max_dlq_rate",
    }


def test_failure_callback_is_a_default_arg() -> None:
    default_args = _kwargs(_dag_call())["default_args"]
    assert isinstance(default_args, ast.Dict)
    keys = [ast.literal_eval(k) for k in default_args.keys if k is not None]
    assert "on_failure_callback" in keys


def test_mapping_with_expand_is_used_for_brand_tasks() -> None:
    expands = [
        n
        for n in ast.walk(_tree())
        if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "expand"
    ]
    mapped = {getattr(n.func.value, "id", "") for n in expands if isinstance(n.func, ast.Attribute)}
    assert {"landing_check", "dlq_rate_gate"} <= mapped


def test_retries_and_execution_timeout_are_set_on_tasks() -> None:
    task_calls = [
        n
        for n in ast.walk(_tree())
        if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "task"
    ]
    assert task_calls
    with_retries = [c for c in task_calls if "retries" in _kwargs(c)]
    assert len(with_retries) >= 4
    override_kwargs = [
        _kwargs(n)
        for n in ast.walk(_tree())
        if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "override"
    ]
    assert override_kwargs
    assert all({"retries", "execution_timeout"} <= set(k) for k in override_kwargs)


def test_trigger_rules_come_from_the_documented_constants() -> None:
    used = {}
    for node in ast.walk(_tree()):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "trigger_rule":
                    assert isinstance(kw.value, ast.Name), "use the documented plan constants"
                    used[kw.value.id] = _constants()[kw.value.id]
    assert used == {"TRIGGER_ALL_DONE": "all_done", "TRIGGER_ONE_FAILED": "one_failed"}
    documented = {rule for _, rule, _ in plan.TRIGGER_RULES}
    assert set(used.values()) <= documented


def test_every_task_id_in_the_chain_is_a_known_job() -> None:
    """The DAG builds the chain from the plan lists, so no job name is typed twice."""
    names = {n.id for n in ast.walk(_tree()) if isinstance(n, ast.Name)}
    assert {"BRONZE_JOBS", "DLQ_GATE_JOB", "SILVER_JOBS", "RECONCILE_JOB"} <= names


def test_dag_does_not_import_other_components_or_hold_secrets() -> None:
    imported = []
    for node in ast.walk(_tree()):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
        elif isinstance(node, ast.Import):
            imported.extend(a.name for a in node.names)
    lakehouse = [m for m in imported if m.startswith("lakehouse")]
    assert lakehouse
    assert all(m.startswith("lakehouse.orchestration") for m in lakehouse)
    source = DAG_FILE.read_text(encoding="utf-8").lower()
    assert "password" not in source
    assert "pii_hmac_key" not in source


def test_dlq_gate_task_passes_all_four_bronze_tables() -> None:
    """V-28: dlq_gate gates only customers and clickstream by default (D-077); the DAG must name all four."""
    assert plan.DLQ_GATE_JOB.name == "dlq_gate"
    assert plan.DLQ_GATE_JOB.args == (
        "--table",
        "raw_customers",
        "--table",
        "raw_clickstream",
        "--table",
        "raw_orders",
        "--table",
        "raw_products",
    )
    # the DAG hands the arguments of every JobSpec to the job, unchanged
    helper = next(
        n for n in ast.walk(_tree()) if isinstance(n, ast.FunctionDef) and n.name == "spark"
    )
    assert "job_args=list(spec.args)" in ast.unparse(helper)
    chain = next(
        n for n in ast.walk(_tree()) if isinstance(n, ast.For) and "DLQ_GATE_JOB" in ast.unparse(n)
    )
    assert "spark(spec)" in ast.unparse(chain)


def test_callback_skips_rollup_and_handles_a_missing_exception() -> None:
    source = ast.unparse(
        next(
            n
            for n in ast.walk(_tree())
            if isinstance(n, ast.FunctionDef) and n.name == "alert_on_failure"
        )
    )
    assert "is_alertable_task" in source
    assert "failure_text" in source


def _raised_exception_names(func_name: str) -> set[str]:
    funcs = [n for n in ast.walk(_tree()) if isinstance(n, ast.FunctionDef) and n.name == func_name]
    assert len(funcs) == 1, f"expected one {func_name} definition"
    names = set()
    for node in ast.walk(funcs[0]):
        if isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call):
            names.add(getattr(node.exc.func, "id", ""))
    return names


def test_spark_job_failures_are_retryable() -> None:
    """AirflowFailException skips retries; the Spark jobs' SPARK_RETRIES must stay effective."""
    raised = _raised_exception_names("spark_job")
    assert raised == {"AirflowException"}, f"spark_job raises {raised}"


def test_deliberate_failures_are_not_retried() -> None:
    """Gates and the verdict fail on purpose: a retry would only repeat the same answer."""
    for func_name in ("dlq_rate_gate", "run_verdict"):
        assert _raised_exception_names(func_name) == {"AirflowFailException"}, func_name


def test_spark_tasks_take_retries_from_the_job_spec() -> None:
    """The override must pass ``spec.retries``; a literal would silently bypass SPARK_RETRIES."""
    overrides = [
        n
        for n in ast.walk(_tree())
        if isinstance(n, ast.Call)
        and getattr(n.func, "attr", "") == "override"
        and getattr(n.func.value, "id", "") == "spark_job"
    ]
    assert len(overrides) == 1
    retries = _kwargs(overrides[0])["retries"]
    assert isinstance(retries, ast.Attribute)
    assert (getattr(retries.value, "id", ""), retries.attr) == ("spec", "retries")
