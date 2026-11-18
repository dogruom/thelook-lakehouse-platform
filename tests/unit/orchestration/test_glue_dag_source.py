"""Static checks of ``dags/lakehouse_glue.py`` through ``ast`` (Airflow is not installed here).

Pins the D-107 pattern in the cloud DAG: every deliberate failure records its reason through
``_fail()`` and the failure callback reads that key, because ``airflow dags test`` calls the
callback without ``context["exception"]`` (observed 2026-11-18 on the invalid-bucket run).
"""

from __future__ import annotations

import ast
from pathlib import Path


DAG_FILE = Path(__file__).resolve().parents[3] / "dags" / "lakehouse_glue.py"
TREE = ast.parse(DAG_FILE.read_text(encoding="utf-8"))


def _function(name: str) -> ast.FunctionDef:
    funcs = [n for n in ast.walk(TREE) if isinstance(n, ast.FunctionDef) and n.name == name]
    assert len(funcs) == 1, f"expected one {name} definition"
    return funcs[0]


def test_every_raise_in_the_dag_body_goes_through_fail() -> None:
    raises = [
        node
        for node in ast.walk(_function("lakehouse_glue"))
        if isinstance(node, ast.Raise) and node.exc is not None
    ]
    assert raises, "validate_params must raise on bad params"
    for node in raises:
        assert isinstance(node.exc, ast.Call)
        assert getattr(node.exc.func, "id", "") == "_fail", ast.unparse(node)


def test_active_run_check_sits_between_validation_and_the_first_glue_job() -> None:
    body = _function("lakehouse_glue")
    defined = [n.name for n in body.body if isinstance(n, ast.FunctionDef)]
    assert defined == ["validate_params", "check_no_active_runs"]
    text = ast.unparse(body)
    # validate_params -> check_no_active_runs -> first operator (the loop chains from `previous`)
    assert "validated >> previous" in text
    assert text.index("previous: Any = check_no_active_runs()") < text.index("for job, args")
    assert "retries=0" in ast.unparse(_function("check_no_active_runs"))


def test_active_run_check_uses_a_glue_client_from_the_aws_hook_and_fails_closed() -> None:
    check = ast.unparse(_function("check_no_active_runs"))
    assert "AwsBaseHook(aws_conn_id='aws_default', client_type='glue'" in check
    assert "region_name=p.get('region')" in check
    assert "assert_no_active_runs" in check
    assert "except Exception" in check  # an unverifiable check must not let the run go on


def test_validate_params_checks_the_local_endpoint_variables() -> None:
    source = ast.unparse(_function("validate_params"))
    assert "validate_cloud_environment(os.environ)" in source


def test_callback_falls_back_to_the_recorded_reason() -> None:
    source = ast.unparse(_function("alert_on_failure"))
    assert "FAILURE_REASON_KEY" in source
    assert "is_alertable_task" in source
