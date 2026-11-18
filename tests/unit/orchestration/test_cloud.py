"""Unit tests of the cloud (Glue) run plan: names, order, argument JSON, validation."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from lakehouse.orchestration import cloud, plan


def test_order_matches_the_cloud_runner() -> None:
    assert cloud.cloud_job_names() == (
        "register_tables",
        "bronze_batch",
        "dlq_gate",
        "silver_customers",
        "silver_customers_history",
        "silver_orders",
        "silver_products",
        "silver_clickstream",
        "reconcile",
        "metrics_report",
    )


def test_cloud_jobs_are_the_local_chain_without_streaming_and_dbt() -> None:
    local = [n for n in plan.pipeline_job_names() if n != "bronze_stream"]
    cloud_names = [
        n for n in cloud.cloud_job_names() if n not in ("register_tables", "metrics_report")
    ]
    assert cloud_names == local


def test_job_names_are_unique() -> None:
    names = cloud.cloud_job_names()
    assert len(set(names)) == len(names)


def test_glue_job_name_keeps_underscores() -> None:
    assert cloud.glue_job_name("crm-lakehouse-dev", "silver_orders") == (
        "crm-lakehouse-dev-silver_orders"
    )


@pytest.mark.parametrize("prefix", ["", "A", "x", "-abc", "abc-", "a b", "a_b", "x" * 60, 3])
def test_bad_prefix_is_rejected(prefix: object) -> None:
    with pytest.raises(ValueError, match="glue_prefix"):
        cloud.validate_glue_prefix(prefix)


@pytest.mark.parametrize("job", ["", "Silver", "a-b", "1abc", "a b"])
def test_bad_job_is_rejected(job: str) -> None:
    with pytest.raises(ValueError, match="job"):
        cloud.glue_job_name("crm-lakehouse-dev", job)


def test_no_args_means_no_glue_argument() -> None:
    assert cloud.glue_script_args(()) == {}


def test_args_are_a_json_list_of_strings() -> None:
    args = dict(cloud.CLOUD_JOBS)["dlq_gate"]
    out = cloud.glue_script_args(args)
    assert list(out) == ["--lakehouse_args"]
    assert json.loads(out["--lakehouse_args"]) == [
        "--table",
        "raw_customers",
        "--table",
        "raw_clickstream",
        "--table",
        "raw_orders",
        "--table",
        "raw_products",
    ]


def test_metrics_report_output_uri_uses_the_bucket() -> None:
    args = dict(cloud.CLOUD_JOBS)["metrics_report"]
    out = cloud.glue_script_args(args, "my-data-bucket")
    assert json.loads(out["--lakehouse_args"]) == [
        "--output-uri",
        "s3://my-data-bucket/ops/reports",
    ]


def test_bucket_can_be_a_jinja_expression() -> None:
    args = dict(cloud.CLOUD_JOBS)["metrics_report"]
    out = cloud.glue_script_args(args, "{{ params.bucket }}")
    assert "s3://{{ params.bucket }}/ops/reports" in out["--lakehouse_args"]


@pytest.mark.parametrize("bucket", ["crm-lakehouse-dev-123456789012", "abc", "a1b"])
def test_valid_bucket(bucket: str) -> None:
    assert cloud.validate_bucket(bucket) == bucket


@pytest.mark.parametrize(
    "bucket",
    [
        "",
        "ab",
        "s3://bucket",
        "bucket/path",
        "Bucket",
        "-bucket",
        "bucket-",
        "a--b",
        "10-0-0-1",
        "x" * 64,
        None,
        5,
    ],
)
def test_bad_bucket_is_rejected(bucket: object) -> None:
    with pytest.raises(ValueError, match="bucket"):
        cloud.validate_bucket(bucket)


def test_region_validation() -> None:
    assert cloud.validate_region("eu-central-1") == "eu-central-1"
    for bad in ("", "eu_central_1", "europe", "eu-central-", None):
        with pytest.raises(ValueError, match="region"):
            cloud.validate_region(bad)


# ---- static checks of dags/lakehouse_glue.py (Airflow is not installed on the host) ------------

DAG_FILE = Path(__file__).resolve().parents[3] / "dags" / "lakehouse_glue.py"
FORBIDDEN_JOB_CREATION_KWARGS = {
    "script_location",
    "iam_role_name",
    "iam_role_arn",
    "create_job_kwargs",
    "s3_bucket",
    "update_config",
}


def _glue_operator_call() -> ast.Call:
    calls = [
        n
        for n in ast.walk(ast.parse(DAG_FILE.read_text(encoding="utf-8")))
        if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "GlueJobOperator"
    ]
    assert len(calls) == 1, "one GlueJobOperator call inside the CLOUD_JOBS loop"
    return calls[0]


def test_dag_never_creates_or_updates_glue_jobs() -> None:
    kwargs = {kw.arg for kw in _glue_operator_call().keywords}
    assert not kwargs & FORBIDDEN_JOB_CREATION_KWARGS


def test_dag_waits_for_the_run_and_stops_it_on_kill() -> None:
    kwargs = {
        kw.arg: ast.literal_eval(kw.value)
        for kw in _glue_operator_call().keywords
        if isinstance(kw.value, ast.Constant)
    }
    assert kwargs["wait_for_completion"] is True
    assert kwargs["stop_job_run_on_kill"] is True
    assert kwargs["verbose"] is False
    assert kwargs["aws_conn_id"] == "aws_default"


def test_dag_is_manual_only_without_catchup() -> None:
    source = DAG_FILE.read_text(encoding="utf-8")
    assert "dag_id=DAG_ID" in source
    assert "schedule=None" in source
    assert "catchup=False" in source
    assert "max_active_runs=1" in source
    assert cloud.DAG_ID == "lakehouse_glue"
