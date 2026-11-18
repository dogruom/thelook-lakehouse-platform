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


@pytest.mark.parametrize("bucket", ["crm-lakehouse-dev-123456789012", "crm-lakehouse-x"])
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
        "abc",  # valid S3 name, but not a crm-lakehouse-* data bucket (D-102)
        "my-data-bucket",
        "crm-lakehouse-",
        "other-crm-lakehouse-dev",
        None,
        5,
    ],
)
def test_bad_bucket_is_rejected(bucket: object) -> None:
    with pytest.raises(ValueError, match="bucket"):
        cloud.validate_bucket(bucket)


def test_bucket_rule_is_the_shared_aws_rule() -> None:
    from lakehouse.common import aws
    from lakehouse.generator import landing

    assert landing.AWS_BUCKET_RE is aws.AWS_BUCKET_RE
    assert aws.is_data_bucket("crm-lakehouse-dev-1")
    assert not aws.is_data_bucket("lakehouse")
    assert not aws.is_data_bucket(None)


# ---- local-endpoint guard and active-run check (no AWS: dict env, fake and stubbed clients) -----


@pytest.mark.parametrize("name", cloud.LOCAL_ENDPOINT_ENV_VARS)
def test_local_endpoint_variable_is_refused(name: str) -> None:
    with pytest.raises(ValueError, match=name) as err:
        cloud.validate_cloud_environment({name: "http://seaweedfs:9000"})
    assert "seaweedfs" not in str(err.value)  # names the variable, never its value
    assert "env -u AWS_ENDPOINT_URL" in str(err.value)


def test_any_service_specific_endpoint_is_refused() -> None:
    # not in LOCAL_ENDPOINT_ENV_VARS: the guard matches the prefix, not a fixed list
    with pytest.raises(ValueError, match="AWS_ENDPOINT_URL_ATHENA"):
        cloud.validate_cloud_environment({"AWS_ENDPOINT_URL_ATHENA": "http://localhost:1"})


@pytest.mark.parametrize("env", [{}, {"AWS_ENDPOINT_URL": ""}, {"AWS_ENDPOINT_URL": "  "}])
def test_unset_or_empty_endpoint_is_fine(env: dict[str, str]) -> None:
    cloud.validate_cloud_environment({**env, "AWS_REGION": "eu-central-1"})


class FakeGlue:
    """``get_job_runs`` over ``{glue job: [pages]}``; records every call."""

    def __init__(self, pages: dict[str, list[list[dict[str, str]]]] | None = None) -> None:
        self.pages = pages or {}
        self.calls: list[dict[str, object]] = []

    def get_job_runs(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        pages = self.pages.get(str(kwargs["JobName"]), [[]])
        index = int(str(kwargs.get("NextToken", "0")))
        out: dict[str, object] = {"JobRuns": pages[index]}
        if index + 1 < len(pages):
            out["NextToken"] = str(index + 1)
        return out


PREFIX = "crm-lakehouse-dev"


def test_no_runs_means_no_active_runs() -> None:
    glue = FakeGlue()
    assert cloud.active_job_runs(glue, PREFIX, cloud.cloud_job_names()) == []
    assert len(glue.calls) == len(cloud.cloud_job_names())
    cloud.assert_no_active_runs(glue, PREFIX, cloud.cloud_job_names())


def test_only_active_states_count_and_all_pages_are_read() -> None:
    runs = {
        f"{PREFIX}-silver_orders": [
            [{"Id": "jr_old", "JobRunState": "SUCCEEDED"}],
            [{"Id": "jr_new", "JobRunState": "RUNNING"}],
        ],
        f"{PREFIX}-reconcile": [
            [
                {"Id": "jr_f", "JobRunState": "FAILED"},
                {"Id": "jr_t", "JobRunState": "TIMEOUT"},
                {"Id": "jr_s", "JobRunState": "STOPPING"},
                {"Id": "jr_w", "JobRunState": "WAITING"},
                {"Id": "jr_b", "JobRunState": "STARTING"},
            ]
        ],
    }
    active = cloud.active_job_runs(FakeGlue(runs), PREFIX, ["silver_orders", "reconcile"])
    assert active == [
        ("silver_orders", "jr_new", "RUNNING"),
        ("reconcile", "jr_s", "STOPPING"),
        ("reconcile", "jr_w", "WAITING"),
        ("reconcile", "jr_b", "STARTING"),
    ]


def test_assert_no_active_runs_names_job_run_and_state() -> None:
    glue = FakeGlue({f"{PREFIX}-bronze_batch": [[{"Id": "jr_1", "JobRunState": "RUNNING"}]]})
    with pytest.raises(cloud.ActiveRunsError, match=r"bronze_batch run jr_1 \(RUNNING\)") as err:
        cloud.assert_no_active_runs(glue, PREFIX, cloud.cloud_job_names())
    assert err.value.runs == [("bronze_batch", "jr_1", "RUNNING")]


def test_duplicate_jobs_are_checked_once_and_bad_prefix_is_refused() -> None:
    glue = FakeGlue()
    cloud.active_job_runs(glue, PREFIX, ["reconcile", "reconcile"])
    assert len(glue.calls) == 1
    with pytest.raises(ValueError, match="glue_prefix"):
        cloud.active_job_runs(glue, "Bad Prefix", ["reconcile"])


def test_aws_errors_are_not_swallowed() -> None:
    class Broken:
        def get_job_runs(self, **_kw: object) -> None:
            raise RuntimeError("no credentials")

    with pytest.raises(RuntimeError, match="no credentials"):
        cloud.assert_no_active_runs(Broken(), PREFIX, ["reconcile"])


def test_call_shape_matches_the_glue_api_without_touching_aws() -> None:
    boto3 = pytest.importorskip("boto3")
    from botocore.stub import Stubber

    client = boto3.client(
        "glue", region_name="eu-central-1", aws_access_key_id="x", aws_secret_access_key="x"
    )
    with Stubber(client) as stub:
        stub.add_response(
            "get_job_runs",
            {"JobRuns": [{"Id": "jr_9", "JobRunState": "RUNNING"}]},
            {"JobName": f"{PREFIX}-reconcile", "MaxResults": 200},
        )
        assert cloud.active_job_runs(client, PREFIX, ["reconcile"]) == [
            ("reconcile", "jr_9", "RUNNING")
        ]
        stub.assert_no_pending_responses()


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
