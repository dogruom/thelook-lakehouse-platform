"""``scripts/cloud/run_job.py``: exit codes, cost math, CSV row, sequence (Glue stubbed)."""

from __future__ import annotations

import csv
from typing import TYPE_CHECKING, Any

import pytest

from .conftest import load_script


if TYPE_CHECKING:
    from pathlib import Path
    from types import ModuleType

HEADER = "date,job,run_id,state,execution_seconds,workers,estimated_usd,artifact_sha"
PREFIX = "crm-lakehouse-dev-"
RECONCILE = f"{PREFIX}reconcile"


@pytest.fixture(scope="module")
def rj() -> ModuleType:
    return load_script("cloud/run_job")


class FakeGlue:
    """start_job_run + get_job_run; ``runs`` maps a Glue job name to the polled run dicts."""

    def __init__(self, runs: dict[str, list[dict[str, Any]]] | None = None) -> None:
        self.runs = runs or {}
        self.started: list[dict[str, Any]] = []

    def start_job_run(self, **kwargs: Any) -> dict[str, str]:
        self.started.append(kwargs)
        return {"JobRunId": f"jr_{len(self.started)}"}

    def get_job_run(self, JobName: str, RunId: str) -> dict[str, Any]:  # noqa: ARG002
        queue = self.runs.get(JobName) or [{"JobRunState": "SUCCEEDED", "ExecutionTime": 90}]
        run = queue.pop(0) if len(queue) > 1 else queue[0]
        return {"JobRun": {"NumberOfWorkers": 2, **run}}


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, rj: ModuleType) -> None:
    monkeypatch.setenv("LAKEHOUSE_AWS_BUCKET", "crm-lakehouse-dev-123456789012")
    monkeypatch.delenv("LAKEHOUSE_GLUE_PREFIX", raising=False)
    monkeypatch.setattr(rj, "CSV_PATH", tmp_path / "cloud_runs.csv")


def run(rj: ModuleType, glue: FakeGlue, *argv: str) -> int:
    return int(rj.main(list(argv), glue=glue, sleep=lambda _s: None))


def state_run(state: str, seconds: int = 90, **extra: Any) -> dict[str, list[dict[str, Any]]]:
    return {RECONCILE: [{"JobRunState": state, "ExecutionTime": seconds, **extra}]}


@pytest.mark.parametrize(
    ("seconds", "usd"),
    [(0, 0.0147), (30, 0.0147), (60, 0.0147), (600, 0.1467), (3600, 0.88)],
)
def test_cost_math_has_a_one_minute_minimum(rj: ModuleType, seconds: int, usd: float) -> None:
    assert rj.estimate_cost_usd(seconds, 2) == pytest.approx(usd, abs=1e-4)


def test_success_exits_0_and_starts_the_prefixed_job(
    rj: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    glue = FakeGlue()
    assert run(rj, glue, "silver_orders") == 0
    assert glue.started == [{"JobName": f"{PREFIX}silver_orders"}]
    out = capsys.readouterr().out
    assert "state=SUCCEEDED" in out
    assert "estimated_cost=USD 0.0220" in out  # 90 s x 2 DPU x 0.44 / 3600


@pytest.mark.parametrize("state", ["FAILED", "STOPPED", "TIMEOUT", "ERROR"])
def test_non_success_states_exit_1_and_print_the_error(
    rj: ModuleType, state: str, capsys: pytest.CaptureFixture[str]
) -> None:
    glue = FakeGlue(state_run(state, ErrorMessage="boom"))
    assert run(rj, glue, "reconcile") == 1
    assert "ErrorMessage: boom" in capsys.readouterr().err


def test_polls_until_terminal(rj: ModuleType) -> None:
    glue = FakeGlue(
        {
            RECONCILE: [
                {"JobRunState": "STARTING"},
                {"JobRunState": "RUNNING"},
                {"JobRunState": "SUCCEEDED", "ExecutionTime": 120},
            ]
        }
    )
    assert run(rj, glue, "reconcile") == 0


def test_wait_timeout_exits_1(rj: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    glue = FakeGlue({RECONCILE: [{"JobRunState": "RUNNING"}]})
    ticks = iter(range(0, 10_000, 100))
    code = rj.main(
        ["reconcile", "--timeout-seconds", "250"],
        glue=glue,
        sleep=lambda _s: None,
        clock=lambda: float(next(ticks)),
    )
    assert code == 1
    assert "NOT stopped" in capsys.readouterr().err


def test_no_wait_returns_0_after_start(rj: ModuleType) -> None:
    glue = FakeGlue()
    assert run(rj, glue, "silver_orders", "--no-wait") == 0
    assert len(glue.started) == 1


def test_artifact_sha_and_args_become_run_arguments(rj: ModuleType) -> None:
    glue = FakeGlue()
    code = run(
        rj,
        glue,
        "dlq_gate",
        "--artifact-sha",
        "abcdef012345",
        "--args-json",
        '["--table","raw_orders"]',
    )
    assert code == 0
    assert glue.started[0]["Arguments"] == {
        "--additional-python-modules": (
            "s3://crm-lakehouse-dev-123456789012/artifacts/abcdef012345/"
            "multitenant_crm_lakehouse-0.1.0-py3-none-any.whl"
        ),
        "--lakehouse_args": '["--table", "raw_orders"]',
    }


def test_repeatable_arg_becomes_one_list_element_each(rj: ModuleType) -> None:
    glue = FakeGlue()
    assert run(rj, glue, "dlq_gate", "--arg=--table", "--arg=raw_orders", "--arg", "x y") == 0
    assert glue.started[0]["Arguments"] == {"--lakehouse_args": '["--table", "raw_orders", "x y"]'}


def test_sequence_needs_the_data_bucket(rj: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LAKEHOUSE_AWS_BUCKET")
    glue = FakeGlue()
    with pytest.raises(SystemExit) as exc:
        run(rj, glue, "--sequence", "cloud")
    assert exc.value.code == 2
    assert glue.started == []


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["BAD"],
        ["a_b", "--sequence", "cloud"],
        ["silver_orders", "--args-json", "nope"],
        ["silver_orders", "--args-json", "[1]"],
        ["silver_orders", "--artifact-sha", "XYZ"],
        ["silver_orders", "--no-wait", "--record"],
        ["--sequence", "cloud", "--args-json", "[]"],
        ["--sequence", "cloud", "--arg=--table"],
        ["silver_orders", "--arg=--table", "--args-json", '["x"]'],
        ["silver_orders", "--timeout-seconds", "0"],
    ],
)
def test_usage_errors_exit_2(rj: ModuleType, argv: list[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        run(rj, FakeGlue(), *argv)
    assert exc.value.code == 2


def test_record_appends_header_once_and_one_row_per_run(rj: ModuleType, tmp_path: Path) -> None:
    glue = FakeGlue(state_run("SUCCEEDED", 600))
    assert run(rj, glue, "reconcile", "--record", "--artifact-sha", "abcdef012345") == 0
    assert run(rj, glue, "reconcile", "--record") == 0
    lines = (tmp_path / "cloud_runs.csv").read_text(encoding="utf-8").splitlines()
    assert lines[0] == HEADER
    assert len(lines) == 3
    rows = list(csv.DictReader(lines))
    assert rows[0]["job"] == "reconcile"
    assert rows[0]["run_id"] == "jr_1"
    assert rows[0]["state"] == "SUCCEEDED"
    assert rows[0]["execution_seconds"] == "600"
    assert rows[0]["workers"] == "2"
    assert rows[0]["estimated_usd"] == "0.1467"
    assert rows[0]["artifact_sha"] == "abcdef012345"
    assert rows[1]["artifact_sha"] == "current"
    assert len(rows[0]["date"]) == 10


def test_failed_run_is_recorded_too(rj: ModuleType, tmp_path: Path) -> None:
    glue = FakeGlue(state_run("FAILED", 5))
    assert run(rj, glue, "reconcile", "--record") == 1
    text = (tmp_path / "cloud_runs.csv").read_text(encoding="utf-8").splitlines()
    rows = list(csv.DictReader(text))
    assert rows[0]["state"] == "FAILED"
    assert rows[0]["estimated_usd"] == "0.0147"


def test_sequence_runs_the_cloud_order(rj: ModuleType) -> None:
    glue = FakeGlue()
    assert run(rj, glue, "--sequence", "cloud") == 0
    names = [s["JobName"].removeprefix(PREFIX) for s in glue.started]
    assert names == [
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
    ]
    assert glue.started[-1]["Arguments"] == {
        "--lakehouse_args": '["--output-uri", "s3://crm-lakehouse-dev-123456789012/ops/reports"]'
    }
    assert glue.started[2]["Arguments"]["--lakehouse_args"] == (
        '["--table", "raw_customers", "--table", "raw_clickstream", '
        '"--table", "raw_orders", "--table", "raw_products"]'
    )


def test_sequence_stops_at_the_first_failure(rj: ModuleType) -> None:
    glue = FakeGlue({f"{PREFIX}dlq_gate": [{"JobRunState": "FAILED"}]})
    assert run(rj, glue, "--sequence", "cloud") == 1
    assert glue.started[-1]["JobName"] == f"{PREFIX}dlq_gate"


def test_aws_error_exits_1(rj: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    class Broken(FakeGlue):
        def start_job_run(self, **_kw: Any) -> dict[str, str]:
            raise RuntimeError("ConcurrentRunsExceededException")

    assert run(rj, Broken(), "reconcile") == 1
    assert "ConcurrentRunsExceededException" in capsys.readouterr().err
