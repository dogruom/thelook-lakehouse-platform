"""Integration helper for the Airflow container: pure parts only (offline, no Docker)."""

from __future__ import annotations

import json

import pytest

from tests.integration import airflow_cli


def _output(state: str = "failed") -> str:
    data = {
        "run_id": "manual__x",
        "state": "DagRunState.FAILED" if state == "failed" else state,
        "tasks": [
            {"task_id": "landing_check", "map_index": 0, "state": "success"},
            {"task_id": "landing_check", "map_index": 1, "state": "TaskInstanceState.FAILED"},
            {"task_id": "landing_check", "map_index": 2, "state": "success"},
            {"task_id": "summarize_landing", "map_index": -1, "state": "success"},
            {"task_id": "run_verdict", "map_index": -1, "state": None},
        ],
    }
    return "noise\n" + airflow_cli.STATES_MARKER + json.dumps(data) + "\n"


def test_parse_run_states() -> None:
    run = airflow_cli.parse_run_states(_output())
    assert run.state == "failed"
    assert run.mapped_states("landing_check") == {0: "success", 1: "failed", 2: "success"}
    assert run.state_of("summarize_landing") == "success"
    assert run.state_of("run_verdict") == "none"
    assert run.state_of("missing") is None


def test_parse_without_marker_or_run_fails() -> None:
    with pytest.raises(ValueError, match="no RUN_STATES"):
        airflow_cli.parse_run_states("nothing")
    with pytest.raises(ValueError, match="no run"):
        airflow_cli.parse_run_states(airflow_cli.STATES_MARKER + "null")


def test_dags_test_command_passes_conf_as_one_argument() -> None:
    cmd = airflow_cli.dags_test_command("2026-01-05", {"fail_brand": "brand_beta"})
    assert cmd[:5] == ["airflow", "dags", "test", "lakehouse_pipeline", "2026-01-05"]
    assert json.loads(cmd[cmd.index("--conf") + 1]) == {"fail_brand": "brand_beta"}
    assert "--conf" not in airflow_cli.dags_test_command("2026-01-05")


def test_states_script_compiles_and_targets_the_dag() -> None:
    compile(airflow_cli.STATES_SCRIPT, "<states>", "exec")
    assert "lakehouse_pipeline" in airflow_cli.STATES_SCRIPT


def test_skip_reason_is_actionable() -> None:
    assert "crm_airflow_scheduler" in airflow_cli.SKIP_REASON
    assert "orchestration" in airflow_cli.SKIP_REASON
