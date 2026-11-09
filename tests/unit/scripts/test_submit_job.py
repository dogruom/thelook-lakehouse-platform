"""Host-side spark-submit wrapper: pure helpers only (no Docker, no subprocess)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    from types import ModuleType

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "submit_job.py"
ZIP = "lakehouse_app-0123456789ab.zip"
MARKER = "marker-abc123"
ENTRY = "/opt/spark-apps/dist/job_entry.py"


@pytest.fixture(scope="module")
def submit() -> ModuleType:
    if not SCRIPT.exists():
        pytest.skip("scripts/submit_job.py not implemented yet")
    spec = importlib.util.spec_from_file_location("_script_submit_job", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _build(submit: ModuleType, job: str, job_args: list[str], marker: str = MARKER) -> list[str]:
    return submit.build_submit_command(job, job_args, app_zip=ZIP, marker=marker)


def _pairs(cmd: list[str], flag: str) -> list[str]:
    return [cmd[i + 1] for i, tok in enumerate(cmd[:-1]) if tok == flag]


def test_command_runs_spark_submit_in_the_master_container(submit: ModuleType) -> None:
    cmd = _build(submit, "hello_delta", ["--expect-packaged"])
    assert cmd[:2] == ["docker", "exec"]
    assert "crm_spark_master" in cmd
    assert "/opt/spark/bin/spark-submit" in cmd
    assert _pairs(cmd, "--master") == ["spark://spark-master:7077"]
    assert _pairs(cmd, "--deploy-mode") == ["client"]


def test_command_ships_zip_as_py_files_and_entry_script(submit: ModuleType) -> None:
    """Executors must receive the code via --py-files (zip), not the source mount."""
    cmd = _build(submit, "hello_delta", [])
    py_files = _pairs(cmd, "--py-files")
    assert len(py_files) == 1 and py_files[0].startswith("/opt/spark-apps/dist/")
    assert py_files[0].endswith(ZIP)
    assert ENTRY in cmd


def test_job_name_and_args_come_after_entry_script_in_order(submit: ModuleType) -> None:
    cmd = _build(submit, "hello_delta", ["--rows", "5", "--expect-packaged"])
    i = cmd.index(ENTRY)
    assert cmd[i + 1 :] == ["hello_delta", "--rows", "5", "--expect-packaged"]


def test_command_always_pins_driver_host_cores_and_executor_memory(submit: ModuleType) -> None:
    cmd = _build(submit, "hello_delta", [])
    confs = _pairs(cmd, "--conf")
    assert "spark.driver.host=spark-master" in confs
    assert "spark.cores.max=1" in confs
    assert "spark.executor.memory=512m" in confs


def test_command_carries_the_submit_marker_as_a_spark_conf(submit: ModuleType) -> None:
    cmd = _build(submit, "hello_delta", [], marker="unique-marker-77")
    assert "spark.lakehouse.submit_marker=unique-marker-77" in _pairs(cmd, "--conf")


def test_command_has_no_env_passthrough_or_secret_material(submit: ModuleType) -> None:
    """Negative: no -e / --env to docker exec, no credential-looking tokens."""
    cmd = _build(submit, "hello_delta", [])
    assert "-e" not in cmd and "--env" not in cmd
    assert not [t for t in cmd if any(w in t.lower() for w in ("secret", "password", "token"))]


# ---- parse_worker_capacity -------------------------------------------------------------------


def _master_json(workers: list[dict[str, object]]) -> str:
    return json.dumps({"url": "spark://spark-master:7077", "workers": workers})


def test_capacity_sums_free_cores_and_memory_of_alive_workers(submit: ModuleType) -> None:
    payload = _master_json(
        [
            {"state": "ALIVE", "coresfree": 2, "memoryfree": 1024},
            {"state": "ALIVE", "coresfree": 1, "memoryfree": 512},
        ]
    )
    assert tuple(submit.parse_worker_capacity(payload)) == (3, 1536)


def test_capacity_ignores_workers_that_are_not_alive(submit: ModuleType) -> None:
    """Negative: a DEAD worker's free resources must not count as capacity."""
    payload = _master_json(
        [
            {"state": "DEAD", "coresfree": 8, "memoryfree": 8192},
            {"state": "ALIVE", "coresfree": 1, "memoryfree": 512},
        ]
    )
    assert tuple(submit.parse_worker_capacity(payload)) == (1, 512)


def test_capacity_without_workers_is_zero(submit: ModuleType) -> None:
    assert tuple(submit.parse_worker_capacity(_master_json([]))) == (0, 0)


# ---- find_duplicate_drivers ------------------------------------------------------------------


def _ps(*lines: str) -> str:
    return "  PID COMMAND\n" + "\n".join(lines) + "\n"


def test_duplicate_driver_with_identical_job_and_args_is_found(submit: ModuleType) -> None:
    ps = _ps(
        f"  101 python3 {ENTRY} bronze_stream --namespace ns1 --cdc-topic c",
        "  102 /bin/sleep 5",
    )
    args = ["--namespace", "ns1", "--cdc-topic", "c"]
    assert submit.find_duplicate_drivers(ps, "bronze_stream", args) == [101]


def test_driver_and_spark_submit_processes_of_the_same_run_are_all_reported(
    submit: ModuleType,
) -> None:
    ps = _ps(
        f"  201 java -cp x org.apache.spark.deploy.SparkSubmit {ENTRY} bronze_stream --namespace n",
        f"  202 python3 {ENTRY} bronze_stream --namespace n",
    )
    found = submit.find_duplicate_drivers(ps, "bronze_stream", ["--namespace", "n"])
    assert sorted(found) == [201, 202]


@pytest.mark.parametrize(
    "line",
    [
        f"  301 python3 {ENTRY} bronze_stream --namespace ns2",  # different namespace
        f"  302 python3 {ENTRY} bronze_stream --namespace ns10",  # value merely starts the same
        f"  303 python3 {ENTRY} bronze_stream --namespace ns1 --extra x",  # more args
        f"  304 python3 {ENTRY} bronze_stream",  # fewer args
        f"  305 python3 {ENTRY} bronze_report --namespace ns1",  # other job
        f"  306 python3 {ENTRY} bronze_stream_fault --namespace ns1",  # job name with same prefix
        f"  307 grep bronze_stream --namespace ns1 {ENTRY}",  # same text, other command
        '  308 pgrep -f "job_entry.py bronze_stream --namespace ns1"',  # glued into one token
    ],
)
def test_non_identical_commands_are_not_duplicates(submit: ModuleType, line: str) -> None:
    """Negative: only an exact job + args token sequence counts as the same driver."""
    assert submit.find_duplicate_drivers(_ps(line), "bronze_stream", ["--namespace", "ns1"]) == []


def test_no_driver_running_yields_empty_list(submit: ModuleType) -> None:
    assert submit.find_duplicate_drivers(_ps("  1 /sbin/init"), "bronze_stream", []) == []
