"""Helpers to run the DAG in the Airflow scheduler container and read the run's task states.

Needs the ``orchestration`` profile (``crm_airflow_scheduler`` running); every test using it calls
:func:`require_airflow`, which skips with a clear reason otherwise. ``airflow dags test`` runs the
whole DAG in one process inside the container (the real Spark jobs included); the states are read
back from the metadata database with a short Python snippet, so no CLI table has to be parsed.

Not exercised on a running Airflow yet: the first run needs the image build (owner approval).
"""

from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest


if TYPE_CHECKING:
    from collections.abc import Mapping

CONTAINER = "crm_airflow_scheduler"
DAG_ID = "lakehouse_pipeline"
STATES_MARKER = "RUN_STATES "
DAG_TEST_TIMEOUT_S = 3600
SKIP_REASON = (
    f"Airflow container {CONTAINER} is not running; start it with "
    "'docker compose --profile core --profile orchestration up -d' (image build needs owner approval)"
)
#: Runs inside the container: prints ``RUN_STATES {json}`` for the newest run of the DAG.
STATES_SCRIPT = f"""
import json
from airflow.models import DagRun
from airflow.utils.session import create_session
with create_session() as session:
    run = (
        session.query(DagRun)
        .filter(DagRun.dag_id == {DAG_ID!r})
        .order_by(DagRun.id.desc())
        .first()
    )
    out = None
    if run is not None:
        tis = run.get_task_instances(session=session)
        out = {{
            "run_id": run.run_id,
            "state": str(run.state),
            "end_date": str(run.end_date),
            "tasks": [
                {{"task_id": t.task_id, "map_index": t.map_index, "state": str(t.state)}}
                for t in tis
            ],
        }}
    print({STATES_MARKER!r} + json.dumps(out))
"""


@dataclass(frozen=True)
class RunStates:
    """Newest DAG run: its state and every task instance state."""

    run_id: str
    state: str
    tasks: tuple[tuple[str, int, str], ...]  # (task_id, map_index, state); map_index -1 = unmapped
    end_date: str = ""  # when the run finished; changes when the same run is executed again

    def state_of(self, task_id: str) -> str | None:
        """State of an unmapped task, or ``None`` when it has no instance."""
        for tid, idx, state in self.tasks:
            if tid == task_id and idx == -1:
                return state
        return None

    def mapped_states(self, task_id: str) -> dict[int, str]:
        """``{map_index: state}`` of a mapped task."""
        return {idx: state for tid, idx, state in self.tasks if tid == task_id and idx >= 0}


def parse_run_states(output: str) -> RunStates:
    """Parse the ``RUN_STATES {json}`` line of :data:`STATES_SCRIPT` (``ValueError`` if absent)."""
    for line in reversed(output.splitlines()):
        if line.startswith(STATES_MARKER):
            data = json.loads(line[len(STATES_MARKER) :])
            if data is None:
                raise ValueError("the DAG has no run in the metadata database")
            return RunStates(
                data["run_id"],
                _state(data["state"]),
                tuple(
                    (t["task_id"], int(t["map_index"]), _state(t["state"])) for t in data["tasks"]
                ),
                str(data.get("end_date", "")),
            )
    raise ValueError("no RUN_STATES line in the output")


def _state(raw: Any) -> str:
    """``'TaskInstanceState.SUCCESS'`` / ``'success'`` / ``None`` -> ``'success'`` / ``'none'``."""
    return str(raw).rsplit(".", 1)[-1].lower()


def container_running(container: str = CONTAINER) -> bool:
    try:
        proc = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Running}}", container],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0 and proc.stdout.strip() == "true"


def require_airflow() -> None:
    """Skip the calling test (never fail) when the Airflow scheduler container is not running."""
    if not container_running():
        pytest.skip(SKIP_REASON)


THRIFT_CONTAINER = "crm_spark_thrift"
THRIFT_SKIP_REASON = (
    f"the Spark Thrift Server container {THRIFT_CONTAINER} is not running; the DAG ends with "
    "`dbt build`, which needs it: docker compose --profile core --profile sql up -d"
)


def require_thrift() -> None:
    """Skip the calling test when the ``sql`` profile (Thrift Server for dbt) is not running."""
    if not container_running(THRIFT_CONTAINER):
        pytest.skip(THRIFT_SKIP_REASON)


def _exec(args: list[str], timeout_s: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "exec", CONTAINER, *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout_s,
        check=False,
    )


def dags_test_command(logical_date: str, conf: Mapping[str, Any] | None = None) -> list[str]:
    """The ``airflow dags test`` argument list (inside the container)."""
    cmd = ["airflow", "dags", "test", DAG_ID, logical_date]
    if conf:
        cmd += ["--conf", json.dumps(dict(conf))]
    return cmd


def dags_test(
    logical_date: str, conf: Mapping[str, Any] | None = None, timeout_s: float = DAG_TEST_TIMEOUT_S
) -> subprocess.CompletedProcess[str]:
    """Run the whole DAG once. The exit code is NOT asserted: a failed run is a valid outcome."""
    return _exec(dags_test_command(logical_date, conf), timeout_s)


NON_TERMINAL_STATES = frozenset({"queued", "running"})
TERMINAL_WAIT_S = 180
POLL_S = 5


def latest_run_states(wait_terminal_s: float = TERMINAL_WAIT_S) -> RunStates:
    """Newest run of the DAG, read from the Airflow metadata database.

    ``airflow dags test`` returned once before the run's final state was visible (2026-11-14:
    run ``running``, ``run_verdict`` ``queued``), so a non-terminal run is polled for up to
    ``wait_terminal_s`` seconds; after that the last observed state is returned as it is.
    """
    deadline = time.monotonic() + wait_terminal_s
    while True:
        proc = _exec(["python", "-c", STATES_SCRIPT], 120)
        assert proc.returncode == 0, f"state query failed:\n{proc.stderr[-1500:]}"
        run = parse_run_states(proc.stdout)
        if run.state not in NON_TERMINAL_STATES or time.monotonic() >= deadline:
            return run
        time.sleep(POLL_S)


def output_tail(proc: subprocess.CompletedProcess[str], chars: int = 3000) -> str:
    """Last ``chars`` of a ``dags test`` run's combined output, for assertion messages."""
    return f"{proc.stdout or ''}\n{proc.stderr or ''}"[-chars:]
