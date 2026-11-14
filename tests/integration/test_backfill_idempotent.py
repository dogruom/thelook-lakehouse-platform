"""W4.D23.4 - running the DAG again for the same past logical date gives an idempotent outcome.

Runs ``airflow dags test`` twice for one past date inside the Airflow container and compares the
``content_hash`` job output of the Silver tables after each run: hashes and row counts must be
identical. The jobs are idempotent by construction (``availableNow`` checkpoints, deterministic
``MERGE``); this is the evidence for "idempotent outcome" at orchestration level - not
exactly-once. ``_updated_at`` (a wall clock) is the default exclusion of ``content_hash`` (D-084).

Needs the ``orchestration`` and ``sql`` profiles (the DAG ends with ``dbt build`` on the Thrift
Server); the test skips with a clear reason when either container is not running. Memory: stop the
api-server and dag-processor if the Docker VM is short (``dags test`` runs in the scheduler
container only).

The test is not vacuous: both runs must end ``success`` with the shared chain executed (not
skipped), and the second run must be a real second execution - a different ``run_id`` or a later
``end_date`` than the first - otherwise equal hashes would prove nothing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.integration import airflow_cli, spark_jobs


if TYPE_CHECKING:
    import subprocess


pytestmark = pytest.mark.integration

PAST_DATE = "2026-01-03"
SILVER_TABLES = (
    "customers",
    "customers_history",
    "orders",
    "order_items",
    "clickstream",
    "products",
)
EXECUTED_TASKS = ("bronze_stream", "dlq_gate", "silver_orders", "reconcile", "dbt_build")


def _silver_content() -> tuple[dict[str, str], dict[str, int]]:
    args = ["content_hash"]
    for table in SILVER_TABLES:
        args += ["--table", table]
    metrics = spark_jobs.metrics(spark_jobs.run_ok(args))
    return dict(metrics["hashes"]), dict(metrics["rows"])


def _assert_full_success(
    run: airflow_cli.RunStates, label: str, proc: subprocess.CompletedProcess[str]
) -> None:
    tail = airflow_cli.output_tail(proc)
    assert run.state == "success", f"{label}: run {run.state}, tasks {run.tasks}\n{tail}"
    for task_id in EXECUTED_TASKS:
        state = run.state_of(task_id)
        assert state == "success", f"{label}: {task_id} is {state}\n{tail}"


def test_second_run_of_the_same_interval_changes_no_silver_table() -> None:
    airflow_cli.require_airflow()
    airflow_cli.require_thrift()

    first_proc = airflow_cli.dags_test(PAST_DATE)
    first_run = airflow_cli.latest_run_states()
    _assert_full_success(first_run, "first run", first_proc)
    hashes_1, rows_1 = _silver_content()

    second_proc = airflow_cli.dags_test(PAST_DATE)
    second_run = airflow_cli.latest_run_states()
    _assert_full_success(second_run, "second run", second_proc)
    assert (second_run.run_id, second_run.end_date) != (
        first_run.run_id,
        first_run.end_date,
    ), "the second `airflow dags test` did not execute a new run; the comparison would be vacuous"
    hashes_2, rows_2 = _silver_content()

    assert set(hashes_1) == set(SILVER_TABLES)
    assert rows_2 == rows_1
    assert hashes_2 == hashes_1
