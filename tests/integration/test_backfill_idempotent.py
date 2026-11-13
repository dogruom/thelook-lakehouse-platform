"""W4.D23.4 - running the DAG again for the same past logical date changes nothing.

Runs ``airflow dags test`` twice for one past date inside the Airflow container (skips with a clear
reason when it is not running) and compares the ``content_hash`` job output of the Silver tables
after each run: hashes and row counts must be identical. The jobs are idempotent by construction
(``availableNow`` checkpoints, deterministic ``MERGE``); this is the evidence for the claim
"idempotent outcome" at orchestration level - not exactly-once. ``_updated_at`` (a wall clock) is
the default exclusion of ``content_hash`` (D-084).
"""

from __future__ import annotations

import pytest

from tests.integration import airflow_cli, spark_jobs


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


def _silver_content() -> tuple[dict[str, str], dict[str, int]]:
    args = ["content_hash"]
    for table in SILVER_TABLES:
        args += ["--table", table]
    metrics = spark_jobs.metrics(spark_jobs.run_ok(args))
    return dict(metrics["hashes"]), dict(metrics["rows"])


def test_second_run_of_the_same_interval_changes_no_silver_table() -> None:
    airflow_cli.require_airflow()

    airflow_cli.dags_test(PAST_DATE)
    first_run = airflow_cli.latest_run_states()
    assert first_run.state == "success", f"first run: {first_run.state} {first_run.tasks}"
    hashes_1, rows_1 = _silver_content()

    airflow_cli.dags_test(PAST_DATE)
    second_run = airflow_cli.latest_run_states()
    assert second_run.state == "success", f"second run: {second_run.state} {second_run.tasks}"
    hashes_2, rows_2 = _silver_content()

    assert set(hashes_1) == set(SILVER_TABLES)
    assert rows_2 == rows_1
    assert hashes_2 == hashes_1
