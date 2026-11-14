"""W4.D23.2 / W4.D23.3 - a DLQ-rate breach halts the downstream ``dbt_build`` and alerts per brand.

Runs ``airflow dags test`` once with ``max_dlq_rate = 0``: every brand with pending DLQ rows
(all three in the shared data: the generator injects contract defects per brand) breaches. The
gate is per brand and mapped; ``dbt_build`` must not run on breached data, the run must end red,
and each breach must emit one ``kind = threshold`` alert that names its brand. The rate is
cumulative over the tables (pending DLQ rows / parseable Bronze rows, D-099), not per batch.

Needs the ``core``, ``sql`` and ``orchestration`` profiles; skips otherwise. About 25 minutes
(the whole chain runs before the gate).
"""

from __future__ import annotations

from typing import Any

import pytest

from lakehouse.orchestration.plan import DEFAULT_BRANDS
from tests.integration import airflow_cli


pytestmark = pytest.mark.integration

LOGICAL_DATE = "2026-01-08"


@pytest.fixture(scope="module")
def run() -> tuple[airflow_cli.RunStates, list[dict[str, Any]]]:
    airflow_cli.require_airflow()
    airflow_cli.require_thrift()
    proc = airflow_cli.dags_test(LOGICAL_DATE, {"max_dlq_rate": 0})
    return airflow_cli.latest_run_states(), airflow_cli.alert_payloads(proc)


def test_the_chain_ran_before_the_gate(
    run: tuple[airflow_cli.RunStates, list[dict[str, Any]]],
) -> None:
    """Not vacuous: the gate judged freshly reconciled data."""
    assert run[0].state_of("reconcile") == "success", run[0].tasks


def test_every_breached_brand_fails_its_gate_and_dbt_does_not_run(
    run: tuple[airflow_cli.RunStates, list[dict[str, Any]]],
) -> None:
    states = run[0]
    gates = states.mapped_states("dlq_rate_gate")
    assert gates == dict.fromkeys(range(len(DEFAULT_BRANDS)), "failed"), gates
    assert states.state_of("dbt_build") == "upstream_failed"
    assert states.state_of("run_verdict") == "failed"
    assert states.state == "failed"


def test_each_breach_emits_one_threshold_alert_with_its_brand(
    run: tuple[airflow_cli.RunStates, list[dict[str, Any]]],
) -> None:
    gate_alerts = [p for p in run[1] if p.get("task_id") == "dlq_rate_gate"]
    assert sorted(p["brand_id"] for p in gate_alerts) == sorted(DEFAULT_BRANDS), gate_alerts
    assert {p["kind"] for p in gate_alerts} == {"threshold"}
    assert all(p["reason"].startswith("dlq_rate_breach brand_id=") for p in gate_alerts)
