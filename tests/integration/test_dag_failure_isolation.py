"""W4.D23.1 - one brand fails, the other brands complete (runs ``airflow dags test`` in the container).

Precondition: the stack runs with the ``core`` and ``orchestration`` profiles and the landing zone
holds complete orders and products batches for all three tenants (``python -m lakehouse.generator``
landing commands). The test skips with a clear reason when the Airflow scheduler container is not
running. It injects the failure with the ``fail_brand`` DAG param and reads the task states back
from the metadata database. The mapped ``landing_check`` instances follow the order of the
``brands`` param, so ``map_index`` equals the position in ``DEFAULT_BRANDS``.
"""

from __future__ import annotations

import pytest

from lakehouse.orchestration.plan import DEFAULT_BRANDS
from tests.integration import airflow_cli


pytestmark = pytest.mark.integration

LOGICAL_DATE = "2026-01-05"
FAILED_BRAND = "brand_beta"


@pytest.fixture(scope="module")
def states() -> airflow_cli.RunStates:
    airflow_cli.require_airflow()
    airflow_cli.dags_test(LOGICAL_DATE, {"fail_brand": FAILED_BRAND})
    return airflow_cli.latest_run_states()


def test_only_the_injected_brand_fails_its_landing_check(states: airflow_cli.RunStates) -> None:
    landing = states.mapped_states("landing_check")
    expected = {
        DEFAULT_BRANDS.index(b): ("failed" if b == FAILED_BRAND else "success")
        for b in DEFAULT_BRANDS
    }
    assert landing == expected, f"landing_check states: {landing}"


def test_healthy_brands_are_collected_and_the_run_ends_red(states: airflow_cli.RunStates) -> None:
    assert states.state_of("summarize_landing") == "success"  # all_done: runs after the failure
    assert states.state_of("run_verdict") == "failed"  # one_failed: a degraded run is not green
    assert states.state == "failed"


def test_shared_chain_was_not_blocked_by_the_failed_brand(states: airflow_cli.RunStates) -> None:
    """The first shared job ran: it is not upstream_failed or skipped."""
    assert states.state_of("bronze_stream") in {"success", "failed"}
