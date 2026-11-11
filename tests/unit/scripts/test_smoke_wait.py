"""``smoke_test.wait_for_stack``: bounded readiness wait before the strict checks (W2.D12)."""

from __future__ import annotations

from typing import Any

import pytest

from .conftest import load_script


@pytest.fixture(scope="module")
def smoke() -> Any:
    return load_script("smoke_test")


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_ready_immediately_waits_zero_seconds(smoke: Any) -> None:
    clock = FakeClock()
    waited = smoke.wait_for_stack(10, 2, is_ready=lambda: True, clock=clock, sleep=clock.sleep)
    assert waited == 0
    assert clock.sleeps == []


def test_becomes_ready_after_polls(smoke: Any) -> None:
    clock = FakeClock()
    answers = iter([False, False, True])
    waited = smoke.wait_for_stack(
        10, 2, is_ready=lambda: next(answers), clock=clock, sleep=clock.sleep
    )
    assert waited == 4
    assert clock.sleeps == [2, 2]


def test_timeout_returns_none_and_stops_polling(smoke: Any) -> None:
    clock = FakeClock()
    calls: list[int] = []

    def never() -> bool:
        calls.append(1)
        return False

    waited = smoke.wait_for_stack(6, 2, is_ready=never, clock=clock, sleep=clock.sleep)
    assert waited is None
    assert clock.now == 6
    assert len(calls) == 4  # t = 0, 2, 4, 6


def test_stack_ready_requires_health_and_finished_init_jobs(
    smoke: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = {name: "healthy" for name in smoke.CORE_CONTAINERS}
    state.update({name: "exited 0" for name in smoke.INIT_CONTAINERS})
    monkeypatch.setattr(smoke, "_inspect", lambda name, _template: state[name])
    assert smoke.stack_ready() is True

    state["crm_spark_worker"] = "starting"
    assert smoke.stack_ready() is False
    state["crm_spark_worker"] = "healthy"

    state["crm_kafka_init"] = "running 0"
    assert smoke.stack_ready() is False
    state["crm_kafka_init"] = "exited 1"
    assert smoke.stack_ready() is False
