"""``scripts/submit_job.py`` guards against the real cluster (exit codes 1, 3, 4, 124).

The unit tests cover the parsing with fakes; only a real container shows that a timed-out driver
is really gone, because killing the host-side ``docker exec`` client leaves it running.
"""

from __future__ import annotations

import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.messages import cdc_messages
from lakehouse.generator.producer import create_producer, produce_messages
from tests.integration.spark_jobs import REPO_ROOT, SUBMIT, submit, submit_streaming


if TYPE_CHECKING:
    from collections.abc import Callable


pytestmark = pytest.mark.integration

MASTER = "crm_spark_master"
WORKER = "crm_spark_worker"
EXIT_NO_CAPACITY = 3
EXIT_DUPLICATE = 4
EXIT_TIMEOUT = 124
WORKER_WAIT_S = 90
DRIVER_GONE_WAIT_S = 30


def _drivers_of(namespace: str) -> list[str]:
    result = subprocess.run(
        ["docker", "exec", MASTER, "ps", "-eo", "pid,args"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in result.stdout.splitlines() if namespace in line]


def _submit_once(*args: str) -> subprocess.CompletedProcess[str]:
    # Not spark_jobs.submit: that helper retries exit 3 on purpose.
    return subprocess.run(
        [sys.executable, str(SUBMIT), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
        check=False,
    )


def _slow_job_args(
    servers: str, make_topics: Callable[..., list[str]], namespace: str
) -> list[str]:
    """A job reading one message per batch, so that it outlives the short timeouts below."""
    cdc_topic, click_topic = make_topics("guard_cdc", "guard_click")
    config = GeneratorConfig(_env_file=None)  # type: ignore[call-arg]
    messages = cdc_messages(generate_cdc(config, brands=3, events=200), cdc_topic)
    assert produce_messages(messages, producer=create_producer(servers)).ok
    return [
        "bronze_stream",
        "--namespace",
        namespace,
        "--cdc-topic",
        cdc_topic,
        "--clickstream-topic",
        click_topic,
        "--max-offsets-per-trigger",
        "1",
    ]


def test_timeout_kills_the_driver_inside_the_container(
    bootstrap_servers: str, make_topics: Callable[..., list[str]]
) -> None:
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    args = _slow_job_args(bootstrap_servers, make_topics, namespace)
    code, output = submit(args, timeout_s=45)
    assert code == EXIT_TIMEOUT, output[-2000:]
    deadline = time.monotonic() + DRIVER_GONE_WAIT_S
    while (left := _drivers_of(namespace)) and time.monotonic() < deadline:
        time.sleep(2)
    assert not left, f"driver survived the timeout: {left}"


def test_second_identical_driver_is_refused_while_the_first_runs(
    bootstrap_servers: str, make_topics: Callable[..., list[str]]
) -> None:
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    args = _slow_job_args(bootstrap_servers, make_topics, namespace)
    first = submit_streaming(args, timeout_s=90, namespace=namespace)
    try:
        assert first.wait_for_event("bronze_stream.batch_started", 80) is not None, first.output
        second = _submit_once("--skip-build", *args)
        assert second.returncode == EXIT_DUPLICATE, second.stderr[-1500:]
    finally:
        code, _ = first.wait()  # its own timeout ends it and kills the driver
    assert code == EXIT_TIMEOUT
    assert not _drivers_of(namespace)


def test_no_worker_capacity_fails_at_once_instead_of_waiting_forever(
    bootstrap_servers: str,
) -> None:
    # Build first: with the worker stopped the pre-flight refuses, so a build inside the loop
    # below would otherwise happen (or not) depending on timing.
    assert _submit_once("--timeout-seconds", "120", "hello_delta").returncode == 0
    subprocess.run(["docker", "stop", WORKER], check=True, capture_output=True)
    try:
        deadline = time.monotonic() + WORKER_WAIT_S
        code = -1
        while time.monotonic() < deadline:
            # The master notices a stopped worker only after its heartbeat timeout or disconnect.
            code = _submit_once("--skip-build", "--timeout-seconds", "60", "hello_delta").returncode
            if code == EXIT_NO_CAPACITY:
                break
            time.sleep(3)
        assert code == EXIT_NO_CAPACITY
    finally:
        subprocess.run(["docker", "start", WORKER], check=True, capture_output=True)
    deadline = time.monotonic() + WORKER_WAIT_S
    while _submit_once("--skip-build", "--timeout-seconds", "120", "hello_delta").returncode != 0:
        assert time.monotonic() < deadline, "worker did not come back"
        time.sleep(5)


def test_skip_build_refuses_code_that_differs_from_the_built_package(
    bootstrap_servers: str,
) -> None:
    assert _submit_once("--timeout-seconds", "120", "hello_delta").returncode == 0  # builds
    extra = Path(REPO_ROOT) / "src" / "lakehouse" / f"_guard_{uuid.uuid4().hex[:8]}.py"
    extra.write_text('"""Temporary: changes the source hash."""\n', encoding="utf-8")
    try:
        result = _submit_once("--skip-build", "--timeout-seconds", "60", "hello_delta")
    finally:
        extra.unlink()
    assert result.returncode == 1, result.stderr[-1500:]
    assert _submit_once("--skip-build", "--timeout-seconds", "120", "hello_delta").returncode == 0
