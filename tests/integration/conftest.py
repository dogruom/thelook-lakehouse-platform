"""Integration fixtures: broker reachability gate and throw-away topics (W1.D5)."""

from __future__ import annotations

import contextlib
import os
import subprocess
import uuid
import warnings
from typing import TYPE_CHECKING

import pytest

from lakehouse.generator.config import GeneratorConfig


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


REACHABILITY_TIMEOUT_S = 5.0


@pytest.fixture(scope="session")
def bootstrap_servers() -> str:
    """Broker address; skips (never fails) the test when the broker is unreachable."""
    from confluent_kafka.admin import AdminClient

    servers = GeneratorConfig(_env_file=None).kafka_bootstrap_servers  # type: ignore[call-arg]
    try:
        AdminClient({"bootstrap.servers": servers}).list_topics(timeout=REACHABILITY_TIMEOUT_S)
    except Exception as exc:  # any connection failure means "no stack"
        pytest.skip(f"Kafka broker {servers} unreachable within {REACHABILITY_TIMEOUT_S}s: {exc}")
    return servers


#: Every namespaced test run writes under ``<layer>/it_<hex8>/...`` (and the stream checkpoint under
#: ``checkpoints/bronze_stream/it_<hex8>/...``). The aws CLI filters use fnmatch, where ``*`` also
#: matches ``/``; the glob therefore names the exact ``it_<hex8>`` directory shape, so a table or file
#: that merely starts with ``it_`` is never matched.
IT_NAMESPACE_GLOB = "*/it_" + "[0-9a-f]" * 8 + "/*"
_IT_PURGE_SCRIPT = (
    'aws --endpoint-url "http://$S3_HOST:$S3_PORT" --no-cli-pager --only-show-errors '
    's3 rm "s3://$LAKEHOUSE_BUCKET/" --recursive --exclude "*" '
    f'--include "{IT_NAMESPACE_GLOB}"'
)


def purge_it_namespaces() -> subprocess.CompletedProcess[str]:
    """Delete every ``it_*`` namespace from the lakehouse bucket (test data only).

    Runs in a one-off ``seaweedfs-init`` container, which already has the S3 credentials in its
    environment (same pattern as ``scripts/smoke_test.py``); nothing is printed from it.
    """
    return subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "core",
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "--entrypoint",
            "/bin/sh",
            "seaweedfs-init",
            "-c",
            _IT_PURGE_SCRIPT,
        ],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )


@pytest.fixture(scope="session", autouse=True)
def _purge_it_namespaces_after_session() -> Iterator[None]:
    """Remove the namespaced Bronze/DLQ/checkpoint data the integration tests leave behind.

    Runs once at the end of the session (not per test: tests inspect their tables until teardown).
    ``KEEP_IT_DATA=1`` keeps the data for debugging. Best effort: a failure is reported as a
    warning, never as a test failure.
    """
    yield
    if os.environ.get("KEEP_IT_DATA") == "1":
        return
    try:
        proc = purge_it_namespaces()
    except (OSError, subprocess.TimeoutExpired) as exc:
        warnings.warn(f"it_* namespace purge did not run: {exc}", stacklevel=1)
        return
    if proc.returncode != 0:
        warnings.warn(
            f"it_* namespace purge failed (exit {proc.returncode}): {proc.stderr.strip()[-300:]}",
            stacklevel=1,
        )


@pytest.fixture
def make_topics(bootstrap_servers: str) -> Iterator[Callable[..., list[str]]]:
    """Factory creating fresh topics ``it_<prefix>_<uuid8>``; all are deleted on teardown."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    created: list[str] = []

    def _make(*prefixes: str, partitions: int = 6) -> list[str]:
        names = [f"it_{prefix}_{uuid.uuid4().hex[:8]}" for prefix in prefixes]
        futures = admin.create_topics(
            [NewTopic(n, num_partitions=partitions, replication_factor=1) for n in names]
        )
        for name, future in futures.items():
            future.result(timeout=30)
            created.append(name)
        return names

    yield _make

    if created:
        for future in admin.delete_topics(created, operation_timeout=30).values():
            with contextlib.suppress(Exception):  # best-effort cleanup
                future.result(timeout=30)
