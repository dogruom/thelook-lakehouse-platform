"""Integration fixtures: broker reachability gate and throw-away topics (W1.D5)."""

from __future__ import annotations

import contextlib
import uuid
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
