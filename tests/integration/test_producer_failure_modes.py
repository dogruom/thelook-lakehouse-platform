"""Producer failure modes against a real librdkafka client (W1 week-gate follow-up).

No broker is needed (and none must exist) at the address used here: the test proves that an
unreachable cluster neither hangs the producer nor yields a silent success.
"""

from __future__ import annotations

import time

import pytest

from lakehouse.generator.clickstream import generate_clickstream
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.producer import create_producer, produce_messages, producer_config


pytestmark = pytest.mark.integration

UNREACHABLE = "127.0.0.1:1"  # nothing listens on port 1
MESSAGE_COUNT = 20
MARGIN_S = 15.0  # scheduling slack on top of delivery.timeout.ms


def test_unreachable_broker_fails_every_message_within_the_delivery_timeout() -> None:
    config = GeneratorConfig(_env_file=None, seed=42)  # type: ignore[call-arg]
    messages = generate_clickstream(config, brands=3, events=100).messages[:MESSAGE_COUNT]
    assert len(messages) == MESSAGE_COUNT
    delivery_timeout_s = producer_config(UNREACHABLE)["delivery.timeout.ms"] / 1000

    started = time.monotonic()
    stats = produce_messages(messages, producer=create_producer(UNREACHABLE))
    elapsed = time.monotonic() - started
    print(f"outage run: elapsed={elapsed:.1f}s stats={stats}")  # shown with -s / on failure

    assert elapsed < delivery_timeout_s + MARGIN_S, f"producer hung for {elapsed:.1f}s"
    assert stats.ok is False
    assert stats.delivered == 0
    assert stats.attempted == MESSAGE_COUNT
    assert stats.failed + stats.undelivered == stats.attempted
    # flush (65 s) outlasts delivery.timeout.ms (60 s), so librdkafka reports each message
    # through its callback instead of leaving it in the queue.
    assert stats.failed == stats.attempted
    assert stats.undelivered == 0
    assert stats.by_brand == {} and stats.by_topic == {}
