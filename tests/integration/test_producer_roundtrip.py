"""Produce -> consume round trip against the local Kafka stack (W1.D5 / D5.3).

Requires ``docker compose --profile core up -d``; skips cleanly when the broker is unreachable.
"""

from __future__ import annotations

import re
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.clickstream import generate_clickstream
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.messages import OutboundMessage, cdc_messages
from lakehouse.generator.producer import create_producer, produce_messages


if TYPE_CHECKING:
    from collections.abc import Callable


pytestmark = pytest.mark.integration

PARTITIONS = 6
CONSUME_TIMEOUT_S = 60.0
KEY_RE = re.compile(r"^[a-z0-9_]+:")


@dataclass
class Received:
    key: str
    value: bytes | None
    headers: dict[str, bytes]
    partition: int


def _consume_all(
    servers: str, topic: str, expected: int
) -> tuple[list[Received], Consumer, list[TopicPartition]]:
    consumer = Consumer(
        {
            "bootstrap.servers": servers,
            "group.id": f"it-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    assignment = [TopicPartition(topic, p, OFFSET_BEGINNING) for p in range(PARTITIONS)]
    consumer.assign(assignment)
    received: list[Received] = []
    deadline = time.monotonic() + CONSUME_TIMEOUT_S
    while len(received) < expected and time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        assert msg.error() is None, msg.error()
        key = msg.key()
        received.append(
            Received(
                key=key.decode() if key is not None else "",
                value=msg.value(),
                headers=dict(msg.headers() or []),
                partition=msg.partition(),
            )
        )
    return received, consumer, assignment


def _assert_roundtrip(
    sent: list[OutboundMessage], got: list[Received], consumer: Consumer, topic: str
) -> None:
    assert len(got) == len(sent), f"consumed {len(got)} of {len(sent)} within timeout"

    # same multiset of (key, value, brand header)
    sent_view = sorted((m.key, m.value or b"", dict(m.headers)["brand_id"]) for m in sent)
    got_view = sorted((r.key, r.value or b"", r.headers.get("brand_id", b"")) for r in got)
    assert got_view == sent_view

    for r in got:
        assert KEY_RE.match(r.key), r.key
        assert {"brand_id", "schema_version", "event_type"} <= set(r.headers)
        assert r.headers["brand_id"].decode() == r.key.split(":", 1)[0]
        assert r.headers["schema_version"] == b"1"

    assert len({r.key.split(":", 1)[0] for r in got}) >= 3

    # one partition per key (ordering unit)
    partitions: dict[str, set[int]] = defaultdict(set)
    for r in got:
        partitions[r.key].add(r.partition)
    split = {k: p for k, p in partitions.items() if len(p) != 1}
    assert not split, f"keys spread over several partitions: {list(split.items())[:3]}"

    # every record consumed (committed == high watermark) and no extra records on the topic
    # (total_high == len(sent): the idempotent producer wrote no retry duplicates)
    consumer.commit(asynchronous=False)
    committed = {
        tp.partition: tp.offset
        for tp in consumer.committed(
            [TopicPartition(topic, p) for p in range(PARTITIONS)], timeout=30
        )
    }
    total_high = 0
    for p in range(PARTITIONS):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30)
        total_high += high - low
        if high == low:
            continue  # empty partition: nothing to commit
        assert committed[p] == high, f"partition {p}: committed {committed[p]} != high {high}"
    assert total_high == len(sent)


def _produce_and_consume(
    servers: str, messages: list[OutboundMessage], topic: str
) -> tuple[list[Received], Consumer]:
    stats = produce_messages(messages, producer=create_producer(servers))
    assert stats.ok, stats
    assert stats.attempted == stats.delivered == len(messages)
    got, consumer, _ = _consume_all(servers, topic, len(messages))
    return got, consumer


def test_cdc_roundtrip_keys_headers_partitions_and_zero_lag(
    bootstrap_servers: str, make_topics: Callable[..., list[str]]
) -> None:
    (topic,) = make_topics("cdc", partitions=PARTITIONS)
    config = GeneratorConfig(_env_file=None, seed=42)  # type: ignore[call-arg]
    messages = cdc_messages(generate_cdc(config, brands=3, events=1000), topic)
    assert len(messages) >= 1000
    got, consumer = _produce_and_consume(bootstrap_servers, messages, topic)
    try:
        _assert_roundtrip(messages, got, consumer, topic)
        for r in got:
            assert r.headers["event_type"] == b"customer_cdc"
    finally:
        consumer.close()


def test_clickstream_roundtrip_keys_headers_partitions_and_zero_lag(
    bootstrap_servers: str, make_topics: Callable[..., list[str]]
) -> None:
    (topic,) = make_topics("click", partitions=PARTITIONS)
    config = GeneratorConfig(_env_file=None, seed=42)  # type: ignore[call-arg]
    run = generate_clickstream(config, brands=3, events=5000, topic=topic)
    assert len(run.messages) >= 5000
    got, consumer = _produce_and_consume(bootstrap_servers, run.messages, topic)
    try:
        _assert_roundtrip(run.messages, got, consumer, topic)
        # null-session events keep the "{brand_id}:" key on a single partition per brand
        null_keys = {r.key for r in got if r.key.endswith(":")}
        assert null_keys and all(KEY_RE.match(k) for k in null_keys)
    finally:
        consumer.close()


def test_partition_check_detects_a_key_split_across_partitions() -> None:
    """Negative: the one-partition-per-key assertion logic must flag a violation."""
    partitions: dict[str, set[int]] = defaultdict(set)
    for key, part in [("brand_alpha:a", 0), ("brand_alpha:a", 3), ("brand_beta:b", 1)]:
        partitions[key].add(part)
    assert [k for k, p in partitions.items() if len(p) != 1] == ["brand_alpha:a"]
