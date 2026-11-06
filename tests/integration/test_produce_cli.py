"""``python -m lakehouse.generator produce`` end to end against the local Kafka stack.

Requires ``docker compose --profile core up -d``; skips cleanly when the broker is unreachable.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition


if TYPE_CHECKING:
    from collections.abc import Callable


pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
PARTITIONS = 6
CLI_TIMEOUT_S = 180
CONSUME_TIMEOUT_S = 60.0


def _count_topic(servers: str, topic: str, expected: int) -> tuple[int, int]:
    """Return (messages consumed, total records on the topic by watermarks)."""
    consumer = Consumer(
        {
            "bootstrap.servers": servers,
            "group.id": f"it-cli-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in range(PARTITIONS)])
        consumed = 0
        deadline = time.monotonic() + CONSUME_TIMEOUT_S
        while consumed < expected and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            assert msg.error() is None, msg.error()
            consumed += 1
        on_topic = 0
        for p in range(PARTITIONS):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30)
            on_topic += high - low
        return consumed, on_topic
    finally:
        consumer.close()


def _emitted(stdout: str, prefix: str) -> int:
    match = re.search(rf"^\s*{re.escape(prefix)}\.emitted=(\d+)\s*$", stdout, re.MULTILINE)
    assert match, f"no '{prefix}.emitted=' line in CLI stdout:\n{stdout}"
    return int(match.group(1))


def test_produce_cli_exit_zero_and_topics_hold_exactly_the_emitted_counts(
    bootstrap_servers: str, make_topics: Callable[..., list[str]]
) -> None:
    cdc_topic, click_topic = make_topics("cli_cdc", "cli_click", partitions=PARTITIONS)
    env = {
        **os.environ,
        "GEN_KAFKA_BOOTSTRAP_SERVERS": bootstrap_servers,
        "GEN_TOPIC_CUSTOMER_CDC": cdc_topic,
        "GEN_TOPIC_CLICKSTREAM": click_topic,
    }
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "lakehouse.generator",
            "produce",
            "--brands",
            "3",
            "--events",
            "500",
            "--cdc-events",
            "200",
            "--seed",
            "7",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=CLI_TIMEOUT_S,
        check=False,
    )
    assert result.returncode == 0, f"exit {result.returncode}\nstdout:\n{result.stdout}\n" + (
        f"stderr tail:\n{result.stderr[-1500:]}"
    )
    assert "failed=0 undelivered=0" in result.stdout, result.stdout

    cdc_emitted = _emitted(result.stdout, "cdc")
    click_emitted = _emitted(result.stdout, "clickstream")
    assert cdc_emitted > 0 and click_emitted > 0
    assert f"delivered[topic={cdc_topic}]={cdc_emitted}" in result.stdout
    assert f"delivered[topic={click_topic}]={click_emitted}" in result.stdout

    assert _count_topic(bootstrap_servers, cdc_topic, cdc_emitted) == (cdc_emitted, cdc_emitted)
    assert _count_topic(bootstrap_servers, click_topic, click_emitted) == (
        click_emitted,
        click_emitted,
    )
