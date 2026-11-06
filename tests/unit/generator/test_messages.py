"""cdc_messages: one OutboundMessage per CDC line, in emission order (W1.D5)."""

from __future__ import annotations

import dataclasses
import json
from typing import Any

import pytest

from lakehouse.generator.cdc import CdcRun, generate_cdc
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.messages import OutboundMessage, cdc_messages


TOPIC = "t_cdc"


def _canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


@pytest.fixture(scope="module")
def run() -> CdcRun:
    config = GeneratorConfig(_env_file=None, seed=42)  # type: ignore[call-arg]
    return generate_cdc(config, brands=3, events=300)


@pytest.fixture(scope="module")
def messages(run: CdcRun) -> list[OutboundMessage]:
    return cdc_messages(run, TOPIC)


def test_one_message_per_line_in_emission_order(
    run: CdcRun, messages: list[OutboundMessage]
) -> None:
    assert len(messages) == len(run.lines) == run.manifest.counts["emitted"]
    assert [m.key for m in messages] == [line["key"] for line in run.lines]


def test_value_is_canonical_json_of_the_line_value(
    run: CdcRun, messages: list[OutboundMessage]
) -> None:
    for message, line in zip(messages, run.lines, strict=True):
        assert isinstance(message.value, bytes)
        assert message.value == _canonical(line["value"])


def test_topic_key_and_headers_follow_the_contract(
    run: CdcRun, messages: list[OutboundMessage]
) -> None:
    for message, line in zip(messages, run.lines, strict=True):
        payload = line["value"]
        assert message.topic == TOPIC
        assert message.key == f"{payload['brand_id']}:{payload['customer_id']}"
        assert message.brand_id == payload["brand_id"]
        headers = dict(message.headers)
        assert headers == {n: v.encode() for n, v in line["headers"].items()}
        assert headers["brand_id"] == payload["brand_id"].encode()
        assert headers["schema_version"] == b"1"
        assert headers["event_type"] == b"customer_cdc"


def test_header_pairs_are_str_bytes_tuples(messages: list[OutboundMessage]) -> None:
    for message in messages:
        assert isinstance(message.headers, tuple)
        for name, value in message.headers:
            assert isinstance(name, str)
            assert isinstance(value, bytes)


def test_messages_are_immutable(messages: list[OutboundMessage]) -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        messages[0].key = "x:y"  # type: ignore[misc]


def test_three_brands_present(messages: list[OutboundMessage]) -> None:
    assert len({m.brand_id for m in messages}) == 3


def test_topic_argument_is_applied(run: CdcRun) -> None:
    assert {m.topic for m in cdc_messages(run, "another")} == {"another"}
