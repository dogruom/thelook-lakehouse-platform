"""Kafka -> Bronze end to end (W2.D8.3) against the core stack.

Requires ``docker compose --profile core up -d``; skips cleanly when the broker is unreachable.
Expectations come from the generated messages and the generator manifest, never from job output.
"""

from __future__ import annotations

import json
import subprocess
import sys
import uuid
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.clickstream import generate_clickstream
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.messages import OutboundMessage, cdc_messages
from lakehouse.generator.producer import DeliveryStats, create_producer, produce_messages


if TYPE_CHECKING:
    from collections.abc import Callable


pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
SUBMIT = REPO_ROOT / "scripts" / "submit_job.py"
SUBMIT_TIMEOUT_S = 600
BRANDS = 3
CLICK_EVENTS = 400
CDC_EVENTS = 200
MAX_OFFSETS = 100
NULL_BRAND = "__null__"


def _handcrafted(topic: str, tombstone_brand: str, bad_utf8_brand: str) -> list[OutboundMessage]:
    def make(value: bytes | None, brand: str, key: str) -> OutboundMessage:
        return OutboundMessage(
            topic=topic,
            key=f"{brand}:{key}",
            value=value,  # type: ignore[arg-type]  # a tombstone has no value
            headers=(("brand_id", brand.encode("utf-8")),),
            brand_id=brand,
        )

    return [
        make(None, tombstone_brand, "tombstone"),
        make(b"\xff\xfeabc", bad_utf8_brand, "badutf8"),
    ]


def _produce(servers: str, messages: list[OutboundMessage]) -> DeliveryStats:
    stats = produce_messages(messages, producer=create_producer(servers))
    assert stats.ok, stats
    assert stats.failed == 0 and stats.delivered == len(messages)
    return stats


def _run(args: list[str]) -> str:
    result = subprocess.run(
        [sys.executable, str(SUBMIT), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=SUBMIT_TIMEOUT_S,
        check=False,
    )
    output = result.stdout + "\n" + result.stderr
    assert result.returncode == 0, f"exit {result.returncode}; output tail:\n{output[-3000:]}"
    return output


def _metrics(output: str) -> dict[str, Any]:
    found: dict[str, Any] | None = None
    for line in output.splitlines():
        start = line.find("{")
        if start < 0:
            continue
        try:
            record = json.loads(line[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get("event") == "job.succeeded":
            found = record["metrics"]
    assert found is not None, f"no job.succeeded line; output tail:\n{output[-3000:]}"
    return found


def _nonzero(counts: dict[str, int]) -> dict[str, int]:
    return {k: v for k, v in counts.items() if v}


def test_bronze_rows_equal_produced_messages_including_parse_errors(
    bootstrap_servers: str, make_topics: Callable[..., list[str]]
) -> None:
    cdc_topic, click_topic = make_topics("bronze_cdc", "bronze_click")
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    config = GeneratorConfig(_env_file=None)  # type: ignore[call-arg]

    click_run = generate_clickstream(config, brands=BRANDS, events=CLICK_EVENTS, topic=click_topic)
    click_msgs = [
        *click_run.messages,
        *_handcrafted(click_topic, "brand_alpha", "brand_gamma"),
    ]
    cdc_msgs = [
        *cdc_messages(generate_cdc(config, brands=BRANDS, events=CDC_EVENTS), cdc_topic),
        *_handcrafted(cdc_topic, "brand_beta", "brand_alpha"),
    ]
    malformed = click_run.manifest.counts["malformed_json"]
    assert malformed > 0, "test data must contain generator-made malformed JSON"

    cdc_stats = _produce(bootstrap_servers, cdc_msgs)
    click_stats = _produce(bootstrap_servers, click_msgs)
    assert cdc_stats.by_topic == {cdc_topic: len(cdc_msgs)}
    assert click_stats.by_topic == {click_topic: len(click_msgs)}

    stream_args = [
        "bronze_stream",
        "--namespace",
        namespace,
        "--cdc-topic",
        cdc_topic,
        "--clickstream-topic",
        click_topic,
        "--max-offsets-per-trigger",
        str(MAX_OFFSETS),
    ]
    stream_out = _run(stream_args)
    queries = _metrics(stream_out)["queries"]
    report_out = _run(["--skip-build", "bronze_report", "--namespace", namespace])
    tables = _metrics(report_out)["tables"]

    expected = {
        "raw_customers": (
            cdc_msgs,
            {"null_value": 1, "invalid_utf8": 1},
        ),
        "raw_clickstream": (
            click_msgs,
            {"null_value": 1, "invalid_utf8": 1, "malformed_json": malformed},
        ),
    }
    for table, (messages, errors) in expected.items():
        report = tables[table]
        assert report["rows"] == len(messages), table
        assert report["distinct_source_refs"] == len(messages), table
        assert _nonzero(report["parse_errors"]) == errors, table
        nulls = report["null_counts"]
        for column in ("_source_ref", "_ingest_ts", "_batch_id", "_kafka_ts", "brand_id"):
            assert nulls[column] == 0, f"{table}.{column}"
        assert nulls["_raw_value"] == 1, table  # exactly the tombstone
        # Malformed rows are counted under their header brand (payload unreadable).
        by_brand = Counter(m.brand_id for m in messages)
        assert _nonzero(report["rows_by_brand"]) == dict(by_brand), table
        assert NULL_BRAND not in report["rows_by_brand"], table
        assert queries[table]["rows_seen"] == len(messages), table
        assert queries[table]["parse_errors"] == sum(errors.values()), table
        assert queries[table]["batches"] > 1, f"{table}: small trigger must force several batches"
        assert report["distinct_batch_ids"] > 1, table

    assert queries["raw_customers"]["checkpoint"] != queries["raw_clickstream"]["checkpoint"]

    # Job-level rerun: same namespace and topics, no new data -> nothing read, nothing added.
    rerun_queries = _metrics(_run(["--skip-build", *stream_args]))["queries"]
    rerun_tables = _metrics(_run(["--skip-build", "bronze_report", "--namespace", namespace]))[
        "tables"
    ]
    for table, (messages, _errors) in expected.items():
        assert rerun_queries[table]["rows_seen"] == 0, table
        assert rerun_queries[table]["batches"] == 0, table
        assert rerun_queries[table]["query_id"] == queries[table]["query_id"], table
        assert rerun_tables[table]["rows"] == len(messages), table
        assert rerun_tables[table]["distinct_source_refs"] == len(messages), table
