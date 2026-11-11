"""Kafka -> Bronze restart, replay, checkpoint-loss and retention scenarios (W2.D9.1).

Requires the core stack (``docker compose --profile core up -d``) plus the Spark profile; skips
when the broker is unreachable. Expectations come from the produced messages and the exact
topic watermarks, never from job output. Faults are injected by ``bronze_stream_fault``.

Delivery vocabulary: every scenario asserts that each produced offset is present in Bronze
exactly once after a restart ("offset committed once per checkpoint lineage"), not exactly-once
delivery in general.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import urllib.request
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.clickstream import generate_clickstream
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.messages import OutboundMessage, cdc_messages
from lakehouse.generator.producer import create_producer, produce_messages

from .spark_jobs import (
    exact_offsets,
    kill_job_processes,
    log_events,
    metrics,
    run_ok,
    submit,
    submit_streaming,
)


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


pytestmark = pytest.mark.integration

BRANDS = 3
CLICK_EVENTS = 400
CDC_EVENTS = 200
MAX_OFFSETS = 20
MIN_BATCHES = 5
SLOW_MAX_OFFSETS = 5
SKIP = "--skip-build"
CDC_TABLE = "raw_customers"
CLICK_TABLE = "raw_clickstream"
TABLES = (CDC_TABLE, CLICK_TABLE)
KAFKA_CONTAINER = "crm_kafka"
FILER_URL = "http://127.0.0.1:9001"  # local only, bound to 127.0.0.1 (LOCAL_VS_CLOUD #1)
FIRST_BATCH_WAIT_S = 420.0
BROKER_DOWN_WAIT_S = 180.0
NO_DATA_LOSS_JOB_TIMEOUT_S = 300
HARD_TIMEOUT_EXIT = 124
INJECTED_MESSAGE = f"injected fault after write: {CDC_TABLE} batch 1"  # bronze_stream_fault.py

Watermarks = dict[int, tuple[int, int]]


# ---- environment ----------------------------------------------------------------------------


@dataclass
class Env:
    """Topics, namespace and the messages that were (or will be) produced for one test."""

    servers: str
    namespace: str
    cdc_topic: str
    click_topic: str
    cdc_msgs: list[OutboundMessage]
    click_msgs: list[OutboundMessage]

    def topic(self, table: str) -> str:
        return self.cdc_topic if table == CDC_TABLE else self.click_topic

    def messages(self, table: str) -> list[OutboundMessage]:
        return self.cdc_msgs if table == CDC_TABLE else self.click_msgs

    def produce(self, cdc: Sequence[OutboundMessage], click: Sequence[OutboundMessage]) -> None:
        for batch in (cdc, click):
            if not batch:
                continue
            stats = produce_messages(list(batch), producer=create_producer(self.servers))
            assert stats.ok and stats.failed == 0 and stats.delivered == len(batch), stats

    def produce_all(self) -> None:
        self.produce(self.cdc_msgs, self.click_msgs)

    def watermarks(self, table: str) -> Watermarks:
        return exact_offsets(self.servers, self.topic(table))

    def job_args(
        self,
        job: str = "bronze_stream",
        *,
        max_offsets: int = MAX_OFFSETS,
        skip_build: bool = True,
        extra: Sequence[str] = (),
        namespace: str | None = None,
    ) -> list[str]:
        return [
            *([SKIP] if skip_build else []),
            job,
            "--namespace",
            namespace or self.namespace,
            "--cdc-topic",
            self.cdc_topic,
            "--clickstream-topic",
            self.click_topic,
            "--max-offsets-per-trigger",
            str(max_offsets),
            *extra,
        ]

    def fault_args(self, fault: str, table: str, batch: int, **kwargs: Any) -> list[str]:
        extra = ["--fault", fault, "--fault-table", table, "--fault-batch", str(batch)]
        return self.job_args("bronze_stream_fault", extra=extra, **kwargs)

    def report(self) -> dict[str, Any]:
        out = run_ok([SKIP, "bronze_report", "--namespace", self.namespace])
        return dict(metrics(out)["tables"])

    def assert_each_offset_once(self) -> None:
        report = self.report()
        for table in TABLES:
            assert_each_offset_once(
                report[table], self.watermarks(table), len(self.messages(table)), label=table
            )


@pytest.fixture
def env(bootstrap_servers: str, make_topics: Callable[..., list[str]]) -> Env:
    cdc_topic, click_topic = make_topics("rst_cdc", "rst_click")
    config = GeneratorConfig(_env_file=None)  # type: ignore[call-arg]
    click = generate_clickstream(config, brands=BRANDS, events=CLICK_EVENTS, topic=click_topic)
    cdc = cdc_messages(generate_cdc(config, brands=BRANDS, events=CDC_EVENTS), cdc_topic)
    cdc_msgs, click_msgs = list(cdc), list(click.messages)
    for name, msgs in ((CDC_TABLE, cdc_msgs), (CLICK_TABLE, click_msgs)):
        assert len(msgs) // MAX_OFFSETS >= MIN_BATCHES, f"{name}: too few messages for batches"
    return Env(
        servers=bootstrap_servers,
        namespace=f"it_{uuid.uuid4().hex[:8]}",
        cdc_topic=cdc_topic,
        click_topic=click_topic,
        cdc_msgs=cdc_msgs,
        click_msgs=click_msgs,
    )


# ---- shared assertions ----------------------------------------------------------------------


def assert_each_offset_once(
    report_table: dict[str, Any], watermarks: Watermarks, produced: int, *, label: str = ""
) -> None:
    """Bronze holds every produced offset of every partition once: no gap, no duplicate."""
    assert sum(high - low for low, high in watermarks.values()) == produced, label
    assert report_table["rows"] == produced, f"{label}: rows"
    assert report_table["distinct_source_refs"] == produced, f"{label}: distinct_source_refs"
    by_partition = report_table["offsets_by_partition"]
    for partition, (low, high) in watermarks.items():
        if high == low:
            continue
        got = by_partition[str(partition)]
        assert got["min"] == low, f"{label} p{partition} min"
        assert got["max"] == high - 1, f"{label} p{partition} max"
        assert got["rows"] == high - low, f"{label} p{partition} rows"
        assert got["distinct"] == high - low, f"{label} p{partition} distinct"


def test_assertion_helper_detects_duplicates_and_gaps() -> None:
    """Negative: the shared assertion must fail on a duplicated and on a missing offset."""
    wm: Watermarks = {0: (0, 10)}
    good = {
        "rows": 10,
        "distinct_source_refs": 10,
        "offsets_by_partition": {"0": {"min": 0, "max": 9, "rows": 10, "distinct": 10}},
    }
    assert_each_offset_once(good, wm, 10)
    duplicated = {**good, "rows": 11}
    with pytest.raises(AssertionError):
        assert_each_offset_once(duplicated, wm, 10)
    gap = {
        "rows": 9,
        "distinct_source_refs": 9,
        "offsets_by_partition": {"0": {"min": 0, "max": 9, "rows": 9, "distinct": 9}},
    }
    with pytest.raises(AssertionError):
        assert_each_offset_once(gap, wm, 10)


def _succeeded(output: str) -> bool:
    return bool(log_events(output, "job.succeeded"))


def _query_ids(output: str) -> dict[str, str]:
    return {e["table"]: e["query_id"] for e in log_events(output, "bronze_stream.query_started")}


def _first_started_batch(output: str, table: str) -> int:
    started = [e for e in log_events(output, "bronze_stream.batch_started") if e["table"] == table]
    assert started, f"no batch_started for {table}; output tail:\n{output[-2000:]}"
    return int(started[0]["batch_id"])


def _crash_restart(env: Env, fault: str, table: str) -> None:
    """Shared body of S1/S2: crash at batch 2 of ``table``, restart, expect a real replay."""
    env.produce_all()
    code, out = submit(env.fault_args(fault, table, 2, skip_build=False))
    assert code != 0, f"fault run must not succeed; tail:\n{out[-2000:]}"
    assert not _succeeded(out)
    assert log_events(out, "bronze_stream_fault.crash"), "the crash fault never fired"
    first_ids = _query_ids(out)

    restart = run_ok(env.job_args())
    assert _first_started_batch(restart, table) == 2, "restart did not replay the crashed batch"
    assert _query_ids(restart)[table] == first_ids[table], "restart must reuse the query lineage"
    env.assert_each_offset_once()


# ---- scenarios ------------------------------------------------------------------------------


def test_s1_crash_after_write_replays_batch_and_keeps_offsets_unique(env: Env) -> None:
    _crash_restart(env, "crash_after_write", CDC_TABLE)


def test_s2_crash_before_write_replays_batch_and_keeps_offsets_unique(env: Env) -> None:
    _crash_restart(env, "crash_before_write", CLICK_TABLE)


def test_s3_external_sigkill_of_the_driver_then_restart_keeps_offsets_unique(env: Env) -> None:
    env.produce_all()
    job = submit_streaming(
        env.job_args(max_offsets=SLOW_MAX_OFFSETS, skip_build=False),
        timeout_s=600,
        namespace=env.namespace,
    )
    try:
        first = job.wait_for_event("bronze_stream.batch", FIRST_BATCH_WAIT_S)
        assert first is not None, f"no batch before the kill; tail:\n{job.output[-2000:]}"
        kill_job_processes(env.namespace)
        code, out = job.wait(timeout_s=300)
    finally:
        job.close()
    assert code != 0, "the job finished before the kill; the test would be vacuous"
    assert not _succeeded(out)

    run_ok(env.job_args())
    env.assert_each_offset_once()


def test_s4_exception_in_callback_fails_job_then_restart_keeps_offsets_unique(env: Env) -> None:
    env.produce_all()
    code, out = submit(env.fault_args("raise_in_callback", CDC_TABLE, 1, skip_build=False))
    assert code == 1, f"expected exit 1; tail:\n{out[-2000:]}"
    failed = log_events(out, "job.failed")
    assert failed, "no job.failed event"
    # The injected text itself (not the job name, which also contains "fault") must be the cause.
    assert INJECTED_MESSAGE in json.dumps(failed) or INJECTED_MESSAGE in out, out[-2000:]
    assert not _succeeded(out)
    started = [
        int(e["batch_id"])
        for e in log_events(out, "bronze_stream.batch_started")
        if e["table"] == CDC_TABLE
    ]
    assert 1 in started, f"the faulted CDC batch never started; started={started}"

    restart = run_ok(env.job_args())
    # The batch whose callback raised had no commit-log entry, so the restart plans it again.
    assert _first_started_batch(restart, CDC_TABLE) == 1, "restart did not replay batch 1"
    env.assert_each_offset_once()


def test_s5_checkpoint_loss_measurement_of_reread_amount(env: Env) -> None:
    """Measures (does not prevent) duplication: a fresh checkpoint re-reads everything."""
    env.produce_all()
    first = run_ok(env.job_args(skip_build=False))
    first_ids = _query_ids(first)
    env.assert_each_offset_once()

    second = run_ok(env.job_args(extra=["--checkpoint-namespace", f"fresh_{uuid.uuid4().hex[:6]}"]))
    second_ids = _query_ids(second)
    report = env.report()
    for table in TABLES:
        produced = len(env.messages(table))
        marks = env.watermarks(table)
        assert second_ids[table] != first_ids[table], f"{table}: lineage must be new"
        row = report[table]
        assert row["rows"] == 2 * produced, table
        assert row["distinct_source_refs"] == produced, table
        assert row["distinct_lineages"] == 2, table
        for partition, (low, high) in marks.items():
            if high == low:
                continue
            got = row["offsets_by_partition"][str(partition)]
            assert got["rows"] == 2 * (high - low), f"{table} p{partition}"
            assert got["distinct"] == high - low, f"{table} p{partition}"


def _delete_records_before(topic: str, partition: int, offset: int) -> None:
    payload = json.dumps(
        {"partitions": [{"topic": topic, "partition": partition, "offset": offset}], "version": 1}
    )
    script = (
        "cat > /tmp/delete_records.json && /opt/kafka/bin/kafka-delete-records.sh "
        "--bootstrap-server kafka:9092 --offset-json-file /tmp/delete_records.json"
    )
    result = subprocess.run(
        ["docker", "exec", "-i", KAFKA_CONTAINER, "sh", "-c", script],
        input=payload,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, (result.stdout + result.stderr)[-1500:]


def test_s6_retention_gap_fails_loudly_and_leaves_bronze_unchanged(env: Env) -> None:
    half_cdc, half_click = len(env.cdc_msgs) // 2, len(env.click_msgs) // 2
    env.produce(env.cdc_msgs[:half_cdc], env.click_msgs[:half_click])
    run_ok(env.job_args(skip_build=False))
    before_marks = {t: env.watermarks(t) for t in TABLES}
    before = env.report()

    env.produce(env.cdc_msgs[half_cdc:], env.click_msgs[half_click:])
    for table in TABLES:
        after = env.watermarks(table)
        # A partition that got >= 2 new records: delete everything below its 2nd new record,
        # i.e. past the checkpointed offset, so the next unread offset no longer exists.
        partition = max(after, key=lambda p: after[p][1] - before_marks[table][p][1])
        old_high, new_high = before_marks[table][partition][1], after[partition][1]
        assert new_high - old_high >= 2, f"{table}: no partition received enough new data"
        _delete_records_before(env.topic(table), partition, old_high + 1)
        assert env.watermarks(table)[partition][0] == old_high + 1

    code, out = submit(env.job_args(), timeout_s=NO_DATA_LOSS_JOB_TIMEOUT_S)
    assert code != 0, f"job must fail on data loss; tail:\n{out[-2000:]}"
    assert code != HARD_TIMEOUT_EXIT, "job hung instead of failing"
    assert not _succeeded(out)
    causes = [ln[:300] for ln in out.splitlines() if re.search(r"exception|caused by", ln, re.I)]
    assert re.search(r"failondataloss|data loss", out, re.IGNORECASE), "\n".join(causes[:30])

    unchanged = env.report()
    for table in TABLES:
        assert unchanged[table]["rows"] == before[table]["rows"], table
        assert unchanged[table]["distinct_source_refs"] == before[table]["distinct_source_refs"]


def test_s8_empty_topics_create_empty_tables_then_data_arrives_with_unique_offsets(
    env: Env,
) -> None:
    # Measured: a first run on empty topics plans one zero-row batch, so Delta creates the table
    # with its schema and zero rows. A second empty run plans nothing.
    queries = metrics(run_ok(env.job_args(skip_build=False)))["queries"]
    for table in TABLES:
        assert queries[table]["rows_seen"] == 0, table
        assert queries[table]["batches"] == 1, table
    assert all(r["rows"] == 0 for r in env.report().values())

    second = metrics(run_ok(env.job_args()))["queries"]
    for table in TABLES:
        assert second[table]["batches"] == 0, table

    env.produce_all()
    run_ok(env.job_args())
    env.assert_each_offset_once()


def _delete_checkpoint_logs(namespace: str, table: str) -> None:
    """Partial loss: keep the checkpoint ``metadata`` (query id), lose offsets and commits."""
    base = f"{FILER_URL}/buckets/lakehouse/checkpoints/bronze_stream/{namespace}/{table}"
    for name in ("offsets", "commits"):
        request = urllib.request.Request(f"{base}/{name}?recursive=true", method="DELETE")
        with urllib.request.urlopen(request, timeout=30) as response:
            assert response.status < 300, f"{table}/{name}: {response.status}"


def test_s9_partial_checkpoint_loss_is_refused_instead_of_skipping_new_data(env: Env) -> None:
    half_cdc, half_click = len(env.cdc_msgs) // 2, len(env.click_msgs) // 2
    env.produce(env.cdc_msgs[:half_cdc], env.click_msgs[:half_click])
    run_ok(env.job_args(skip_build=False))
    before = env.report()
    env.produce(env.cdc_msgs[half_cdc:], env.click_msgs[half_click:])
    for table in TABLES:
        _delete_checkpoint_logs(env.namespace, table)

    # One large batch: batch 0 now holds old and new rows, and Delta already recorded version 0
    # for this query id, so without a guard the whole batch would be skipped silently.
    code, out = submit(env.job_args(max_offsets=100000), timeout_s=NO_DATA_LOSS_JOB_TIMEOUT_S)
    after = env.report()
    print(f"S9_OUTCOME: exit={code} before={_rows(before)} after={_rows(after)}")
    assert code not in (0, HARD_TIMEOUT_EXIT), "partial checkpoint loss must fail loudly"
    assert not _succeeded(out)
    assert "rewound" in out.lower()
    assert _rows(after) == _rows(before)


def _rows(report: dict[str, Any]) -> dict[str, int]:
    return {table: int(report[table]["rows"]) for table in TABLES}


def test_s7_broker_outage_then_restart_keeps_offsets_unique(env: Env) -> None:
    if os.environ.get("RUN_BROKER_CHAOS") != "1":
        pytest.skip("broker chaos is opt-in: set RUN_BROKER_CHAOS=1")
    env.produce_all()
    job = submit_streaming(
        env.job_args(max_offsets=SLOW_MAX_OFFSETS, skip_build=False), 300, namespace=env.namespace
    )
    started = time.monotonic()
    code: int | None = None
    try:
        first = job.wait_for_event("bronze_stream.batch", FIRST_BATCH_WAIT_S)
        assert first is not None, f"no batch before the outage; tail: {job.output[-2000:]}"
        started = time.monotonic()
        subprocess.run(["docker", "stop", KAFKA_CONTAINER], timeout=120, check=True)
        code = job.wait_until_done(BROKER_DOWN_WAIT_S)
    finally:
        subprocess.run(["docker", "start", KAFKA_CONTAINER], timeout=120, check=False)
        _wait_for_broker(env.servers)
        if code is None:
            # Still running: let the wrapper's own --timeout-seconds end it (exit 124), so no
            # driver is left behind to block the rerun.
            code, _out = job.wait()
        job.close()
    print(f"S7_OUTCOME: exit={code} duration={time.monotonic() - started:.0f}")

    run_ok(env.job_args())
    env.assert_each_offset_once()


def _wait_for_broker(servers: str, timeout_s: float = 120.0) -> None:
    from confluent_kafka.admin import AdminClient

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            AdminClient({"bootstrap.servers": servers}).list_topics(timeout=5)
            return
        except Exception:  # broker still starting
            time.sleep(2)
    pytest.fail(f"broker {servers} did not come back within {timeout_s:.0f}s")
