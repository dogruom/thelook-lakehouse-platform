"""Bronze -> DLQ gate reconciliation (W2.D10.3) against the core stack.

Requires ``docker compose --profile core up -d`` (skips cleanly when the broker is unreachable).
Expectations come from the generator manifests and the produced messages, never from job output:
the DLQ must hold exactly the manifest's DLQ events (no duplicates, late or out-of-order events,
no parse errors), a rerun must insert nothing, and a lost checkpoint (Bronze rows twice) must not
create a second DLQ row.
"""

from __future__ import annotations

import contextlib
import json
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.clickstream import generate_clickstream
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.messages import OutboundMessage, cdc_messages
from lakehouse.generator.producer import create_producer, produce_messages

from .spark_jobs import exact_offsets, log_events, run_ok, submit


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


pytestmark = pytest.mark.integration

BRANDS = 3
CDC_EVENTS = 300
CLICK_EVENTS = 400
MAX_OFFSETS = 100
CDC_TABLE, CLICK_TABLE = "raw_customers", "raw_clickstream"
CDC_DLQ, CLICK_DLQ = "dlq_customers", "dlq_clickstream"
SCHEMA = "SCHEMA_VIOLATION"
BUSINESS = "BUSINESS_RULE_VIOLATION"
CDC_DEFECT_CODE = {
    "invalid_email": ("email_invalid", SCHEMA),
    "null_email": ("email_null_or_wrong_type", SCHEMA),
    "future_signup": ("signup_after_produced", BUSINESS),
}
CLICK_CODE = "session_id_null_or_wrong_type"


# ---- helpers --------------------------------------------------------------------------------


def _produce(servers: str, messages: list[OutboundMessage]) -> None:
    stats = produce_messages(messages, producer=create_producer(servers))
    assert stats.ok, stats
    assert stats.failed == 0 and stats.delivered == len(messages)


def _stream_args(namespace: str, cdc_topic: str, click_topic: str, *extra: str) -> list[str]:
    return [
        "bronze_stream",
        "--namespace",
        namespace,
        "--cdc-topic",
        cdc_topic,
        "--clickstream-topic",
        click_topic,
        "--max-offsets-per-trigger",
        str(MAX_OFFSETS),
        *extra,
    ]


def _gate(namespace: str, *, build: bool = False) -> dict[str, dict[str, Any]]:
    args = ["dlq_gate", "--namespace", namespace]
    out = run_ok(args if build else ["--skip-build", *args])
    return {e["table"]: e for e in log_events(out, "dlq_gate.table")}


def _report(namespace: str) -> tuple[dict[str, dict[str, Any]], dict[str, list[list[Any]]]]:
    out = run_ok(["--skip-build", "dlq_report", "--namespace", namespace, "--list-refs"])
    tables = {e["table"]: e for e in log_events(out, "dlq_report.table")}
    refs = {e["table"]: e["refs"] for e in log_events(out, "dlq_report.refs")}
    return tables, refs


def _nonzero(counts: dict[str, int]) -> dict[str, int]:
    return {k: v for k, v in counts.items() if v}


def _kafka_events_by_ref(servers: str, topic: str) -> dict[str, tuple[str | None, bool]]:
    """``{topic/partition/offset: (event_id, parseable)}`` of every message of ``topic``."""
    from confluent_kafka import Consumer, TopicPartition

    offsets = exact_offsets(servers, topic)
    wanted = sum(high - low for low, high in offsets.values())
    consumer = Consumer(
        {
            "bootstrap.servers": servers,
            "group.id": f"it-dlq-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    found: dict[str, tuple[str | None, bool]] = {}
    try:
        consumer.assign([TopicPartition(topic, p, low) for p, (low, _high) in offsets.items()])
        deadline = time.monotonic() + 90
        while len(found) < wanted and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            ref = f"{topic}/{msg.partition()}/{msg.offset()}"
            value = msg.value()
            try:
                decoded = json.loads(value) if value is not None else None
            except ValueError:
                decoded = None
            if isinstance(decoded, dict):
                found[ref] = (decoded.get("event_id"), True)
            else:
                found[ref] = (None, False)
    finally:
        consumer.close()
    assert len(found) == wanted, f"{topic}: read {len(found)} of {wanted} messages"
    return found


@pytest.fixture(scope="module")
def module_topics(bootstrap_servers: str) -> Iterator[Callable[..., list[str]]]:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    created: list[str] = []

    def _make(*prefixes: str) -> list[str]:
        names = [f"it_{prefix}_{uuid.uuid4().hex[:8]}" for prefix in prefixes]
        futures = admin.create_topics(
            [NewTopic(n, num_partitions=6, replication_factor=1) for n in names]
        )
        for name, future in futures.items():
            future.result(timeout=30)
            created.append(name)
        return names

    yield _make
    if created:
        for future in admin.delete_topics(created, operation_timeout=30).values():
            with contextlib.suppress(Exception):
                future.result(timeout=30)


# ---- generated scenario (module scoped: Spark jobs are slow) ---------------------------------


@dataclass
class Scenario:
    namespace: str
    cdc_topic: str
    click_topic: str
    cdc_run: Any
    click_run: Any
    cdc_messages: list[OutboundMessage]
    kafka_cdc: dict[str, tuple[str | None, bool]]
    kafka_click: dict[str, tuple[str | None, bool]]
    gate1: dict[str, dict[str, Any]]
    report1: dict[str, dict[str, Any]]
    refs1: dict[str, list[list[Any]]]
    gate2: dict[str, dict[str, Any]]
    report2: dict[str, dict[str, Any]]
    refs2: dict[str, list[list[Any]]]
    gate3: dict[str, dict[str, Any]]
    report3: dict[str, dict[str, Any]]
    refs3: dict[str, list[list[Any]]]


@pytest.fixture(scope="module")
def scenario(bootstrap_servers: str, module_topics: Callable[..., list[str]]) -> Scenario:
    cdc_topic, click_topic = module_topics("dlq_cdc", "dlq_click")
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    config = GeneratorConfig(_env_file=None)  # type: ignore[call-arg]
    cdc_run = generate_cdc(config, brands=BRANDS, events=CDC_EVENTS)
    click_run = generate_clickstream(config, brands=BRANDS, events=CLICK_EVENTS, topic=click_topic)
    cdc_msgs = cdc_messages(cdc_run, cdc_topic)
    _produce(bootstrap_servers, cdc_msgs)
    _produce(bootstrap_servers, click_run.messages)

    run_ok(_stream_args(namespace, cdc_topic, click_topic))
    gate1 = _gate(namespace, build=True)
    report1, refs1 = _report(namespace)
    gate2 = _gate(namespace)  # rerun, same Bronze
    report2, refs2 = _report(namespace)
    # Checkpoint loss: a fresh checkpoint lineage re-reads both topics, Bronze holds every row twice.
    run_ok(
        [
            "--skip-build",
            *_stream_args(
                namespace, cdc_topic, click_topic, "--checkpoint-namespace", f"{namespace}_ck2"
            ),
        ]
    )
    gate3 = _gate(namespace)
    report3, refs3 = _report(namespace)
    return Scenario(
        namespace=namespace,
        cdc_topic=cdc_topic,
        click_topic=click_topic,
        cdc_run=cdc_run,
        click_run=click_run,
        cdc_messages=cdc_msgs,
        kafka_cdc=_kafka_events_by_ref(bootstrap_servers, cdc_topic),
        kafka_click=_kafka_events_by_ref(bootstrap_servers, click_topic),
        gate1=gate1,
        report1=report1,
        refs1=refs1,
        gate2=gate2,
        report2=report2,
        refs2=refs2,
        gate3=gate3,
        report3=report3,
        refs3=refs3,
    )


def _cdc_dlq_entries(s: Scenario) -> list[Any]:
    return [e for e in s.cdc_run.manifest.entries if e.expected_outcome == "DLQ"]


def _click_dlq_entries(s: Scenario) -> list[dict[str, Any]]:
    return [e for e in s.click_run.manifest.entries if e["defect"] == "null_session"]


def _non_dlq_event_ids_cdc(s: Scenario) -> set[str]:
    dlq = {e.event_id for e in _cdc_dlq_entries(s)}
    return {e.event_id for e in s.cdc_run.manifest.entries} - dlq


# ---- exact DLQ set --------------------------------------------------------------------------


def test_manifests_contain_every_defect_kind_the_test_relies_on(scenario: Scenario) -> None:
    counts = scenario.cdc_run.manifest.counts
    assert all(counts[d] > 0 for d in (*CDC_DEFECT_CODE, "duplicate", "out_of_order"))
    click = scenario.click_run.manifest.counts
    assert all(click[d] > 0 for d in ("late", "duplicate", "null_session", "malformed_json"))


def test_cdc_dlq_event_ids_equal_manifest_dlq_entries_exactly(scenario: Scenario) -> None:
    expected = Counter(e.event_id for e in _cdc_dlq_entries(scenario))
    assert sum(expected.values()) == (
        sum(scenario.cdc_run.manifest.counts[d] for d in CDC_DEFECT_CODE)
    )
    actual = Counter(ref[0] for ref in scenario.refs1[CDC_DLQ])
    assert actual == expected


def test_clickstream_dlq_event_ids_equal_manifest_null_session_entries_exactly(
    scenario: Scenario,
) -> None:
    expected = Counter(e["event_id"] for e in _click_dlq_entries(scenario))
    assert sum(expected.values()) == scenario.click_run.manifest.counts["null_session"]
    actual = Counter(ref[0] for ref in scenario.refs1[CLICK_DLQ])
    assert actual == expected


def test_no_duplicate_late_out_of_order_or_parse_error_event_is_in_the_dlq(
    scenario: Scenario,
) -> None:
    cdc_ids = {ref[0] for ref in scenario.refs1[CDC_DLQ]}
    click_ids = {ref[0] for ref in scenario.refs1[CLICK_DLQ]}
    # CDC: every duplicate / out-of-order emission shares its event_id with a valid event.
    for entry in scenario.cdc_run.manifest.entries:
        if entry.defect is not None and entry.defect.value in {"duplicate", "out_of_order"}:
            assert entry.event_id not in cdc_ids, entry
    assert not cdc_ids & _non_dlq_event_ids_cdc(scenario)
    for entry in scenario.click_run.manifest.entries:
        if entry["defect"] in {"late", "duplicate", "malformed_json"}:
            assert entry["event_id"] not in click_ids, entry


def test_dlq_refs_point_at_exactly_the_defective_kafka_messages(scenario: Scenario) -> None:
    for table, kafka, expected_ids in (
        (CDC_DLQ, scenario.kafka_cdc, {e.event_id for e in _cdc_dlq_entries(scenario)}),
        (CLICK_DLQ, scenario.kafka_click, {e["event_id"] for e in _click_dlq_entries(scenario)}),
    ):
        refs = scenario.refs1[table]
        bronze_refs = [ref[1] for ref in refs]
        assert len(set(bronze_refs)) == len(bronze_refs), f"{table}: duplicate _bronze_ref"
        for event_id, bronze_ref, _codes in refs:
            assert bronze_ref in kafka, (table, bronze_ref)
            assert kafka[bronze_ref] == (event_id, True), (table, bronze_ref)
        # Every Kafka message that is a manifest DLQ event is referenced; nothing else is.
        defective = {r for r, (eid, parsed) in kafka.items() if parsed and eid in expected_ids}
        assert set(bronze_refs) == defective, table
        unparseable = {r for r, (_eid, parsed) in kafka.items() if not parsed}
        assert not unparseable & set(bronze_refs), f"{table}: parse error row in DLQ"


def test_dlq_codes_match_manifest_defect_counts(scenario: Scenario) -> None:
    counts = scenario.cdc_run.manifest.counts
    expected_codes = {code: counts[defect] for defect, (code, _cat) in CDC_DEFECT_CODE.items()}
    for source in (scenario.gate1[CDC_TABLE]["by_code"], scenario.report1[CDC_DLQ]["by_code"]):
        assert _nonzero(source) == expected_codes
    click_expected = {CLICK_CODE: scenario.click_run.manifest.counts["null_session"]}
    for source in (
        scenario.gate1[CLICK_TABLE]["by_code"],
        scenario.report1[CLICK_DLQ]["by_code"],
    ):
        assert _nonzero(source) == click_expected
    for event_id, _ref, codes in scenario.refs1[CLICK_DLQ]:
        assert list(codes) == [CLICK_CODE], event_id
    defect_of = {e.event_id: e.defect.value for e in _cdc_dlq_entries(scenario)}
    for event_id, _ref, codes in scenario.refs1[CDC_DLQ]:
        assert list(codes) == [CDC_DEFECT_CODE[defect_of[event_id]][0]], event_id


def test_dlq_report_categories_brands_and_replay_status(scenario: Scenario) -> None:
    counts = scenario.cdc_run.manifest.counts
    cdc = scenario.report1[CDC_DLQ]
    n_cdc = len(_cdc_dlq_entries(scenario))
    assert cdc["rows"] == cdc["distinct_dlq_ids"] == n_cdc
    assert _nonzero(cdc["by_category"]) == {
        SCHEMA: counts["invalid_email"] + counts["null_email"],
        BUSINESS: counts["future_signup"],
    }
    assert _nonzero(cdc["by_brand"]) == dict(
        Counter(e.brand_id for e in _cdc_dlq_entries(scenario))
    )
    assert _nonzero(cdc["replay_status"]) == {"PENDING": n_cdc}

    click = scenario.report1[CLICK_DLQ]
    n_click = len(_click_dlq_entries(scenario))
    assert click["rows"] == click["distinct_dlq_ids"] == n_click
    assert _nonzero(click["by_category"]) == {SCHEMA: n_click}
    assert _nonzero(click["by_brand"]) == dict(
        Counter(e["brand_id"] for e in _click_dlq_entries(scenario))
    )
    assert _nonzero(click["replay_status"]) == {"PENDING": n_click}


# ---- reconciliation arithmetic --------------------------------------------------------------


def _assert_identity(event: dict[str, Any], table: str) -> None:
    assert event["rows_read"] == (
        event["parse_errors"]
        + event["valid_rows"]
        + event["rejected_rows"]
        + event["rejected_duplicates"]
    ), table


def test_gate_counts_reconcile_with_the_manifest(scenario: Scenario) -> None:
    cdc, click = scenario.gate1[CDC_TABLE], scenario.gate1[CLICK_TABLE]
    n_cdc_dlq, n_click_dlq = len(_cdc_dlq_entries(scenario)), len(_click_dlq_entries(scenario))
    malformed = scenario.click_run.manifest.counts["malformed_json"]

    assert cdc["rows_read"] == len(scenario.cdc_messages)
    assert cdc["parse_errors"] == 0
    assert cdc["rejected_rows"] == n_cdc_dlq
    # Duplicates, late and out-of-order events are valid rows: only the DLQ events leave the count.
    assert cdc["valid_rows"] == len(scenario.cdc_messages) - n_cdc_dlq

    assert click["rows_read"] == len(scenario.click_run.messages)
    assert click["parse_errors"] == malformed
    assert click["rejected_rows"] == n_click_dlq
    assert click["valid_rows"] == len(scenario.click_run.messages) - malformed - n_click_dlq

    for table, event, dlq in ((CDC_TABLE, cdc, n_cdc_dlq), (CLICK_TABLE, click, n_click_dlq)):
        assert event["rejected_duplicates"] == 0, table
        assert event["bronze_duplicate_rows"] == 0, table
        assert event["dlq_missing"] == 0, table
        assert event["dlq_rows_before"] == 0, table
        assert event["dlq_rows_after"] == event["dlq_inserted"] == event["rejected_rows"] == dlq
        _assert_identity(event, table)


# ---- idempotency ----------------------------------------------------------------------------


def test_rerun_of_the_gate_inserts_nothing_and_leaves_the_dlq_unchanged(
    scenario: Scenario,
) -> None:
    for table in (CDC_TABLE, CLICK_TABLE):
        again = scenario.gate2[table]
        first = scenario.gate1[table]
        assert again["dlq_inserted"] == 0, table
        assert again["dlq_rows_before"] == again["dlq_rows_after"] == first["dlq_rows_after"]
        assert again["dlq_missing"] == 0, table
        assert again["rows_read"] == first["rows_read"], table
        assert again["rejected_rows"] == first["rejected_rows"], table
        assert again["by_code"] == first["by_code"], table
        _assert_identity(again, table)
    for table in (CDC_DLQ, CLICK_DLQ):
        assert scenario.report2[table]["rows"] == scenario.report1[table]["rows"], table
        assert scenario.report2[table]["distinct_dlq_ids"] == scenario.report1[table]["rows"]
        assert scenario.refs2[table] == scenario.refs1[table], table


def test_checkpoint_loss_doubles_bronze_but_not_the_dlq(scenario: Scenario) -> None:
    n_cdc, n_click = len(_cdc_dlq_entries(scenario)), len(_click_dlq_entries(scenario))
    malformed = scenario.click_run.manifest.counts["malformed_json"]
    n_cdc_msgs, n_click_msgs = len(scenario.cdc_messages), len(scenario.click_run.messages)
    # (table, messages, parse errors per copy, DLQ events per copy)
    for table, total, parse, dlq in (
        (CDC_TABLE, n_cdc_msgs, 0, n_cdc),
        (CLICK_TABLE, n_click_msgs, malformed, n_click),
    ):
        event = scenario.gate3[table]
        assert event["rows_read"] == 2 * total, table
        assert event["bronze_duplicate_rows"] == total, table  # rows_read - distinct _source_ref
        assert event["parse_errors"] == 2 * parse, table
        assert event["valid_rows"] == 2 * (total - parse - dlq), table
        assert event["rejected_rows"] == dlq, table  # one copy per DLQ id
        assert event["rejected_duplicates"] == dlq, table  # the second copy
        assert event["dlq_inserted"] == 0, table
        assert event["dlq_missing"] == 0, table
        assert event["dlq_rows_before"] == event["dlq_rows_after"] == dlq, table
        assert event["by_code"] == scenario.gate1[table]["by_code"], table
        _assert_identity(event, table)
    for table in (CDC_DLQ, CLICK_DLQ):
        assert scenario.report3[table]["rows"] == scenario.report1[table]["rows"], table
        assert scenario.report3[table]["distinct_dlq_ids"] == scenario.report1[table]["rows"]
        assert scenario.refs3[table] == scenario.refs1[table], table


# ---- cross-tenant, multi-failure, empty, missing --------------------------------------------


def _cdc_payload(brand: str = "brand_alpha", **over: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "brand_id": brand,
        "schema_version": 1,
        "produced_at": "2026-01-01T10:00:00.123Z",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": "ada@example.com",
        "phone": None,
        "city": "Berlin",
        "country": "DE",
        "loyalty_tier": "Gold",
        "signup_date": "2025-12-01",
        "_cdc_action": "I",
        "_cdc_sequence": 1,
        "_cdc_ts": "2026-01-01T10:00:00.000Z",
    }
    payload.update(over)
    return payload


def _msg(
    topic: str, payload: dict[str, Any], *, key_brand: str, header_brand: str, key_field: str
) -> OutboundMessage:
    return OutboundMessage(
        topic=topic,
        key=f"{key_brand}:{payload[key_field]}",
        value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        headers=(("brand_id", header_brand.encode("utf-8")),),
        brand_id=header_brand,
    )


def test_cross_tenant_and_multi_failure_events_are_rejected_with_the_payload_brand(
    bootstrap_servers: str, make_topics: Callable[..., list[str]]
) -> None:
    cdc_topic, click_topic = make_topics("dlq_xt_cdc", "dlq_xt_click")
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    ok = _cdc_payload()
    key_mismatch = _cdc_payload()
    header_mismatch = _cdc_payload()
    multi = _cdc_payload(email="not-an-email@")
    cdc_msgs = [
        _msg(
            cdc_topic,
            ok,
            key_brand="brand_alpha",
            header_brand="brand_alpha",
            key_field="customer_id",
        ),
        _msg(
            cdc_topic,
            key_mismatch,
            key_brand="brand_beta",
            header_brand="brand_alpha",
            key_field="customer_id",
        ),
        _msg(
            cdc_topic,
            header_mismatch,
            key_brand="brand_alpha",
            header_brand="brand_beta",
            key_field="customer_id",
        ),
        _msg(
            cdc_topic,
            multi,
            key_brand="brand_alpha",
            header_brand="brand_beta",
            key_field="customer_id",
        ),
    ]
    click_ok = {
        "event_id": str(uuid.uuid4()),
        "brand_id": "brand_alpha",
        "schema_version": 1,
        "produced_at": "2026-01-01T10:00:00.500Z",
        "session_id": str(uuid.uuid4()),
        "customer_id": None,
        "event_type": "page_view",
        "page_url": "/home",
        "device_category": "mobile",
        "event_timestamp": "2026-01-01T09:59:59.000Z",
    }
    click_leak = {**click_ok, "event_id": str(uuid.uuid4()), "session_id": str(uuid.uuid4())}
    click_msgs = [
        _msg(
            click_topic,
            click_ok,
            key_brand="brand_alpha",
            header_brand="brand_alpha",
            key_field="session_id",
        ),
        _msg(
            click_topic,
            click_leak,
            key_brand="brand_beta",
            header_brand="brand_alpha",
            key_field="session_id",
        ),
    ]
    _produce(bootstrap_servers, cdc_msgs)
    _produce(bootstrap_servers, click_msgs)

    run_ok(_stream_args(namespace, cdc_topic, click_topic))
    gate = _gate(namespace)
    report, refs = _report(namespace)

    assert gate[CDC_TABLE]["rows_read"] == 4
    assert gate[CDC_TABLE]["valid_rows"] == 1
    assert gate[CDC_TABLE]["rejected_rows"] == 3
    assert gate[CDC_TABLE]["dlq_inserted"] == 3
    assert {r[0]: list(r[2]) for r in refs[CDC_DLQ]} == {
        key_mismatch["event_id"]: ["brand_key_mismatch"],
        header_mismatch["event_id"]: ["brand_header_mismatch"],
        multi["event_id"]: ["brand_header_mismatch", "email_invalid"],
    }
    assert ok["event_id"] not in {r[0] for r in refs[CDC_DLQ]}
    # One DLQ row for the multi-failure event, and the tenant is the payload brand.
    assert report[CDC_DLQ]["rows"] == report[CDC_DLQ]["distinct_dlq_ids"] == 3
    assert _nonzero(report[CDC_DLQ]["by_brand"]) == {"brand_alpha": 3}
    assert _nonzero(report[CDC_DLQ]["by_code"]) == {
        "brand_key_mismatch": 1,
        "brand_header_mismatch": 2,
        "email_invalid": 1,
    }
    assert _nonzero(report[CDC_DLQ]["by_category"]) == {SCHEMA: 3}

    assert gate[CLICK_TABLE]["rejected_rows"] == 1
    assert {r[0]: list(r[2]) for r in refs[CLICK_DLQ]} == {
        click_leak["event_id"]: ["brand_key_mismatch"]
    }
    assert _nonzero(report[CLICK_DLQ]["by_brand"]) == {"brand_alpha": 1}


def test_empty_topics_give_exit_zero_and_empty_dlq_tables(
    bootstrap_servers: str, make_topics: Callable[..., list[str]]
) -> None:
    cdc_topic, click_topic = make_topics("dlq_empty_cdc", "dlq_empty_click")
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    run_ok(_stream_args(namespace, cdc_topic, click_topic))
    gate = _gate(namespace)
    for table in (CDC_TABLE, CLICK_TABLE):
        event = gate[table]
        assert (event["rows_read"], event["parse_errors"], event["valid_rows"]) == (0, 0, 0), table
        assert (event["rejected_rows"], event["dlq_inserted"]) == (0, 0), table
    report, refs = _report(namespace)
    for table in (CDC_DLQ, CLICK_DLQ):
        assert report[table]["rows"] == 0, table
        assert report[table]["distinct_dlq_ids"] == 0, table
        assert refs[table] == [], table


def test_gate_fails_with_exit_one_when_the_bronze_namespace_does_not_exist() -> None:
    code, output = submit(
        ["--skip-build", "dlq_gate", "--namespace", f"it_missing_{uuid.uuid4().hex[:8]}"]
    )
    assert code == 1, output[-2000:]


def test_null_and_invalid_brand_and_invalid_event_id_are_kept_null_safe(
    bootstrap_servers: str, make_topics: Callable[..., list[str]]
) -> None:
    cdc_topic, click_topic = make_topics("dlq_nb_cdc", "dlq_nb_click")
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    null_brand = _cdc_payload()
    del null_brand["brand_id"]  # parseable message without a payload brand
    invalid_brand = _cdc_payload(brand="Brand_X")
    bad_event_id = _cdc_payload(event_id="not-a-uuid")
    same = {"header_brand": "brand_alpha", "key_field": "customer_id"}
    cdc_msgs = [
        _msg(cdc_topic, null_brand, key_brand="brand_alpha", **same),
        _msg(
            cdc_topic,
            invalid_brand,
            key_brand="Brand_X",
            header_brand="Brand_X",
            key_field="customer_id",
        ),
        _msg(cdc_topic, bad_event_id, key_brand="brand_alpha", **same),
    ]
    _produce(bootstrap_servers, cdc_msgs)
    run_ok(_stream_args(namespace, cdc_topic, click_topic))

    gate = _gate(namespace)[CDC_TABLE]
    assert (gate["rows_read"], gate["valid_rows"], gate["rejected_rows"]) == (3, 0, 3)
    assert gate["dlq_inserted"] == 3
    assert gate["dlq_rows_before"] == 0 and gate["dlq_rows_after"] == 3
    report, refs = _report(namespace)
    assert report[CDC_DLQ]["rows"] == report[CDC_DLQ]["distinct_dlq_ids"] == 3
    assert _nonzero(report[CDC_DLQ]["by_brand"]) == {
        "__null__": 1,
        "__invalid__": 1,
        "brand_alpha": 1,
    }
    assert {r[0]: list(r[2]) for r in refs[CDC_DLQ]} == {
        null_brand["event_id"]: ["brand_id_null_or_wrong_type"],
        invalid_brand["event_id"]: ["brand_id_invalid"],
        "__invalid__": ["event_id_invalid"],
    }
    # Null-safe MERGE: a rerun must not insert the NULL-brand row again.
    again = _gate(namespace)[CDC_TABLE]
    assert again["dlq_inserted"] == 0
    assert again["dlq_rows_before"] == again["dlq_rows_after"] == 3
    assert again["dlq_missing"] == 0
    assert _report(namespace)[0][CDC_DLQ]["rows"] == 3
