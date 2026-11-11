"""CDC correctness and PII checks of ``silver.customers`` (W3.D15.1 / W3.D15.2) against the stack.

Requires ``docker compose --profile core up -d`` (plus the Spark profile used by ``submit_job``);
skips cleanly when the broker is unreachable. Expectations come from crafted events and from the
generator manifest, never from job output. Silver is read back from S3 (copied out through the
``seaweedfs-init`` container, parsed with pyarrow) and compared with the oracle.

Crafted scenarios share one namespace and one topic. They run in three rounds (produce ->
``bronze_stream`` -> ``silver_customers``); every round is exactly one Silver micro-batch, so
"stale", "late" and "duplicate" mean *arrives in a later batch than the event it conflicts with*.
Customer ids are distinct per scenario. A snapshot of Silver is taken after each round, so
"state unchanged" is a whole-row equality (including ``_updated_at``: a NOOP must not even touch
the row).

Generator-driven test: a seeded CDC run is produced in two halves (two Silver micro-batches) and
compared with the manifest. The manifest's ``expected_outcome`` is computed with batch size 1, but
Silver decides ACCEPT/NOOP per micro-batch (DATA_CONTRACTS 4.1), so the oracle for the counters is
``classify_outcomes(..., batch_ends=[k])``; DLQ and DUPLICATE are batching-independent and are also
compared directly with ``manifest.counts``.

The HMAC key lives only in the Spark containers (``PII_HMAC_KEY`` from ``.env``): the host never
sees it, so tokens are checked structurally (format, determinism, normalisation, no plain hash);
key dependence is covered by ``tests/spark/test_silver_customers.py``.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

from lakehouse.contracts.kafka import cdc_headers, cdc_key
from lakehouse.contracts.models import CustomerCdcEvent, to_wire
from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.io import canonical_json
from lakehouse.generator.manifest import Outcome, classify_outcomes
from lakehouse.generator.messages import OutboundMessage, cdc_messages
from lakehouse.generator.producer import create_producer, produce_messages

from .delta_io import download_table, read_rows, temp_topics
from .spark_jobs import log_events, metrics, run_ok


if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from pathlib import Path

pytestmark = pytest.mark.integration

ALPHA, BETA = "brand_alpha", "brand_beta"
BRANDS = 3
GEN_EVENTS = 300
MAX_OFFSETS = 1000
MAX_FILES = 1000
HEX64 = re.compile(r"^[0-9a-f]{64}$")
Key = tuple[str, str]
Row = dict[str, Any]
COUNTER_FIELDS = (
    "rows_read",
    "reread_rows",
    "parse_excluded",
    "dlq_excluded",
    "duplicates_removed",
    "stale_cdc_noop",
    "silver_accepted",
    "late_beyond_window",
)
OUTCOME_TERM = {
    Outcome.ACCEPT: "silver_accepted",
    Outcome.DLQ: "dlq_excluded",
    Outcome.DUPLICATE: "duplicates_removed",
    Outcome.NOOP: "stale_cdc_noop",
}

_built = [False]  # the first submit builds the job zip, later ones reuse it


# ---------------------------------------------------------------------------- pure helpers


def _uuid(n: int) -> str:
    return str(uuid.UUID(int=n, version=4))


def _message(topic: str, payload: dict[str, Any]) -> OutboundMessage:
    """A message built with the real key/header builders of the contracts package."""
    return OutboundMessage(
        topic=topic,
        key=cdc_key(payload),
        value=canonical_json(payload).encode("utf-8"),
        headers=tuple(cdc_headers(payload)),
        brand_id=payload["brand_id"],
    )


def _digits_tail(phone: str | None) -> str | None:
    if phone is None:
        return None
    digits = re.sub(r"\D", "", phone)
    return digits[-4:] if len(digits) >= 4 else None


def final_mismatches(expected: Sequence[Row], silver: Sequence[Row]) -> list[str]:
    """Differences between the manifest's ``expected_final`` and the Silver rows (empty = equal)."""
    got = {(r["brand_id"], r["customer_id"]): r for r in silver}
    problems: list[str] = []
    if len(got) != len(silver):
        problems.append("Silver holds duplicate (brand_id, customer_id) keys")
    wanted = {(e["brand_id"], e["customer_id"]) for e in expected}
    problems += [f"unexpected row {k}" for k in sorted(set(got) - wanted)]
    for e in expected:
        key = (e["brand_id"], e["customer_id"])
        row = got.get(key)
        if row is None:
            problems.append(f"missing row {key}")
            continue
        checks = {
            "_cdc_sequence": e["cdc_sequence"],
            "event_id": e["event_id"],
            "_cdc_action": e["cdc_action"],
            "is_deleted": e["is_deleted"],
            "country": e["country"],
            "loyalty_tier": e["loyalty_tier"],
            "city": None if e["is_deleted"] else e["city"],
        }
        problems += [
            f"{key} {col}: expected {want!r}, got {row[col]!r}"
            for col, want in checks.items()
            if row[col] != want
        ]
    return problems


def pii_leaks(rows: Sequence[Row], needles: Sequence[str]) -> list[tuple[str, str]]:
    """``(column, needle)`` for every string cell that contains a raw value (case-insensitive)."""
    wanted = [n.lower() for n in needles if n]
    found: list[tuple[str, str]] = []
    for row in rows:
        for column, value in row.items():
            if isinstance(value, str):
                low = value.lower()
                found += [(column, n) for n in wanted if n in low]
    return found


def counter_total(per_run: Sequence[dict[str, int]]) -> dict[str, int]:
    return {f: sum(run[f] for run in per_run) for f in COUNTER_FIELDS}


def expected_counters(outcomes: Sequence[Outcome]) -> dict[str, int]:
    """Counters of one micro-batch from its per-event outcomes (no re-reads, no parse errors)."""
    tally = Counter(OUTCOME_TERM[o] for o in outcomes)
    out = dict.fromkeys(COUNTER_FIELDS, 0)
    out.update(tally)
    out["rows_read"] = len(outcomes)
    return out


# ---------------------------------------------------------------------------- stack helpers


def _produce(servers: str, messages: Sequence[OutboundMessage]) -> None:
    stats = produce_messages(list(messages), producer=create_producer(servers))
    assert stats.ok, stats
    assert stats.failed == 0 and stats.delivered == len(messages)


def _run(args: list[str]) -> str:
    """``run_ok`` that builds the job zip once per session and reuses it afterwards."""
    if _built[0]:
        args = ["--skip-build", *args]
    out = run_ok(args)
    _built[0] = True
    return out


def _ingest_round(
    servers: str,
    messages: Sequence[OutboundMessage],
    namespace: str,
    cdc_topic: str,
    click_topic: str,
) -> dict[str, Any]:
    """Produce ``messages``, drain them into Bronze, then into Silver; returns Silver metrics."""
    _produce(servers, messages)
    _run(
        [
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
    )
    out = _run(
        [
            "silver_customers",
            "--namespace",
            namespace,
            "--max-files-per-trigger",
            str(MAX_FILES),
        ]
    )
    result = metrics(out)
    batch_events = log_events(out, "silver_customers.batch")
    assert result["batches"] == len(batch_events) == 1, (
        "each round must be exactly one Silver micro-batch "
        f"(batches={result['batches']}, batch log lines={len(batch_events)})"
    )
    return dict(result)


def _snapshot(namespace: str, root: Path, tag: str) -> list[Row]:
    dest = root / tag
    download_table("silver", "customers", namespace, dest)
    return read_rows(dest)


# ---------------------------------------------------------------------------- crafted scenarios

BASE_TS = datetime(2026, 3, 1, tzinfo=UTC)
NAMES = ("s1", "s2", "s3", "s4", "s5", "s5b", "s6")
CUST = {name: _uuid(100 + i) for i, name in enumerate(NAMES)}
PHONE = {name: f"+4915{i + 2}98761{i}34" for i, name in enumerate(NAMES)}
SHARED_EMAIL_ALPHA = "Shared.Person@Example.COM"
SHARED_EMAIL_BETA = "shared.person@example.com"
_event_counter = [0]


def _ev(
    brand: str,
    name: str,
    seq: int,
    action: str,
    *,
    label: str | None = None,
    email: str | None = None,
) -> dict[str, Any]:
    """A contract-valid ``customer_cdc`` wire payload; ``city`` encodes scenario and sequence."""
    _event_counter[0] += 1
    label = label or name
    model = CustomerCdcEvent.model_validate(
        {
            "event_id": _uuid(10_000 + _event_counter[0]),
            "brand_id": brand,
            "schema_version": 1,
            "produced_at": BASE_TS + timedelta(minutes=seq, seconds=1),
            "customer_id": CUST[name],
            "first_name": f"First{name}",
            "last_name": f"Last{name}",
            "email": email or f"{name}.person@example.com",
            "phone": PHONE[name],
            "city": f"City-{label}-{seq}",
            "country": "DE",
            "loyalty_tier": "Gold",
            "signup_date": date(2025, 1, 1),
            "_cdc_action": action,
            "_cdc_sequence": seq,
            "_cdc_ts": BASE_TS + timedelta(minutes=seq),
        }
    )
    return to_wire(model)


@dataclass(frozen=True)
class Crafted:
    namespace: str
    runs: list[dict[str, Any]]
    snaps: list[dict[Key, Row]]  # Silver after round 1, 2, 3
    all_rows: list[Row]  # every row of every snapshot (for leak scans)
    emails: list[str]
    phones: list[str]

    def row(self, rnd: int, brand: str, name: str) -> Row | None:
        return self.snaps[rnd - 1].get((brand, CUST[name]))

    def need(self, rnd: int, brand: str, name: str) -> Row:
        row = self.row(rnd, brand, name)
        assert row is not None, f"no Silver row for {brand}/{name} after round {rnd}"
        return row


def _crafted_rounds() -> list[list[dict[str, Any]]]:
    u2_s4 = _ev(ALPHA, "s4", 2, "U")  # produced twice: round 2, re-sent in round 3
    round1 = [
        _ev(ALPHA, "s1", 1, "I"),
        _ev(ALPHA, "s2", 1, "I"),
        _ev(ALPHA, "s3", 1, "I"),
        _ev(ALPHA, "s4", 1, "I"),
        # multi-event batch whose max sequence is not the last emitted
        _ev(ALPHA, "s5b", 3, "U"),
        _ev(ALPHA, "s5b", 1, "I"),
        _ev(ALPHA, "s5b", 2, "U"),
        # same customer_id in two brands (same normalised email, different case)
        _ev(ALPHA, "s6", 1, "I", label="s6a", email=SHARED_EMAIL_ALPHA),
        _ev(BETA, "s6", 1, "I", label="s6b", email=SHARED_EMAIL_BETA),
    ]
    round2 = [
        _ev(ALPHA, "s1", 2, "U"),
        _ev(ALPHA, "s2", 3, "U"),
        _ev(ALPHA, "s3", 4, "D"),
        u2_s4,
        # in-order I,U,U,D of one key in one micro-batch: only the D is applied
        _ev(ALPHA, "s5", 1, "I"),
        _ev(ALPHA, "s5", 2, "U"),
        _ev(ALPHA, "s5", 3, "U"),
        _ev(ALPHA, "s5", 4, "D"),
        _ev(ALPHA, "s6", 2, "U", label="s6a", email=SHARED_EMAIL_ALPHA),
    ]
    round3 = [
        _ev(ALPHA, "s1", 3, "U"),
        _ev(ALPHA, "s2", 2, "U"),  # stale: seq 2 after seq 3
        _ev(ALPHA, "s3", 3, "U"),  # late update after the delete (seq 4)
        u2_s4,  # exact re-send
        _ev(BETA, "s6", 2, "D", label="s6b", email=SHARED_EMAIL_BETA),
    ]
    return [round1, round2, round3]


# One entry per round: rows_read, silver_accepted, stale_cdc_noop, duplicates_removed.
CRAFTED_COUNTS = (
    (9, 7, 2, 0),  # s1 s2 s3 s4 + s5b(1 of 3) + s6 x2 accepted; s5b: 2 noop
    (9, 6, 3, 0),  # s1 s2 s3 s4 + s5(1 of 4) + s6 alpha accepted; s5: 3 noop
    (5, 2, 2, 1),  # s1, s6 beta accepted; s2, s3 noop; s4 duplicate
)


@pytest.fixture(scope="module")
def crafted(bootstrap_servers: str, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Crafted]:
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    root = tmp_path_factory.mktemp("cdc_crafted")
    rounds = _crafted_rounds()
    runs: list[dict[str, Any]] = []
    snaps: list[dict[Key, Row]] = []
    all_rows: list[Row] = []
    with temp_topics(bootstrap_servers, "cdc_craft", "cdc_craft_click") as (cdc_topic, click_topic):
        for index, payloads in enumerate(rounds, start=1):
            messages = [_message(cdc_topic, p) for p in payloads]
            runs.append(
                _ingest_round(bootstrap_servers, messages, namespace, cdc_topic, click_topic)
            )
            rows = _snapshot(namespace, root, f"r{index}")
            all_rows += rows
            snaps.append({(r["brand_id"], r["customer_id"]): r for r in rows})
            assert len(snaps[-1]) == len(rows), f"duplicate keys in Silver after round {index}"
    flat = [p for rnd in rounds for p in rnd]
    yield Crafted(
        namespace=namespace,
        runs=runs,
        snaps=snaps,
        all_rows=all_rows,
        emails=sorted({p["email"] for p in flat}),
        phones=sorted({p["phone"] for p in flat}),
    )


def test_round_counters_match_the_crafted_events_exactly(crafted: Crafted) -> None:
    for index, (read, accepted, noop, dup) in enumerate(CRAFTED_COUNTS):
        counts = crafted.runs[index]["counts"]
        want = dict.fromkeys(COUNTER_FIELDS, 0)
        want.update(
            rows_read=read, silver_accepted=accepted, stale_cdc_noop=noop, duplicates_removed=dup
        )
        assert counts == want, f"round {index + 1}"
        assert counts["rows_read"] == accepted + noop + dup  # the reconciliation equation


def test_in_order_events_each_replace_the_state(crafted: Crafted) -> None:
    for rnd, seq in ((1, 1), (2, 2), (3, 3)):
        row = crafted.need(rnd, ALPHA, "s1")
        assert row["_cdc_sequence"] == seq
        assert row["city"] == f"City-s1-{seq}"
        assert row["is_deleted"] is False
    final = crafted.need(3, ALPHA, "s1")
    assert final["_cdc_action"] == "U"
    assert final["email_token"] is not None


def test_stale_update_is_ignored(crafted: Crafted) -> None:
    after_u3 = crafted.need(2, ALPHA, "s2")
    assert (after_u3["_cdc_sequence"], after_u3["city"]) == (3, "City-s2-3")
    after_stale = crafted.need(3, ALPHA, "s2")  # seq 2 arrived after seq 3
    assert after_stale == after_u3, "a stale update must not touch the row at all"
    assert after_stale["city"] != "City-s2-2"


def test_late_update_after_delete_keeps_the_tombstone(crafted: Crafted) -> None:
    tombstone = crafted.need(2, ALPHA, "s3")
    assert tombstone["is_deleted"] is True and tombstone["_cdc_sequence"] == 4
    after_late = crafted.need(3, ALPHA, "s3")  # U seq 3 arrived after D seq 4
    assert after_late == tombstone, "a late update must not resurrect or modify the tombstone"
    for column in ("first_name", "last_name", "email_token", "phone_masked", "city"):
        assert after_late[column] is None, f"tombstone must not carry {column}"
    assert after_late["_cdc_action"] == "D"


def test_duplicate_event_is_counted_and_leaves_state_unchanged(crafted: Crafted) -> None:
    assert crafted.runs[2]["counts"]["duplicates_removed"] == 1
    assert crafted.runs[2]["counts"]["stale_cdc_noop"] == 2  # the duplicate is not also a noop
    before = crafted.need(2, ALPHA, "s4")
    assert before["_cdc_sequence"] == 2
    assert crafted.need(3, ALPHA, "s4") == before


def test_multi_event_batch_applies_only_the_maximum_sequence(crafted: Crafted) -> None:
    assert crafted.row(1, ALPHA, "s5") is None  # first seen in round 2
    row = crafted.need(2, ALPHA, "s5")
    assert (row["_cdc_sequence"], row["_cdc_action"], row["is_deleted"]) == (4, "D", True)
    assert row["city"] is None and row["email_token"] is None
    assert crafted.need(3, ALPHA, "s5") == row

    # Max sequence emitted first, lower ones after it: still only the max is applied.
    first = crafted.need(1, ALPHA, "s5b")
    assert (first["_cdc_sequence"], first["_cdc_action"]) == (3, "U")
    assert first["city"] == "City-s5b-3"
    assert first["is_deleted"] is False


def test_same_customer_id_in_two_brands_stays_two_independent_rows(crafted: Crafted) -> None:
    assert CUST["s6"] in {k[1] for k in crafted.snaps[0]}
    alpha1, beta1 = crafted.need(1, ALPHA, "s6"), crafted.need(1, BETA, "s6")
    assert (alpha1["city"], beta1["city"]) == ("City-s6a-1", "City-s6b-1")
    assert alpha1["brand_id"] == ALPHA and beta1["brand_id"] == BETA

    alpha2, beta2 = crafted.need(2, ALPHA, "s6"), crafted.need(2, BETA, "s6")
    assert (alpha2["_cdc_sequence"], alpha2["city"]) == (2, "City-s6a-2")
    assert beta2 == beta1, "an alpha update must not change the beta row"

    alpha3, beta3 = crafted.need(3, ALPHA, "s6"), crafted.need(3, BETA, "s6")
    assert beta3["is_deleted"] is True and beta3["_cdc_sequence"] == 2
    assert alpha3 == alpha2, "a beta delete must not touch the alpha row"
    assert alpha3["is_deleted"] is False and alpha3["email_token"] is not None
    for snap in crafted.snaps:
        same_id = [r for r in snap.values() if r["customer_id"] == CUST["s6"]]
        assert len(same_id) == 2


def test_silver_has_no_rows_besides_the_crafted_customers(crafted: Crafted) -> None:
    want = {(ALPHA, CUST[n]) for n in NAMES} | {(BETA, CUST["s6"])}
    assert set(crafted.snaps[2]) == want
    assert {r["brand_id"] for r in crafted.snaps[2].values()} == {ALPHA, BETA}


# ---- personal data checks, crafted events ----


def test_silver_has_no_raw_email_or_phone_column_or_value(crafted: Crafted) -> None:
    columns = set(crafted.all_rows[0])
    assert not columns & {"email", "phone"}
    assert {"email_token", "phone_masked"} <= columns
    assert pii_leaks(crafted.all_rows, [*crafted.emails, *crafted.phones]) == []


def test_email_token_is_64_hex_and_null_only_on_tombstones(crafted: Crafted) -> None:
    for row in crafted.snaps[2].values():
        if row["is_deleted"]:
            assert row["email_token"] is None and row["phone_masked"] is None
        else:
            assert HEX64.match(row["email_token"]), row["customer_id"]


def test_equal_normalised_emails_share_a_token_across_brands_and_others_differ(
    crafted: Crafted,
) -> None:
    alpha, beta = crafted.need(1, ALPHA, "s6"), crafted.need(1, BETA, "s6")
    assert SHARED_EMAIL_ALPHA != SHARED_EMAIL_BETA
    assert alpha["email_token"] == beta["email_token"]  # lower(trim()) normalisation
    live = [r["email_token"] for r in crafted.snaps[0].values()]
    assert len(set(live)) == len(live) - 1  # all distinct except the shared-email pair
    # deterministic across batches: the alpha update re-derives the same token
    assert crafted.need(2, ALPHA, "s6")["email_token"] == alpha["email_token"]


def test_email_token_is_not_a_plain_hash(crafted: Crafted) -> None:
    token = crafted.need(1, ALPHA, "s1")["email_token"]
    plain = hashlib.sha256(b"s1.person@example.com").hexdigest()
    assert token != plain, "a keyless SHA-256 is a lookup table, not a pseudonym"


def test_phone_masked_is_the_last_four_digits_of_the_produced_phone(crafted: Crafted) -> None:
    for name in ("s1", "s2", "s4", "s5b"):
        row = crafted.need(1, ALPHA, name)
        assert row["phone_masked"] == _digits_tail(PHONE[name])
        assert len(row["phone_masked"]) == 4


# ---------------------------------------------------------------------------- negative tests


def _good_final() -> tuple[list[Row], list[Row]]:
    expected = [
        {
            "brand_id": ALPHA,
            "customer_id": "c1",
            "cdc_sequence": 3,
            "event_id": "e3",
            "cdc_action": "U",
            "is_deleted": False,
            "city": "Berlin",
            "country": "DE",
            "loyalty_tier": "Gold",
        },
        {
            "brand_id": BETA,
            "customer_id": "c1",
            "cdc_sequence": 4,
            "event_id": "e4",
            "cdc_action": "D",
            "is_deleted": True,
            "city": "Paris",
            "country": "FR",
            "loyalty_tier": "Silver",
        },
    ]
    silver = [
        {
            "brand_id": ALPHA,
            "customer_id": "c1",
            "_cdc_sequence": 3,
            "event_id": "e3",
            "_cdc_action": "U",
            "is_deleted": False,
            "city": "Berlin",
            "country": "DE",
            "loyalty_tier": "Gold",
        },
        {
            "brand_id": BETA,
            "customer_id": "c1",
            "_cdc_sequence": 4,
            "event_id": "e4",
            "_cdc_action": "D",
            "is_deleted": True,
            "city": None,
            "country": "FR",
            "loyalty_tier": "Silver",
        },
    ]
    return expected, silver


def test_final_state_comparison_detects_each_kind_of_violation() -> None:
    expected, silver = _good_final()
    assert final_mismatches(expected, silver) == []

    stale = [{**silver[0], "_cdc_sequence": 2}, silver[1]]
    assert any("_cdc_sequence" in p for p in final_mismatches(expected, stale))
    resurrected = [silver[0], {**silver[1], "is_deleted": False}]
    assert any("is_deleted" in p for p in final_mismatches(expected, resurrected))
    leaked_city = [silver[0], {**silver[1], "city": "Paris"}]
    assert any("city" in p for p in final_mismatches(expected, leaked_city))
    assert any("missing row" in p for p in final_mismatches(expected, silver[:1]))
    cross_brand = [*silver, {**silver[0], "brand_id": "brand_gamma"}]
    assert any("unexpected row" in p for p in final_mismatches(expected, cross_brand))
    duplicated = [*silver, dict(silver[0])]
    assert any("duplicate" in p for p in final_mismatches(expected, duplicated))


def test_pii_scan_detects_a_raw_value_in_any_string_column() -> None:
    clean = [{"brand_id": ALPHA, "email_token": "a" * 64, "city": "Berlin", "n": 1}]
    assert pii_leaks(clean, ["ada@example.com", "+491512345678"]) == []
    leaky = [{**clean[0], "city": "mail ADA@example.com now"}]
    assert pii_leaks(leaky, ["ada@example.com"]) == [("city", "ada@example.com")]
    phone = [{**clean[0], "email_token": "+491512345678"}]
    assert pii_leaks(phone, ["+491512345678"]) == [("email_token", "+491512345678")]


def test_counter_oracle_detects_a_wrong_counter() -> None:
    outcomes = [Outcome.ACCEPT, Outcome.NOOP, Outcome.NOOP, Outcome.DUPLICATE, Outcome.DLQ]
    want = expected_counters(outcomes)
    assert want["rows_read"] == 5 and want["stale_cdc_noop"] == 2
    tampered = {**want, "stale_cdc_noop": want["stale_cdc_noop"] - 1}
    assert tampered != want


# ---------------------------------------------------------------------------- generator-driven


@dataclass(frozen=True)
class Generated:
    run: Any  # CdcRun
    split: int
    runs: list[dict[str, Any]]
    silver: list[Row]


@pytest.fixture(scope="module")
def generated(
    bootstrap_servers: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Generated]:
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    # 1 % of ~40 customers rounds to 0: raise the rate so cross-tenant collisions are present
    config = GeneratorConfig(
        _env_file=None,  # type: ignore[call-arg]
        cdc_cross_tenant_collision_rate=5.0,
    )
    run = generate_cdc(config, brands=BRANDS, events=GEN_EVENTS)
    root = tmp_path_factory.mktemp("cdc_generated")
    with temp_topics(bootstrap_servers, "cdc_gen", "cdc_gen_click") as (cdc_topic, click_topic):
        messages = cdc_messages(run, cdc_topic)
        split = len(messages) // 2
        runs = [
            _ingest_round(bootstrap_servers, part, namespace, cdc_topic, click_topic)
            for part in (messages[:split], messages[split:])
        ]
    yield Generated(run, split, runs, _snapshot(namespace, root, "final"))


def test_generated_run_exercises_the_defects_under_test(generated: Generated) -> None:
    counts = generated.run.manifest.counts
    for name in ("duplicate", "out_of_order", "invalid_email", "cross_tenant_collision"):
        assert counts[name] > 0, f"seeded run has no {name}: the test would be vacuous"
    assert counts["dlq"] > 0 and counts["duplicates_removed"] > 0 and counts["stale_cdc_noop"] > 0


def test_silver_final_rows_equal_manifest_expected_final(generated: Generated) -> None:
    expected = generated.run.manifest.expected_final
    assert expected, "manifest has no expected final rows"
    assert final_mismatches(expected, generated.silver) == []
    assert len(generated.silver) == len(expected)


def test_silver_counters_equal_manifest_ground_truth(generated: Generated) -> None:
    manifest = generated.run.manifest
    entries = manifest.entries
    valid = [e.expected_outcome is not Outcome.DLQ for e in entries]
    oracle = classify_outcomes(entries, valid, batch_ends=[generated.split])
    halves = (oracle[: generated.split], oracle[generated.split :])
    for index, (run, outcomes) in enumerate(zip(generated.runs, halves, strict=True), start=1):
        assert run["counts"] == expected_counters(outcomes), f"micro-batch {index}"

    total = counter_total([r["counts"] for r in generated.runs])
    assert len(generated.runs) == 2 and sum(r["batches"] for r in generated.runs) == 2
    # Batching-independent terms equal the manifest exactly.
    assert total["rows_read"] == manifest.counts["bronze"] == manifest.counts["emitted"]
    assert total["dlq_excluded"] == manifest.counts["dlq"]
    assert total["duplicates_removed"] == manifest.counts["duplicates_removed"]
    assert total["late_beyond_window"] == manifest.counts["late_beyond_window"] == 0
    assert total["parse_excluded"] == 0 and total["reread_rows"] == 0
    # ACCEPT / NOOP depend on micro-batch boundaries; their sum does not.
    assert total["silver_accepted"] + total["stale_cdc_noop"] == (
        manifest.counts["silver_accepted"] + manifest.counts["stale_cdc_noop"]
    )
    assert total["rows_read"] == (
        total["silver_accepted"]
        + total["dlq_excluded"]
        + total["duplicates_removed"]
        + total["stale_cdc_noop"]
        + total["late_beyond_window"]
    )


def test_generated_silver_keeps_brands_separate_for_colliding_customer_ids(
    generated: Generated,
) -> None:
    by_id: dict[str, set[str]] = {}
    for row in generated.silver:
        by_id.setdefault(row["customer_id"], set()).add(row["brand_id"])
    assert any(len(brands) > 1 for brands in by_id.values()), "no cross-brand collision survived"
    expected_keys = {
        (e["brand_id"], e["customer_id"]) for e in generated.run.manifest.expected_final
    }
    assert {(r["brand_id"], r["customer_id"]) for r in generated.silver} == expected_keys


# ---- personal data checks, generated run ----


def _payloads_by_event(generated: Generated) -> dict[str, dict[str, Any]]:
    return {line["value"]["event_id"]: line["value"] for line in generated.run.lines}


def test_generated_silver_contains_no_raw_email_or_phone(generated: Generated) -> None:
    payloads = [line["value"] for line in generated.run.lines]
    emails = {p["email"] for p in payloads if isinstance(p.get("email"), str) and "@" in p["email"]}
    phones = {p["phone"] for p in payloads if p.get("phone")}
    assert emails and phones
    assert pii_leaks(generated.silver, [*emails, *phones]) == []
    assert not set(generated.silver[0]) & {"email", "phone"}


def test_generated_tokens_are_hex_consistent_per_normalised_email_and_distinct_otherwise(
    generated: Generated,
) -> None:
    payloads = _payloads_by_event(generated)
    token_by_email: dict[str, set[str]] = {}
    for row in generated.silver:
        if row["is_deleted"]:
            assert row["email_token"] is None
            continue
        assert HEX64.match(row["email_token"]), row["customer_id"]
        email = payloads[row["event_id"]]["email"].strip().lower()
        token_by_email.setdefault(email, set()).add(row["email_token"])
    assert all(len(tokens) == 1 for tokens in token_by_email.values())
    all_tokens = [next(iter(t)) for t in token_by_email.values()]
    assert len(set(all_tokens)) == len(all_tokens), "different emails must not share a token"


def test_generated_phone_masked_is_last_four_digits_of_the_winning_event(
    generated: Generated,
) -> None:
    payloads = _payloads_by_event(generated)
    checked = 0
    for row in generated.silver:
        if row["is_deleted"]:
            assert row["phone_masked"] is None
            continue
        want = _digits_tail(payloads[row["event_id"]]["phone"])
        assert row["phone_masked"] == want, row["customer_id"]
        checked += want is not None
    assert checked > 0, "no live customer with a phone: the test would be vacuous"
