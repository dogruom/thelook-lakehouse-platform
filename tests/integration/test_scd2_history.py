"""SCD2 ``silver.customers_history`` against the stack (W3.D16.2). Not run by the unit suite.

Requires ``docker compose --profile core up -d`` (plus the Spark profile used by ``submit_job``);
skips cleanly when the broker is unreachable. Expectations come from crafted events and from the
generator manifest, never from job output. History is read back from S3 (copied out through the
``seaweedfs-init`` container, parsed with pyarrow).

Scenarios:
  * crafted CDC delivered in order (one round) to namespace A and the same events reversed over
    three rounds to namespace B -> identical content hash and the expected versions;
  * a seeded generator run: versions per key == valid, non-duplicate emissions per key of the
    manifest (DLQ and duplicates excluded); the current version equals ``expected_final``;
    a rerun without new data adds nothing.

The content hash is computed on the host over the downloaded rows: sha256 of the canonical JSON of
the rows sorted by key, without ``_updated_at`` (wall-clock) - independent of the job's own hash.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from typing import TYPE_CHECKING, Any

import pytest

from lakehouse.contracts.models import CustomerCdcEvent, to_wire
from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.manifest import Outcome
from lakehouse.generator.messages import OutboundMessage, cdc_messages

from .delta_io import download_table, read_rows, temp_topics
from .spark_jobs import metrics, run_ok
from .test_cdc_correctness import _message, _produce, _uuid


if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from pathlib import Path

pytestmark = pytest.mark.integration

ALPHA, BETA = "brand_alpha", "brand_beta"
BASE_TS = datetime(2026, 3, 1, tzinfo=UTC)
C1, C2 = _uuid(501), _uuid(502)
BRANDS = 3
GEN_EVENTS = 300
MAX_OFFSETS = 1000
MAX_FILES = 1000
Key = tuple[str, str]
Row = dict[str, Any]
PII = ("first_name", "last_name", "email_token", "phone_masked", "city")
WALL_CLOCK = "_updated_at"
#: Lineage differs legitimately between in-order and shuffled deliveries (other Kafka offsets),
#: so content comparisons leave it out together with the wall clock (D-076).
NOT_CONTENT = frozenset({WALL_CLOCK, "_bronze_ref"})

_built = [False]


# ---------------------------------------------------------------------------- pure helpers


def content_hash(rows: Sequence[Row]) -> str:
    """Order-independent hash of history rows without the wall clock and lineage."""
    canon = sorted(
        json.dumps(
            {k: v for k, v in r.items() if k not in NOT_CONTENT}, sort_keys=True, default=str
        )
        for r in rows
    )
    return hashlib.sha256("\n".join(canon).encode()).hexdigest()


def by_key(rows: Sequence[Row]) -> dict[Key, list[Row]]:
    out: dict[Key, list[Row]] = {}
    for r in rows:
        out.setdefault((r["brand_id"], r["customer_id"]), []).append(r)
    for versions in out.values():
        versions.sort(key=lambda r: r["_cdc_sequence"])
    return out


def structural_problems(rows: Sequence[Row]) -> list[str]:
    """SCD2 invariants: chain, single current, open last version (empty = well formed)."""
    problems: list[str] = []
    for key, versions in by_key(rows).items():
        seqs = [v["_cdc_sequence"] for v in versions]
        if len(set(seqs)) != len(seqs):
            problems.append(f"{key}: duplicate versions {seqs}")
        for cur, nxt in pairwise(versions):
            if cur["valid_to"] != nxt["valid_from"]:
                problems.append(f"{key}: broken valid_to chain at seq {cur['_cdc_sequence']}")
            if cur["is_current"]:
                problems.append(f"{key}: non-last version is current")
        if not versions[-1]["is_current"] or versions[-1]["valid_to"] is not None:
            problems.append(f"{key}: last version must be current and open")
    return problems


def manifest_problems(run: Any, rows: Sequence[Row]) -> list[str]:
    """History vs manifest: version count per key and the current version vs ``expected_final``."""
    manifest = run.manifest
    want: Counter[Key] = Counter(
        (e.brand_id, e.customer_id)
        for e in manifest.entries
        if e.expected_outcome in (Outcome.ACCEPT, Outcome.NOOP)
    )
    got = by_key(rows)
    problems = [
        f"{k}: expected {n} versions, got {len(got.get(k, []))}"
        for k, n in sorted(want.items())
        if len(got.get(k, [])) != n
    ]
    problems += [f"unexpected key {k}" for k in sorted(set(got) - set(want))]
    for e in manifest.expected_final:
        versions = got.get((e["brand_id"], e["customer_id"]))
        if not versions:
            continue
        current = [v for v in versions if v["is_current"]]
        if len(current) != 1:
            problems.append(f"{e['brand_id']}/{e['customer_id']}: {len(current)} current versions")
            continue
        cur = current[0]
        checks = {
            "_cdc_sequence": e["cdc_sequence"],
            "country": e["country"],
            "loyalty_tier": e["loyalty_tier"],
            "city": None if e["is_deleted"] else e["city"],
        }
        problems += [
            f"{e['brand_id']}/{e['customer_id']} {c}: expected {w!r}, got {cur[c]!r}"
            for c, w in checks.items()
            if cur[c] != w
        ]
    return problems


# ---------------------------------------------------------------------------- stack helpers


def _run(args: list[str]) -> str:
    if _built[0]:
        args = ["--skip-build", *args]
    out = run_ok(args)
    _built[0] = True
    return out


def _bronze(namespace: str, cdc_topic: str, click_topic: str) -> None:
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


def _history(namespace: str) -> dict[str, Any]:
    out = _run(
        [
            "silver_customers_history",
            "--namespace",
            namespace,
            "--max-files-per-trigger",
            str(MAX_FILES),
        ]
    )
    return dict(metrics(out))


def _round(
    servers: str,
    messages: Sequence[OutboundMessage],
    namespace: str,
    cdc_topic: str,
    click_topic: str,
) -> dict[str, Any]:
    """Produce, drain into Bronze, then into history; returns the history job metrics."""
    _produce(servers, messages)
    _bronze(namespace, cdc_topic, click_topic)
    result = _history(namespace)
    assert result["batches"] == 1, f"each round must be one history micro-batch: {result}"
    return result


def _read_history(namespace: str, root: Path, tag: str) -> list[Row]:
    dest = root / tag
    download_table("silver", "customers_history", namespace, dest)
    return read_rows(dest)


# ---------------------------------------------------------------------------- crafted scenarios


def _payload(brand: str, cust: str, seq: int, action: str, city: str) -> dict[str, Any]:
    """Contract-valid wire payload with a deterministic event_id (same in every namespace)."""
    n = seq + (1000 if brand == BETA else 0)
    model = CustomerCdcEvent.model_validate(
        {
            "event_id": str(uuid.UUID(int=(7 << 64) + n * 1000 + int(cust[-3:], 16), version=4)),
            "brand_id": brand,
            "schema_version": 1,
            "produced_at": BASE_TS + timedelta(minutes=seq, seconds=1),
            "customer_id": cust,
            "first_name": "Ada",
            "last_name": "Lovelace",
            "email": f"{cust[-3:]}.person@example.com",
            "phone": "+4915298761234",
            "city": city,
            "country": "DE",
            "loyalty_tier": "Gold",
            "signup_date": date(2025, 1, 1),
            "_cdc_action": action,
            "_cdc_sequence": seq,
            "_cdc_ts": BASE_TS + timedelta(minutes=seq),
        }
    )
    return to_wire(model)


CRAFTED = [
    _payload(ALPHA, C1, 1, "I", "Istanbul"),
    _payload(ALPHA, C1, 2, "U", "Berlin"),
    _payload(ALPHA, C1, 3, "U", "Hamburg"),
    _payload(ALPHA, C1, 4, "D", "Hamburg"),
    _payload(ALPHA, C1, 5, "U", "Munich"),  # resurrection
    _payload(ALPHA, C2, 1, "I", "Paris"),
    _payload(ALPHA, C2, 2, "U", "Lyon"),
    _payload(BETA, C1, 1, "I", "Rome"),  # same customer_id, other brand
    _payload(BETA, C1, 2, "U", "Milan"),
]
EXPECTED_CITIES = {
    (ALPHA, C1): ["Istanbul", "Berlin", "Hamburg", None, "Munich"],
    (ALPHA, C2): ["Paris", "Lyon"],
    (BETA, C1): ["Rome", "Milan"],
}


def _craft_run(
    servers: str, root: Path, tag: str, rounds: list[list[dict[str, Any]]]
) -> tuple[str, list[Row], list[dict[str, Any]]]:
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    runs: list[dict[str, Any]] = []
    with temp_topics(servers, f"scd2_{tag}", f"scd2_{tag}_click") as (cdc_topic, click_topic):
        for payloads in rounds:
            messages = [_message(cdc_topic, p) for p in payloads]
            runs.append(_round(servers, messages, namespace, cdc_topic, click_topic))
    return namespace, _read_history(namespace, root, tag), runs


@pytest.fixture(scope="module")
def crafted(
    bootstrap_servers: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp("scd2_crafted")
    in_order = [list(CRAFTED)]
    reversed_events = list(reversed(CRAFTED))
    third = len(reversed_events) // 3
    shuffled = [
        reversed_events[:third],
        reversed_events[third : 2 * third],
        reversed_events[2 * third :],
    ]
    _, rows_a, runs_a = _craft_run(bootstrap_servers, root, "inorder", in_order)
    _, rows_b, runs_b = _craft_run(bootstrap_servers, root, "shuffled", shuffled)
    yield {"a": rows_a, "b": rows_b, "runs_a": runs_a, "runs_b": runs_b}


def test_crafted_history_has_the_expected_versions_per_key(crafted: dict[str, Any]) -> None:
    for label in ("a", "b"):
        grouped = by_key(crafted[label])
        assert set(grouped) == set(EXPECTED_CITIES), label
        for key, cities in EXPECTED_CITIES.items():
            assert [v["city"] for v in grouped[key]] == cities, (label, key)
        assert sum(len(v) for v in grouped.values()) == len(CRAFTED)


def test_crafted_history_is_structurally_sound_and_tenant_isolated(crafted: dict[str, Any]) -> None:
    for label in ("a", "b"):
        assert structural_problems(crafted[label]) == [], label
        alpha = by_key(crafted[label])[(ALPHA, C1)]
        tomb = alpha[3]
        assert all(tomb[c] is None for c in PII), "tombstone version must not carry PII"
        assert alpha[4]["city"] == "Munich" and alpha[4]["email_token"] is not None
        beta = by_key(crafted[label])[(BETA, C1)]
        assert [v["is_current"] for v in beta] == [False, True]


def test_shuffled_delivery_gives_the_identical_content_hash(crafted: dict[str, Any]) -> None:
    assert len(crafted["runs_b"]) == 3 and len(crafted["runs_a"]) == 1
    assert content_hash(crafted["a"]) == content_hash(crafted["b"])


def test_job_counters_cover_every_crafted_version(crafted: dict[str, Any]) -> None:
    written = sum(r["counts"]["versions_written"] for r in crafted["runs_a"])
    assert written == len(CRAFTED)
    assert crafted["runs_a"][0]["counts"]["rows_read"] == len(CRAFTED)
    # the shuffled run rewrites keys touched by several rounds, so it writes at least as many
    assert sum(r["counts"]["versions_written"] for r in crafted["runs_b"]) >= len(CRAFTED)


# ---------------------------------------------------------------------------- generator-driven


@pytest.fixture(scope="module")
def generated(
    bootstrap_servers: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[dict[str, Any]]:
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    config = GeneratorConfig(
        _env_file=None,  # type: ignore[call-arg]
        cdc_cross_tenant_collision_rate=5.0,
    )
    run = generate_cdc(config, brands=BRANDS, events=GEN_EVENTS)
    root = tmp_path_factory.mktemp("scd2_generated")
    with temp_topics(bootstrap_servers, "scd2_gen", "scd2_gen_click") as (cdc_topic, click_topic):
        messages = cdc_messages(run, cdc_topic)
        split = len(messages) // 2
        runs = [
            _round(bootstrap_servers, part, namespace, cdc_topic, click_topic)
            for part in (messages[:split], messages[split:])
        ]
    rows = _read_history(namespace, root, "final")
    # rerun with nothing new: the checkpoint is exhausted, so no batch and no row may change
    rerun = _history(namespace)
    rerun_rows = _read_history(namespace, root, "rerun")
    yield {"run": run, "runs": runs, "rows": rows, "rerun": rerun, "rerun_rows": rerun_rows}


def test_generated_run_exercises_the_defects_under_test(generated: dict[str, Any]) -> None:
    counts = generated["run"].manifest.counts
    for name in ("duplicate", "out_of_order", "invalid_email"):
        assert counts[name] > 0, f"seeded run has no {name}: the test would be vacuous"


def test_versions_per_key_equal_valid_non_duplicate_emissions(generated: dict[str, Any]) -> None:
    assert manifest_problems(generated["run"], generated["rows"]) == []


def test_generated_history_is_structurally_sound(generated: dict[str, Any]) -> None:
    assert structural_problems(generated["rows"]) == []
    assert len(generated["runs"]) == 2


def test_rerun_without_new_data_adds_nothing(generated: dict[str, Any]) -> None:
    assert generated["rerun"]["batches"] == 0
    assert generated["rerun"]["counts"]["versions_written"] == 0
    assert len(generated["rerun_rows"]) == len(generated["rows"])
    assert content_hash(generated["rerun_rows"]) == content_hash(generated["rows"])


# ---------------------------------------------------------------------------- negative tests


def _good_rows() -> list[Row]:
    return [
        {
            "brand_id": ALPHA,
            "customer_id": "c1",
            "_cdc_sequence": 1,
            "valid_from": 1,
            "valid_to": 2,
            "is_current": False,
            "city": "A",
            "country": "DE",
            "loyalty_tier": "Gold",
            WALL_CLOCK: 1,
        },
        {
            "brand_id": ALPHA,
            "customer_id": "c1",
            "_cdc_sequence": 2,
            "valid_from": 2,
            "valid_to": None,
            "is_current": True,
            "city": "B",
            "country": "DE",
            "loyalty_tier": "Gold",
            WALL_CLOCK: 1,
        },
    ]


def test_content_hash_ignores_updated_at_and_row_order_but_sees_content() -> None:
    rows = _good_rows()
    base = content_hash(rows)
    assert content_hash(list(reversed(rows))) == base
    assert content_hash([{**r, WALL_CLOCK: 99} for r in rows]) == base
    assert content_hash([rows[0], {**rows[1], "city": "C"}]) != base
    assert content_hash(rows[:1]) != base


def test_structural_checker_detects_each_kind_of_violation() -> None:
    rows = _good_rows()
    assert structural_problems(rows) == []
    assert structural_problems([{**rows[0], "valid_to": 9}, rows[1]])
    assert structural_problems([{**rows[0], "is_current": True}, rows[1]])
    assert structural_problems([rows[0], {**rows[1], "is_current": False}])
    assert structural_problems([rows[0], {**rows[1], "valid_to": 5}])
    assert structural_problems([rows[0], dict(rows[0])])


def test_manifest_checker_detects_a_missing_extra_or_wrong_current_version() -> None:
    from types import SimpleNamespace

    from lakehouse.generator.manifest import ManifestEntry

    entries = [
        ManifestEntry(0, "e0", ALPHA, "c1", 1, "I"),
        ManifestEntry(1, "e1", ALPHA, "c1", 2, "U"),
        ManifestEntry(2, "e2", ALPHA, "c1", 2, "U", expected_outcome=Outcome.DUPLICATE),
        ManifestEntry(3, "e3", ALPHA, "c1", 9, "U", expected_outcome=Outcome.DLQ),
    ]
    final = [
        {
            "brand_id": ALPHA,
            "customer_id": "c1",
            "cdc_sequence": 2,
            "country": "DE",
            "loyalty_tier": "Gold",
            "city": "B",
            "is_deleted": False,
        }
    ]
    run = SimpleNamespace(manifest=SimpleNamespace(entries=entries, expected_final=final))
    rows = _good_rows()
    assert manifest_problems(run, rows) == []
    assert manifest_problems(run, rows[:1])  # missing version
    assert manifest_problems(
        run, [*rows, {**rows[0], "_cdc_sequence": 9}]
    )  # DLQ row became a version
    assert manifest_problems(run, [rows[0], {**rows[1], "city": "X"}])  # wrong current attributes
    assert manifest_problems(run, [*rows, {**rows[0], "customer_id": "c2"}])  # unexpected key
