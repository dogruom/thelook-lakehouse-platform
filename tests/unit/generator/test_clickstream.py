"""generate_clickstream: funnel shape, exact defect counts, determinism (W1.D5 / D5.1)."""

from __future__ import annotations

import json
from collections import Counter
from datetime import timedelta
from itertools import pairwise
from typing import TYPE_CHECKING, Any

import pytest
from pydantic import ValidationError

from lakehouse.contracts import ClickstreamEvent
from lakehouse.generator.clickstream import FUNNEL, ClickstreamRun, generate_clickstream
from lakehouse.generator.config import GeneratorConfig, defect_count
from lakehouse.generator.io import write_clickstream_run


if TYPE_CHECKING:
    from pathlib import Path

    from lakehouse.generator.messages import OutboundMessage


DEFECTS = {"late", "duplicate", "null_session", "malformed_json"}
OUTCOME = {
    "late": "ACCEPT",
    "duplicate": "DUPLICATE",
    "null_session": "DLQ",
    "malformed_json": "PARSE_ERROR",
}
TEN_MIN, SIXTY_MIN = timedelta(minutes=10), timedelta(minutes=60)


def make(**kwargs: Any) -> GeneratorConfig:
    return GeneratorConfig(_env_file=None, **kwargs)  # type: ignore[call-arg]


def parses(message: OutboundMessage) -> bool:
    try:
        json.loads(message.value)
    except ValueError:
        return False
    return True


def parse(message: OutboundMessage) -> dict[str, Any]:
    return json.loads(message.value)  # type: ignore[no-any-return]


class Case:
    def __init__(self, config: GeneratorConfig, brands: int, events: int) -> None:
        self.config, self.brands, self.events = config, brands, events
        self.run: ClickstreamRun = generate_clickstream(config, brands=brands, events=events)
        self.messages = self.run.messages
        self.counts = self.run.manifest.counts
        self.entries = [e for e in self.run.manifest.entries if e.get("defect")]
        self.by_defect: dict[str, list[dict[str, Any]]] = {d: [] for d in DEFECTS}
        for entry in self.entries:
            self.by_defect[entry["defect"]].append(entry)
        self.defect_idx = {e["index"] for e in self.entries}
        self.invalid_idx = {
            e["index"] for e in self.entries if e["defect"] in ("null_session", "malformed_json")
        }


@pytest.fixture(
    scope="module",
    params=[(42, 3, 5000), (7, 2, 2000)],
    ids=["seed42-3brands-5000", "seed7-2brands-2000"],
)
def case(request: pytest.FixtureRequest) -> Case:
    seed, brands, events = request.param
    return Case(make(seed=seed), brands, events)


def _rate(config: GeneratorConfig, defect: str) -> float:
    return {
        "late": config.click_late_rate,
        "duplicate": config.click_duplicate_rate,
        "null_session": config.click_null_session_rate,
        "malformed_json": config.click_malformed_json_rate,
    }[defect]


# --- exact counts ---------------------------------------------------------------------------


def test_counts_keys_are_exact(case: Case) -> None:
    assert set(case.counts) == {
        "base", "late", "duplicate", "null_session", "malformed_json", "emitted",
    }  # fmt: skip


def test_base_and_emitted_totals(case: Case) -> None:
    assert case.counts["base"] == case.events
    assert case.counts["emitted"] == case.counts["base"] + case.counts["duplicate"]
    assert case.counts["emitted"] == len(case.messages)


@pytest.mark.parametrize("defect", sorted(DEFECTS))
def test_defect_count_equals_config_rate_exactly(case: Case, defect: str) -> None:
    expected = defect_count(_rate(case.config, defect), case.events)
    assert case.counts[defect] == expected
    assert len(case.by_defect[defect]) == expected


def test_default_5000_event_counts_are_the_documented_numbers() -> None:
    run = generate_clickstream(make(seed=42), brands=3, events=5000)
    assert run.manifest.counts == {
        "base": 5000, "late": 500, "duplicate": 200, "null_session": 50,
        "malformed_json": 25, "emitted": 5200,
    }  # fmt: skip


def test_custom_rates_change_counts_exactly() -> None:
    config = make(
        seed=1, click_late_rate=20.0, click_duplicate_rate=10.0,
        click_null_session_rate=2.0, click_malformed_json_rate=1.0,
    )  # fmt: skip
    counts = generate_clickstream(config, brands=2, events=1000).manifest.counts
    assert (counts["late"], counts["duplicate"]) == (200, 100)
    assert (counts["null_session"], counts["malformed_json"]) == (20, 10)
    assert counts["emitted"] == 1100


def test_zero_rates_yield_clean_stream() -> None:
    config = make(
        seed=1, click_late_rate=0, click_duplicate_rate=0,
        click_null_session_rate=0, click_malformed_json_rate=0,
    )  # fmt: skip
    run = generate_clickstream(config, brands=2, events=300)
    assert len(run.messages) == 300
    assert not [e for e in run.manifest.entries if e.get("defect")]
    for message in run.messages:
        ClickstreamEvent.model_validate(parse(message))


# --- manifest entries -----------------------------------------------------------------------


def test_entries_shape_sorted_and_outcomes(case: Case) -> None:
    indexes = [e["index"] for e in case.run.manifest.entries]
    assert indexes == sorted(indexes)
    assert len(indexes) == len(set(indexes))
    for entry in case.entries:
        assert {"index", "event_id", "brand_id", "defect", "expected_outcome"} <= set(entry)
        assert entry["expected_outcome"] == OUTCOME[entry["defect"]]
        assert 0 <= entry["index"] < len(case.messages)
        assert case.messages[entry["index"]].brand_id == entry["brand_id"]


def test_late_null_and_malformed_are_disjoint_base_events(case: Case) -> None:
    mutated = [
        e["index"] for d in ("late", "null_session", "malformed_json") for e in case.by_defect[d]
    ]
    assert len(mutated) == len(set(mutated))
    assert len(mutated) == (
        case.counts["late"] + case.counts["null_session"] + case.counts["malformed_json"]
    )


# --- late -----------------------------------------------------------------------------------


def test_late_events_are_10_to_60_minutes_behind(case: Case) -> None:
    assert case.by_defect["late"]
    for entry in case.by_defect["late"]:
        payload = parse(case.messages[entry["index"]])
        event = ClickstreamEvent.model_validate(payload)
        lag = event.produced_at - event.event_timestamp
        assert TEN_MIN <= lag <= SIXTY_MIN, lag
        assert payload["event_id"] == entry["event_id"]


def test_non_late_clean_events_are_under_10_minutes_behind(case: Case) -> None:
    checked = 0
    for index, message in enumerate(case.messages):
        if index in case.defect_idx:
            continue
        event = ClickstreamEvent.model_validate(parse(message))
        lag = event.produced_at - event.event_timestamp
        assert timedelta(0) <= lag < TEN_MIN, (index, lag)
        checked += 1
    assert checked > 0.5 * len(case.messages)


def test_late_bound_check_detects_an_on_time_event() -> None:
    """Negative: a 5-minute lag must not satisfy the late window predicate."""
    assert not (TEN_MIN <= timedelta(minutes=5) <= SIXTY_MIN)


# --- duplicates -----------------------------------------------------------------------------


def test_duplicates_are_byte_identical_copies_of_clean_events(case: Case) -> None:
    dup_idx = {e["index"] for e in case.by_defect["duplicate"]}
    assert len(dup_idx) == case.counts["duplicate"] > 0
    clean = {
        (m.key, m.value, m.headers) for i, m in enumerate(case.messages) if i not in case.defect_idx
    }
    all_ids = Counter(parse(m)["event_id"] for m in case.messages if parses(m))
    for index in dup_idx:
        m = case.messages[index]
        assert (m.key, m.value, m.headers) in clean, "duplicate must copy a clean event"
        assert all_ids[parse(m)["event_id"]] == 2


def test_event_ids_unique_except_exactly_the_duplicates(case: Case) -> None:
    ids = [parse(m)["event_id"] for m in case.messages if parses(m)]
    assert len(ids) - len(set(ids)) == case.counts["duplicate"]


# --- null session / malformed ---------------------------------------------------------------


def test_null_session_events_have_empty_session_key_and_null_field(case: Case) -> None:
    assert len(case.by_defect["null_session"]) == case.counts["null_session"] > 0
    for entry in case.by_defect["null_session"]:
        message = case.messages[entry["index"]]
        assert parse(message)["session_id"] is None
        assert message.key == f"{entry['brand_id']}:"
        assert dict(message.headers)["brand_id"] == entry["brand_id"].encode()


def test_null_session_is_rejected_by_the_contract(case: Case) -> None:
    entry = case.by_defect["null_session"][0]
    with pytest.raises(ValidationError):
        ClickstreamEvent.model_validate(parse(case.messages[entry["index"]]))


def test_malformed_values_fail_json_but_keep_key_and_headers(case: Case) -> None:
    assert len(case.by_defect["malformed_json"]) == case.counts["malformed_json"] > 0
    for entry in case.by_defect["malformed_json"]:
        message = case.messages[entry["index"]]
        with pytest.raises(ValueError):  # JSONDecodeError / UnicodeDecodeError
            json.loads(message.value)
        brand = entry["brand_id"]
        assert message.key.startswith(f"{brand}:")
        assert len(message.key) > len(brand) + 1
        headers = dict(message.headers)
        assert headers["brand_id"] == brand.encode()
        assert headers["schema_version"] == b"1"
        assert headers["event_type"].decode() in FUNNEL


def test_unparseable_messages_are_exactly_the_malformed_ones(case: Case) -> None:
    bad = {i for i, m in enumerate(case.messages) if not parses(m)}
    assert bad == {e["index"] for e in case.by_defect["malformed_json"]}


# --- clean events: contract, key, headers ---------------------------------------------------


def test_clean_late_and_duplicate_values_validate_against_contract(case: Case) -> None:
    for index, message in enumerate(case.messages):
        if index in case.invalid_idx:
            continue
        assert ClickstreamEvent.model_validate(parse(message)).schema_version == 1


def test_key_and_headers_for_valid_events(case: Case) -> None:
    for index, message in enumerate(case.messages):
        if index in case.invalid_idx:
            continue
        payload = parse(message)
        assert message.key == f"{payload['brand_id']}:{payload['session_id']}"
        assert message.brand_id == payload["brand_id"]
        assert dict(message.headers) == {
            "brand_id": payload["brand_id"].encode(),
            "schema_version": b"1",
            "event_type": payload["event_type"].encode(),
        }


def test_topic_default_and_override() -> None:
    config = make(seed=3)
    default = generate_clickstream(config, brands=2, events=100)
    assert {m.topic for m in default.messages} == {config.topic_clickstream}
    custom = generate_clickstream(config, brands=2, events=100, topic="my_topic")
    assert {m.topic for m in custom.messages} == {"my_topic"}


def test_brands_are_the_first_n_tenants(case: Case) -> None:
    assert {m.brand_id for m in case.messages} == set(case.config.tenants[: case.brands])


def test_a_session_belongs_to_exactly_one_brand(case: Case) -> None:
    owner: dict[str, str] = {}
    for message in case.messages:
        if not parses(message):
            continue
        payload = parse(message)
        if payload.get("session_id") is None:
            continue
        assert owner.setdefault(payload["session_id"], payload["brand_id"]) == payload["brand_id"]


# --- funnel ---------------------------------------------------------------------------------


def test_funnel_constant() -> None:
    assert FUNNEL == ("page_view", "search", "add_to_cart", "checkout", "purchase")


def test_event_type_frequencies_strictly_decrease_along_funnel(case: Case) -> None:
    dup_idx = {e["index"] for e in case.by_defect["duplicate"]}
    counts = Counter(
        parse(m)["event_type"]
        for i, m in enumerate(case.messages)
        if i not in dup_idx and parses(m)
    )
    series = [counts[t] for t in FUNNEL]
    assert all(c > 0 for c in series), series
    assert all(a > b for a, b in pairwise(series)), series


def test_funnel_ordering_predicate_rejects_a_flat_distribution() -> None:
    """Negative: the strictly-decreasing predicate must reject a non-funnel shape."""
    flat = [10, 10, 5, 3, 1]
    assert not all(a > b for a, b in pairwise(flat))


# --- determinism & validation ---------------------------------------------------------------


def _snapshot(run: ClickstreamRun) -> list[tuple[str, bytes, tuple[tuple[str, bytes], ...]]]:
    return [(m.key, m.value, m.headers) for m in run.messages]


def test_same_seed_is_byte_identical() -> None:
    a = generate_clickstream(make(seed=42), brands=3, events=500)
    b = generate_clickstream(make(seed=42), brands=3, events=500)
    assert _snapshot(a) == _snapshot(b)
    assert a.manifest.entries == b.manifest.entries
    assert a.manifest.counts == b.manifest.counts


def test_different_seed_differs() -> None:
    a = generate_clickstream(make(seed=42), brands=3, events=500)
    b = generate_clickstream(make(seed=43), brands=3, events=500)
    assert _snapshot(a) != _snapshot(b)


@pytest.mark.parametrize("brands", [0, 4, -1])
def test_brands_out_of_range_raises(brands: int) -> None:
    with pytest.raises(ValueError):
        generate_clickstream(make(seed=1), brands=brands, events=100)


@pytest.mark.parametrize("events", [0, -5])
def test_events_below_one_raises(events: int) -> None:
    with pytest.raises(ValueError):
        generate_clickstream(make(seed=1), brands=2, events=events)


# --- file output ----------------------------------------------------------------------------


def test_write_clickstream_run_writes_events_and_manifest(tmp_path: Path) -> None:
    run = generate_clickstream(make(seed=42), brands=3, events=500)
    events_path, manifest_path = write_clickstream_run(run, tmp_path / "out")
    assert events_path.name == "clickstream_events.ndjson"
    assert manifest_path.name == "clickstream_manifest.json"
    assert len(events_path.read_bytes().splitlines()) == len(run.messages)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    counts = manifest.get("counts", manifest)
    assert counts["emitted"] == len(run.messages)


def test_write_clickstream_run_is_byte_stable(tmp_path: Path) -> None:
    run_a = generate_clickstream(make(seed=42), brands=3, events=300)
    run_b = generate_clickstream(make(seed=42), brands=3, events=300)
    ea, ma = write_clickstream_run(run_a, tmp_path / "a")
    eb, mb = write_clickstream_run(run_b, tmp_path / "b")
    assert ea.read_bytes() == eb.read_bytes()
    assert ma.read_bytes() == mb.read_bytes()


# --- D-096: identified sessions reference same-brand CDC customers ---------------------------


def _pools_run(events: int = 2000, seed: int = 42) -> tuple[ClickstreamRun, dict[str, list[str]]]:
    from lakehouse.generator.cdc import generate_cdc
    from lakehouse.generator.orders import customer_pools_from_cdc

    config = make(seed=seed)
    pools = customer_pools_from_cdc(generate_cdc(config, brands=3, events=1000))
    return generate_clickstream(config, brands=3, events=events, customer_pools=pools), pools


def test_identified_sessions_draw_from_the_same_brand_pool() -> None:
    run, pools = _pools_run()
    identified = 0
    for message in run.messages:
        if not parses(message):
            continue
        event = parse(message)
        if event.get("customer_id") is None:
            continue
        identified += 1
        assert event["customer_id"] in pools[event["brand_id"]]
    assert identified > 0


def test_pools_keep_anonymous_share_and_counts() -> None:
    run, _ = _pools_run()
    plain = generate_clickstream(make(seed=42), brands=3, events=2000)
    assert run.manifest.counts == plain.manifest.counts
    anon = sum(1 for m in run.messages if parses(m) and parse(m).get("customer_id") is None)
    anon_plain = sum(1 for m in plain.messages if parses(m) and parse(m).get("customer_id") is None)
    assert anon == anon_plain


def test_pools_run_is_byte_identical_and_fingerprint_includes_pools() -> None:
    first, _ = _pools_run()
    second, _ = _pools_run()
    assert [m.value for m in first.messages] == [m.value for m in second.messages]
    plain = generate_clickstream(make(seed=42), brands=3, events=2000)
    assert first.manifest.config_fingerprint != plain.manifest.config_fingerprint


@pytest.mark.parametrize(
    "pools", [{}, {"brand_alpha": [], "brand_beta": ["x"], "brand_gamma": ["y"]}]
)
def test_missing_or_empty_pool_raises(pools: dict[str, list[str]]) -> None:
    with pytest.raises(ValueError, match="customer pool"):
        generate_clickstream(make(seed=42), brands=3, events=100, customer_pools=pools)
