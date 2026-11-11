"""generate_cdc: lifecycle, exact defect counts, outcome invariant, reference MERGE, feasibility."""

from __future__ import annotations

import random
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from lakehouse.contracts import CustomerCdcEvent, cdc_key, safe_errors
from lakehouse.generator.cdc import CdcRun, InfeasibleDefectPlan, generate_cdc
from lakehouse.generator.config import GeneratorConfig, defect_count
from lakehouse.generator.manifest import CdcDefect, Outcome, classify_outcomes

from . import _reference as ref


DLQ_DEFECTS = {CdcDefect.invalid_email, CdcDefect.null_email, CdcDefect.future_signup}
WIRE_FIELDS = {f.alias or n for n, f in CustomerCdcEvent.model_fields.items()}
FINAL_FIELDS = [
    "brand_id", "customer_id", "cdc_sequence", "event_id", "cdc_action",
    "is_deleted", "city", "country", "loyalty_tier",
]  # fmt: skip


def make(**kwargs: Any) -> GeneratorConfig:
    return GeneratorConfig(_env_file=None, **kwargs)  # type: ignore[call-arg]


class Case:
    def __init__(self, config: GeneratorConfig, brands: int, events: int) -> None:
        self.config, self.brands, self.events = config, brands, events
        self.run: CdcRun = generate_cdc(config, brands=brands, events=events)
        self.values = [line["value"] for line in self.run.lines]
        self.entries = self.run.manifest.entries
        self.valid = [ref.is_valid(v) for v in self.values]


@pytest.fixture(
    scope="module",
    params=[(42, 3, 1000), (7, 2, 500)],
    ids=["seed42-3brands-1000", "seed7-2brands-500"],
)
def case(request: pytest.FixtureRequest) -> Case:
    seed, brands, events = request.param
    return Case(make(seed=seed, cdc_cross_tenant_collision_rate=5.0), brands, events)


def _rate(config: GeneratorConfig, defect: CdcDefect) -> float:
    return {
        CdcDefect.invalid_email: config.cdc_invalid_email_rate,
        CdcDefect.null_email: config.cdc_null_email_rate,
        CdcDefect.future_signup: config.cdc_future_signup_rate,
        CdcDefect.out_of_order: config.cdc_out_of_order_rate,
        CdcDefect.duplicate: config.cdc_duplicate_rate,
    }[defect]


# ------------------------------------------------------------------------- structure


def test_lines_and_entries_are_aligned_and_counted(case: Case) -> None:
    m = case.run.manifest
    assert len(case.run.lines) == len(m.entries) == m.emitted_events
    assert [e.emission_index for e in m.entries] == list(range(m.emitted_events))
    assert m.base_events == case.events
    n_dup = defect_count(case.config.cdc_duplicate_rate, m.base_events)
    assert m.emitted_events == m.base_events + n_dup
    assert m.seed == case.config.seed


def test_key_and_header_follow_the_envelope_contract(case: Case) -> None:
    for line in case.run.lines[:200]:
        value = line["value"]
        assert line["key"] == f"{value['brand_id']}:{value['customer_id']}" == cdc_key(value)
        assert "brand_id" in str(line["headers"])
        assert value["brand_id"] in str(line["headers"])


def test_only_first_n_tenants_are_used_and_all_appear(case: Case) -> None:
    brands = {v["brand_id"] for v in case.values}
    assert brands == set(case.config.tenants[: case.brands])


def test_no_defect_marker_or_extra_field_in_any_payload(case: Case) -> None:
    for v in case.values:
        assert set(v) <= WIRE_FIELDS
    flat = " ".join(" ".join(map(str, v)) for v in case.values).lower()
    for word in ("defect", "expected_outcome", "manifest", "injected"):
        assert word not in flat


# ------------------------------------------------------------------------- lifecycle


def _by_customer(case: Case) -> dict[tuple[str, str], list[dict[str, Any]]]:
    groups: dict[tuple[str, str], dict[int, dict[str, Any]]] = defaultdict(dict)
    for v in case.values:
        groups[(v["brand_id"], v["customer_id"])][v["_cdc_sequence"]] = v
    return {k: [g[s] for s in sorted(g)] for k, g in groups.items()}


def test_lifecycle_first_insert_delete_terminal_sequences_increasing(case: Case) -> None:
    for events in _by_customer(case).values():
        seqs = [e["_cdc_sequence"] for e in events]
        actions = [e["_cdc_action"] for e in events]
        assert seqs == sorted(set(seqs))
        assert seqs[0] >= 1
        assert actions[0] == "I"
        assert "I" not in actions[1:]
        assert "D" not in actions[:-1]
        assert len(events) - 1 <= case.config.cdc_max_updates + 1  # updates (+ optional D)


def test_cdc_ts_is_monotonic_with_sequence_and_not_after_produced_at(case: Case) -> None:
    for events in _by_customer(case).values():
        stamps = [datetime.fromisoformat(e["_cdc_ts"]) for e in events]
        assert stamps == sorted(stamps)
        for e, ts in zip(events, stamps, strict=True):
            assert ts.tzinfo is not None
            assert ts <= datetime.fromisoformat(e["produced_at"])
            assert ts >= case.config.sim_start


def test_deleted_customers_per_tenant_equal_defect_count_of_tenant_customers(case: Case) -> None:
    per_brand: dict[str, list[list[dict[str, Any]]]] = defaultdict(list)
    for (brand, _), events in _by_customer(case).items():
        per_brand[brand].append(events)
    assert per_brand
    for brand, custs in per_brand.items():
        deleted = sum(1 for ev in custs if ev[-1]["_cdc_action"] == "D")
        assert deleted == defect_count(case.config.cdc_delete_rate, len(custs)), brand


def test_zero_delete_rate_produces_no_deletes(case: Case) -> None:
    none = generate_cdc(
        make(cdc_delete_rate=0.0, seed=case.config.seed), brands=case.brands, events=200
    )
    assert all(line["value"]["_cdc_action"] != "D" for line in none.lines)


def largest_remainder(total: int, weights: list[float]) -> list[int]:
    """Independent Hamilton apportionment: Decimal, ties broken by lower index."""
    w = [Decimal(str(x)) for x in weights]
    quotas = [Decimal(total) * x / sum(w) for x in w]
    counts = [int(q) for q in quotas]
    order = sorted(range(len(w)), key=lambda i: (-(quotas[i] - counts[i]), i))
    for i in order[: total - sum(counts)]:
        counts[i] += 1
    return counts


def test_per_tenant_base_event_counts_use_largest_remainder_on_renormalised_weights(
    case: Case,
) -> None:
    tenants = case.config.tenants[: case.brands]
    weights = case.config.tenant_weights[: case.brands]
    got = Counter(e.brand_id for e in case.entries if e.defect != CdcDefect.duplicate)
    assert [got[t] for t in tenants] == largest_remainder(case.events, weights)


def test_largest_remainder_reference_has_teeth() -> None:
    assert largest_remainder(10, [0.6, 0.3, 0.1]) == [6, 3, 1]
    assert largest_remainder(10, [1, 1, 1]) == [4, 3, 3]  # tie -> lowest index
    assert largest_remainder(5, [0.6, 0.3]) == [3, 2]


# ------------------------------------------------------------------------- exact counts


@pytest.mark.parametrize(
    "defect",
    [
        CdcDefect.invalid_email, CdcDefect.null_email, CdcDefect.future_signup,
        CdcDefect.out_of_order, CdcDefect.duplicate,
    ],
)  # fmt: skip
def test_defect_count_is_exact(case: Case, defect: CdcDefect) -> None:
    tagged = sum(1 for e in case.entries if e.defect == defect)
    assert tagged == defect_count(_rate(case.config, defect), case.run.manifest.base_events)


def test_payload_level_dlq_counts_are_exact_independent_of_manifest_tags(case: Case) -> None:
    base = case.run.manifest.base_events
    c = case.config
    null_email = sum(1 for v in case.values if v["email"] is None)
    assert null_email == defect_count(c.cdc_null_email_rate, base)
    total_dlq = defect_count(c.cdc_invalid_email_rate, base) + null_email
    total_dlq += defect_count(c.cdc_future_signup_rate, base)
    assert case.valid.count(False) == total_dlq


def test_cross_tenant_collisions_are_same_customer_id_in_two_brands(case: Case) -> None:
    owners: dict[str, set[str]] = defaultdict(set)
    for v in case.values:
        owners[v["customer_id"]].add(v["brand_id"])
    colliding = {c for c, b in owners.items() if len(b) > 1}
    first = case.config.tenants[0]
    later_customers = {
        (v["brand_id"], v["customer_id"]) for v in case.values if v["brand_id"] != first
    }
    assert len(colliding) == defect_count(
        case.config.cdc_cross_tenant_collision_rate, len(later_customers)
    )
    assert len(colliding) >= 1
    tagged = [e for e in case.entries if e.defect == CdcDefect.cross_tenant_collision]
    assert len(tagged) == len(colliding)
    for e in tagged:
        assert e.cdc_action == "I"
        assert e.brand_id != first
        assert e.customer_id in colliding


def test_collision_states_stay_independent_per_brand(case: Case) -> None:
    final = {(r["brand_id"], r["customer_id"]) for r in case.run.manifest.expected_final}
    ids = Counter(c for _, c in final)
    shared = [c for c, n in ids.items() if n > 1]
    assert shared  # same id, separate rows
    assert len(final) == len(set(final))


# ------------------------------------------------------------------------- payload validity


def test_clean_defect_kinds_validate_and_dlq_kinds_fail(case: Case) -> None:
    for entry, ok in zip(case.entries, case.valid, strict=True):
        assert ok == (entry.defect not in DLQ_DEFECTS), entry


def test_dlq_payload_errors_point_at_the_defective_field(case: Case) -> None:
    seen: set[CdcDefect] = set()
    for entry, value in zip(case.entries, case.values, strict=True):
        if entry.defect not in DLQ_DEFECTS:
            continue
        with pytest.raises(ValidationError) as info:
            CustomerCdcEvent.model_validate(value)
        locs = [e["loc"] for e in safe_errors(info.value)]
        assert locs
        if entry.defect in (CdcDefect.invalid_email, CdcDefect.null_email):
            assert locs == [("email",)]
        else:
            assert ("email",) not in locs
            assert (
                date.fromisoformat(value["signup_date"])
                > datetime.fromisoformat(value["produced_at"]).astimezone(UTC).date()
            )
        seen.add(entry.defect)
    assert seen == DLQ_DEFECTS


def test_null_email_payloads_carry_null_and_invalid_email_payloads_carry_a_string(
    case: Case,
) -> None:
    for entry, value in zip(case.entries, case.values, strict=True):
        if entry.defect == CdcDefect.null_email:
            assert value["email"] is None
        if entry.defect == CdcDefect.invalid_email:
            assert isinstance(value["email"], str)


def test_duplicates_are_byte_identical_replays_that_arrive_after_the_original(case: Case) -> None:
    first_seen: dict[tuple[str, str, int], int] = {}
    n_dup = 0
    for i, (entry, v) in enumerate(zip(case.entries, case.values, strict=True)):
        key = (v["brand_id"], v["customer_id"], v["_cdc_sequence"])
        if entry.defect == CdcDefect.duplicate:
            n_dup += 1
            j = first_seen[key]
            assert j < i
            assert ref.canonical(case.run.lines[i]) == ref.canonical(case.run.lines[j])
            assert case.entries[j].event_id == entry.event_id
            assert case.entries[j].defect != CdcDefect.out_of_order  # never a moved event
        else:
            assert key not in first_seen, "non-duplicate key repeated"
            first_seen[key] = i
    assert n_dup > 0


# ------------------------------------------------------------------------- outcome oracle


def test_expected_outcome_equals_independent_event_at_a_time_reference(case: Case) -> None:
    events = [(v["brand_id"], v["customer_id"], v["_cdc_sequence"]) for v in case.values]
    want = ref.reference_outcomes(events, case.valid)
    assert [e.expected_outcome.value for e in case.entries] == want


def test_outcome_counts_and_reconciliation_invariant(case: Case) -> None:
    m = case.run.manifest
    by = Counter(e.expected_outcome for e in m.entries)
    c = m.counts
    assert c["silver_accepted"] == by[Outcome.ACCEPT]
    assert c["dlq"] == by[Outcome.DLQ]
    assert c["duplicates_removed"] == by[Outcome.DUPLICATE]
    assert c["stale_cdc_noop"] == by[Outcome.NOOP]
    assert c["late_beyond_window"] == 0
    assert c["bronze"] == c["emitted"] == m.emitted_events
    assert c["emitted"] == (
        c["silver_accepted"] + c["dlq"] + c["duplicates_removed"] + c["stale_cdc_noop"]
        + c["late_beyond_window"]
    )  # fmt: skip


def test_dlq_and_duplicate_outcomes_match_defect_counts_exactly(case: Case) -> None:
    c, cfg = case.run.manifest.counts, case.config
    base = case.run.manifest.base_events
    assert c["dlq"] == sum(
        defect_count(r, base)
        for r in (cfg.cdc_invalid_email_rate, cfg.cdc_null_email_rate, cfg.cdc_future_signup_rate)
    )
    assert c["duplicates_removed"] == defect_count(cfg.cdc_duplicate_rate, base)


def test_stale_noop_count_equals_number_of_moved_events(case: Case) -> None:
    moved = defect_count(case.config.cdc_out_of_order_rate, case.run.manifest.base_events)
    assert case.run.manifest.counts["stale_cdc_noop"] == moved


def test_every_moved_event_is_stale_noop_and_its_customer_has_a_later_sequence_earlier(
    case: Case,
) -> None:
    seen_max: dict[tuple[str, str], int] = defaultdict(int)
    for entry in case.entries:
        k = (entry.brand_id, entry.customer_id)
        if entry.defect == CdcDefect.out_of_order:
            assert entry.expected_outcome == Outcome.NOOP
            assert seen_max[k] > entry.cdc_sequence
        seen_max[k] = max(seen_max[k], entry.cdc_sequence)


def _last_sequence(case: Case) -> dict[tuple[str, str], int]:
    last: dict[tuple[str, str], int] = defaultdict(int)
    for entry in case.entries:
        k = (entry.brand_id, entry.customer_id)
        last[k] = max(last[k], entry.cdc_sequence)
    return last


def test_moved_events_have_both_intermediate_and_after_last_cases(case: Case) -> None:
    last = _last_sequence(case)
    moved = [e for e in case.entries if e.defect == CdcDefect.out_of_order]
    assert moved
    assert all(e.late_after_sequence is not None for e in moved)
    intermediate = [
        e for e in moved
        if e.late_after_sequence < last[(e.brand_id, e.customer_id)]  # type: ignore[operator]
    ]  # fmt: skip
    after_last = [e for e in moved if e.late_after_sequence == last[(e.brand_id, e.customer_id)]]
    assert intermediate and after_last
    assert len(intermediate) + len(after_last) == len(moved)
    assert case.run.manifest.counts["out_of_order_intermediate"] == len(intermediate)
    assert case.run.manifest.counts["out_of_order_intermediate"] < len(moved)


def test_late_after_sequence_is_set_only_on_moved_events(case: Case) -> None:
    for entry in case.entries:
        assert (entry.late_after_sequence is not None) == (entry.defect == CdcDefect.out_of_order)


def test_anchor_is_valid_later_and_emitted_before_its_late_event(case: Case) -> None:
    first_index: dict[tuple[str, str, int], int] = {}
    for i, entry in enumerate(case.entries):
        first_index.setdefault((entry.brand_id, entry.customer_id, entry.cdc_sequence), i)
    for i, entry in enumerate(case.entries):
        if entry.defect != CdcDefect.out_of_order:
            continue
        anchor_seq = entry.late_after_sequence
        assert anchor_seq is not None and anchor_seq > entry.cdc_sequence
        j = first_index[(entry.brand_id, entry.customer_id, anchor_seq)]
        anchor = case.entries[j]
        assert j < i
        assert case.valid[j]  # the anchor is never a DLQ defect
        assert anchor.defect != CdcDefect.out_of_order  # and never itself a moved event
        assert anchor.expected_outcome in (Outcome.ACCEPT, Outcome.NOOP)


def test_default_config_late_event_split_is_stable() -> None:
    run = generate_cdc(make(seed=42), brands=3, events=1000)
    c = run.manifest.counts
    assert c["out_of_order"] == c["stale_cdc_noop"]
    assert 0 < c["out_of_order_intermediate"] < c["out_of_order"]


# ------------------------------------------------------------------------- reference MERGE


def test_expected_final_equals_independent_sequence_ordered_merge(case: Case) -> None:
    rows = case.run.manifest.expected_final
    assert rows == ref.reference_final(case.values)
    assert all(set(r) == set(FINAL_FIELDS) for r in rows)
    assert [(r["brand_id"], r["customer_id"]) for r in rows] == sorted(
        (r["brand_id"], r["customer_id"]) for r in rows
    )


def test_customers_with_only_dlq_events_have_no_expected_final_row(case: Case) -> None:
    valid_customers = {
        (v["brand_id"], v["customer_id"])
        for v, ok in zip(case.values, case.valid, strict=True)
        if ok
    }
    final = {(r["brand_id"], r["customer_id"]) for r in case.run.manifest.expected_final}
    assert final == valid_customers


@pytest.mark.parametrize("shuffle_seed", range(5))
def test_final_state_is_identical_for_any_permutation_of_valid_events(
    case: Case, shuffle_seed: int
) -> None:
    valid_values = [v for v, ok in zip(case.values, case.valid, strict=True) if ok]
    random.Random(shuffle_seed).shuffle(valid_values)
    assert ref.reference_final(valid_values) == case.run.manifest.expected_final


def test_negative_last_write_wins_merge_disagrees_with_expected_final(case: Case) -> None:
    """The sequence guard matters: an arrival-order merge must NOT reproduce expected_final."""
    wrong = ref.reference_final(case.values, ignore_sequence=True)
    assert wrong != case.run.manifest.expected_final


def test_negative_tampered_expected_final_is_detected(case: Case) -> None:
    rows = [dict(r) for r in case.run.manifest.expected_final]
    rows[0]["cdc_sequence"] += 1
    assert rows != ref.reference_final(case.values)


def test_negative_tampered_outcome_is_detected(case: Case) -> None:
    events = [(v["brand_id"], v["customer_id"], v["_cdc_sequence"]) for v in case.values]
    want = ref.reference_outcomes(events, case.valid)
    got = [e.expected_outcome.value for e in case.entries]
    got[0] = ref.NOOP if got[0] != ref.NOOP else ref.ACCEPT
    assert got != want


def test_deleted_customers_are_tombstoned_in_expected_final(case: Case) -> None:
    rows = case.run.manifest.expected_final
    assert any(r["is_deleted"] for r in rows)
    for r in rows:
        assert r["is_deleted"] == (r["cdc_action"] == "D")


# ------------------------------------------------------------------------- misc


def test_config_fingerprint_depends_on_generation_fields_only() -> None:
    base = generate_cdc(make(), brands=2, events=100).manifest.config_fingerprint
    same = generate_cdc(
        make(kafka_bootstrap_servers="other:1", topic_customer_cdc="x", order_duplicate_rate=9.0),
        brands=2,
        events=100,
    ).manifest.config_fingerprint
    other = generate_cdc(make(seed=43), brands=2, events=100).manifest.config_fingerprint
    assert len(base) == 64
    assert base == same
    assert base != other


def test_brands_above_configured_tenants_is_rejected() -> None:
    with pytest.raises(ValueError):
        generate_cdc(make(), brands=4, events=100)


def test_single_brand_without_collisions_is_feasible() -> None:
    run = generate_cdc(make(cdc_cross_tenant_collision_rate=0.0), brands=1, events=300)
    assert {v["value"]["brand_id"] for v in run.lines} == {"brand_alpha"}


def test_zero_defect_config_has_only_accepts() -> None:
    zero = make(
        cdc_invalid_email_rate=0, cdc_null_email_rate=0, cdc_future_signup_rate=0,
        cdc_out_of_order_rate=0, cdc_duplicate_rate=0, cdc_cross_tenant_collision_rate=0,
    )  # fmt: skip
    run = generate_cdc(zero, brands=2, events=200)
    assert {e.expected_outcome for e in run.manifest.entries} == {Outcome.ACCEPT}
    assert run.manifest.emitted_events == 200


# ------------------------------------------------------------------------- feasibility (D-026)


def test_infeasible_defect_plan_is_a_value_error() -> None:
    assert issubclass(InfeasibleDefectPlan, ValueError)


def test_single_brand_with_collision_rate_is_infeasible() -> None:
    with pytest.raises(InfeasibleDefectPlan):
        generate_cdc(make(cdc_cross_tenant_collision_rate=1.0), brands=1, events=300)


def test_too_few_clean_events_for_all_defect_pools_is_infeasible() -> None:
    cfg = make(
        cdc_invalid_email_rate=20, cdc_null_email_rate=20, cdc_future_signup_rate=10,
        cdc_out_of_order_rate=50, cdc_duplicate_rate=50, cdc_cross_tenant_collision_rate=0,
    )  # fmt: skip
    with pytest.raises(InfeasibleDefectPlan):
        generate_cdc(cfg, brands=2, events=10)


def test_out_of_order_without_updates_is_infeasible() -> None:
    with pytest.raises(InfeasibleDefectPlan):
        generate_cdc(make(cdc_max_updates=0), brands=2, events=300)


def test_no_updates_without_out_of_order_is_feasible() -> None:
    cfg = make(cdc_max_updates=0, cdc_out_of_order_rate=0)
    run = generate_cdc(cfg, brands=2, events=300)
    assert all(line["value"]["_cdc_action"] in {"I", "D"} for line in run.lines)
    assert run.manifest.counts["stale_cdc_noop"] == 0


# ------------------------------------------------------------------------- phones, batch replay

FICTIONAL_PHONE_PREFIXES = ("+447700900",)  # UK Ofcom drama range only (D-031)


def test_every_phone_is_in_a_fictional_drama_range(case: Case) -> None:
    phones = [v["phone"] for v in case.values if v["phone"] is not None]
    assert phones
    assert all(p.startswith(FICTIONAL_PHONE_PREFIXES) for p in phones), [
        p for p in phones if not p.startswith(FICTIONAL_PHONE_PREFIXES)
    ][:3]


@pytest.mark.parametrize("batch_size", [1, 7, 50, "all"])
def test_batched_classification_reconciles_and_last_accept_matches_expected_final(
    case: Case, batch_size: int | str
) -> None:
    n = len(case.entries)
    size = n if batch_size == "all" else int(batch_size)
    ends = [*range(size, n, size), n]
    outcomes = classify_outcomes(case.entries, case.valid, ends)

    by = Counter(outcomes)
    assert len(outcomes) == n
    assert by[Outcome.ACCEPT] + by[Outcome.DLQ] + by[Outcome.DUPLICATE] + by[Outcome.NOOP] == n
    assert by[Outcome.DLQ] == case.run.manifest.counts["dlq"]
    assert by[Outcome.DUPLICATE] == case.run.manifest.counts["duplicates_removed"]

    last_accept: dict[tuple[str, str], str] = {}
    for entry, outcome in zip(case.entries, outcomes, strict=True):
        if outcome == Outcome.ACCEPT:
            last_accept[(entry.brand_id, entry.customer_id)] = entry.event_id
    final = {
        (r["brand_id"], r["customer_id"]): r["event_id"] for r in case.run.manifest.expected_final
    }
    assert last_accept == final


def test_batched_classification_differs_from_event_at_a_time_for_large_batches(case: Case) -> None:
    """Negative: batching changes ACCEPT/NOOP split, so the reconciliation test can detect it."""
    one = classify_outcomes(case.entries, case.valid, None)
    big = classify_outcomes(case.entries, case.valid, [len(case.entries)])
    assert Counter(one)[Outcome.ACCEPT] > Counter(big)[Outcome.ACCEPT]


# ------------------------------------------------------------------------- input validation


def test_classify_outcomes_rejects_valid_length_mismatch(case: Case) -> None:
    with pytest.raises(ValueError):
        classify_outcomes(case.entries[:5], [True] * 4, None)


def test_classify_outcomes_rejects_descending_batch_ends(case: Case) -> None:
    with pytest.raises(ValueError):
        classify_outcomes(case.entries[:5], [True] * 5, [3, 2, 5])


def test_classify_outcomes_rejects_out_of_range_batch_ends(case: Case) -> None:
    with pytest.raises(ValueError):
        classify_outcomes(case.entries[:5], [True] * 5, [2, 6])
    with pytest.raises(ValueError):
        classify_outcomes(case.entries[:5], [True] * 5, [0, 5])
