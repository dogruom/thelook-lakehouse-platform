"""classify_outcomes: precedence DLQ -> DUPLICATE -> NOOP -> ACCEPT (DATA_CONTRACTS §4.1, D-022..D-025).

Expectations are written by hand and cross-checked against an independent reference
(``_reference.reference_outcomes``); the reference itself has negative tests.
"""

from __future__ import annotations

import random

import pytest

from lakehouse.generator.manifest import ManifestEntry, Outcome, classify_outcomes

from . import _reference as ref


A, N, D_, X = Outcome.ACCEPT, Outcome.NOOP, Outcome.DUPLICATE, Outcome.DLQ


def entry(
    i: int, seq: int, *, brand: str = "brand_alpha", cust: str = "c1", expected: Outcome = A
) -> ManifestEntry:
    return ManifestEntry(
        emission_index=i,
        event_id=f"e{i}",
        brand_id=brand,
        customer_id=cust,
        cdc_sequence=seq,
        cdc_action="I" if seq == 1 else "U",
        defect=None,
        expected_outcome=expected,  # placeholder: classify_outcomes must not read it
    )


def run(
    seqs: list[int],
    valid: list[bool] | None = None,
    batch_ends: list[int] | None = None,
) -> list[Outcome]:
    entries = [entry(i, s) for i, s in enumerate(seqs)]
    return classify_outcomes(
        entries, valid if valid is not None else [True] * len(seqs), batch_ends
    )


def test_in_order_events_are_all_accepted_at_batch_size_one() -> None:
    assert run([1, 2, 3]) == [A, A, A]


def test_in_order_updates_in_one_batch_keep_only_the_max_sequence() -> None:
    assert run([2, 3], batch_ends=[2]) == [N, A]


def test_same_updates_in_separate_batches_are_both_accepted() -> None:
    assert run([2, 3], batch_ends=[1, 2]) == [A, A]


def test_batch_keeps_max_sequence_even_when_it_is_not_last() -> None:
    assert run([1, 5, 3], batch_ends=[3]) == [N, A, N]


def test_replay_of_older_sequence_after_newer_is_duplicate_not_stale() -> None:
    assert run([2, 5, 2]) == [A, A, D_]


def test_cross_batch_duplicate_is_detected_against_history() -> None:
    assert run([3, 3], batch_ends=[1, 2]) == [A, D_]


def test_duplicate_within_one_batch_is_duplicate() -> None:
    assert run([3, 3], batch_ends=[2]) == [A, D_]


def test_dlq_initial_insert_then_valid_update_is_accepted() -> None:
    assert run([1, 2], valid=[False, True]) == [X, A]


def test_all_invalid_events_are_dlq() -> None:
    assert run([1, 2, 3], valid=[False] * 3) == [X, X, X]


def test_invalid_emission_does_not_register_its_key_so_valid_replay_is_not_duplicate() -> None:
    assert run([2, 2], valid=[False, True]) == [X, A]


def test_valid_then_invalid_same_key_is_dlq_not_duplicate() -> None:
    """DLQ outranks DUPLICATE."""
    assert run([2, 2], valid=[True, False]) == [A, X]


def test_chained_move_1_4_2_3_ends_with_two_stale_events() -> None:
    assert run([1, 4, 2, 3]) == [A, A, N, N]


def test_stale_event_in_later_batch_is_noop() -> None:
    assert run([4, 2], batch_ends=[1, 2]) == [A, N]


def test_equal_sequence_from_different_event_is_not_applied_twice() -> None:
    assert run([3, 3, 3]) == [A, D_, D_]


def test_tenants_are_independent_for_same_customer_id_and_sequence() -> None:
    entries = [entry(0, 1, brand="brand_alpha"), entry(1, 1, brand="brand_beta")]
    assert classify_outcomes(entries, [True, True], None) == [A, A]


def test_tenant_isolation_negative_ignoring_brand_would_flag_duplicate() -> None:
    """The brand-blind rule gives a different answer, so the test above can fail."""
    brandless = ref.reference_outcomes([("x", "c1", 1), ("x", "c1", 1)], [True, True])
    assert brandless == [ref.ACCEPT, ref.DUPLICATE]
    assert brandless != [ref.ACCEPT, ref.ACCEPT]


def test_does_not_trust_the_placeholder_expected_outcome_on_input() -> None:
    entries = [entry(0, 2, expected=X), entry(1, 1, expected=A)]
    assert classify_outcomes(entries, [True, True], None) == [A, N]


def test_none_batch_ends_equals_explicit_batch_size_one() -> None:
    seqs = [1, 3, 2, 3, 4]
    assert run(seqs) == run(seqs, batch_ends=[1, 2, 3, 4, 5])


def test_empty_input_gives_empty_output() -> None:
    assert classify_outcomes([], [], None) == []


# --------------------------------------------------------------- reference cross-check (seeded)


def _random_stream(rng: random.Random) -> tuple[list[tuple[str, str, int]], list[bool], list[int]]:
    n = rng.randint(1, 40)
    events = [
        (rng.choice(["brand_a", "brand_b"]), rng.choice(["c1", "c2", "c3"]), rng.randint(1, 6))
        for _ in range(n)
    ]
    valid = [rng.random() > 0.2 for _ in range(n)]
    cuts = sorted(rng.sample(range(1, n), k=rng.randint(0, min(5, n - 1)))) if n > 1 else []
    return events, valid, [*cuts, n]


@pytest.mark.parametrize("seed", range(40))
def test_classify_outcomes_matches_independent_reference_on_random_streams(seed: int) -> None:
    events, valid, ends = _random_stream(random.Random(seed))
    entries = [entry(i, seq, brand=b, cust=c) for i, (b, c, seq) in enumerate(events)]
    for batch_ends in (None, ends):
        got = classify_outcomes(entries, valid, batch_ends)
        want = ref.reference_outcomes(events, valid, batch_ends)
        assert [o.value for o in got] == want
