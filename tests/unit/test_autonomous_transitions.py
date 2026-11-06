"""Unit tests for transition validation and row editing in scripts/autonomous/queue_file.py."""

from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "autonomous"))

from queue_file import (
    COMMIT_HASH,
    VALID_STATUSES,
    Task,
    bump_attempts,
    parse_queue,
    set_task_status,
    validate_transition,
)


HEADER = "| ID | Task | Status | Depends on | Attempts | Note |\n|---|---|---|---|---|---|\n"


def _doc(rows: list[str], *, after: str = "", eol: str = "\n") -> str:
    text = f"# Tasks\n\n## Queue\n\n{HEADER}" + "\n".join(rows) + "\n" + after
    return text.replace("\n", eol) if eol != "\n" else text


def _t(
    id_: str,
    status: str = "todo",
    deps: tuple[str, ...] = (),
    attempts: int = 0,
    note: str = "",
) -> Task:
    return Task(
        id=id_, title=f"title {id_}", status=status, depends_on=deps, attempts=attempts, note=note
    )


def _with(task: Task, **changes: object) -> Task:
    return dataclasses.replace(task, **changes)  # type: ignore[arg-type]


# --- COMMIT_HASH -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "note", ["abc1234", "commit abc1234 shipped", "0123456789abcdef0123456789abcdef01234567"]
)
def test_commit_hash_matches_valid_hashes(note: str) -> None:
    assert COMMIT_HASH.search(note)


@pytest.mark.parametrize("note", ["done", "", "ABC1234", "abc123", "xyz1234"])
def test_commit_hash_rejects_non_hashes(note: str) -> None:
    assert COMMIT_HASH.search(note) is None


# --- validate_transition: allowed --------------------------------------------------------------


def test_identical_lists_are_ok() -> None:
    before = [_t("A1"), _t("A2", deps=("A1",))]
    assert validate_transition(before, list(before)) == []


@pytest.mark.parametrize(
    ("src", "dst"),
    [
        ("todo", "in_progress"),
        ("in_progress", "todo"),
        ("todo", "blocked-owner"),
        ("in_progress", "blocked-owner"),
        ("todo", "failed"),
        ("in_progress", "failed"),
    ],
)
def test_allowed_status_transitions(src: str, dst: str) -> None:
    before = [_t("A1", src)]
    assert validate_transition(before, [_with(before[0], status=dst)]) == []


@pytest.mark.parametrize("src", ["todo", "in_progress"])
def test_done_with_commit_hash_is_allowed(src: str) -> None:
    before = [_t("A1", src)]
    after = [_with(before[0], status="done", note="shipped in abc1234")]
    assert validate_transition(before, after) == []


def test_note_edit_alone_is_allowed() -> None:
    before = [_t("A1", note="old")]
    assert validate_transition(before, [_with(before[0], note="new")]) == []


def test_unchanged_terminal_rows_are_ok() -> None:
    before = [_t("A1", "done", note="abc1234"), _t("A2", "failed"), _t("A3", "blocked-owner")]
    assert validate_transition(before, list(before)) == []


# --- validate_transition: violations -----------------------------------------------------------


def test_added_id_is_a_violation() -> None:
    before = [_t("A1")]
    assert validate_transition(before, [*before, _t("A2")])


def test_removed_id_is_a_violation() -> None:
    before = [_t("A1"), _t("A2")]
    assert validate_transition(before, before[:1])


def test_reordered_ids_are_a_violation() -> None:
    before = [_t("A1"), _t("A2")]
    assert validate_transition(before, [before[1], before[0]])


def test_title_change_is_a_violation() -> None:
    before = [_t("A1")]
    assert validate_transition(before, [_with(before[0], title="renamed")])


def test_depends_on_change_is_a_violation() -> None:
    before = [_t("A1"), _t("A2", deps=("A1",))]
    after = [before[0], _with(before[1], depends_on=())]
    assert validate_transition(before, after)


@pytest.mark.parametrize("new_attempts", [1, 5])
def test_attempts_increase_is_a_violation(new_attempts: int) -> None:
    before = [_t("A1", attempts=2)]
    assert validate_transition(before, [_with(before[0], attempts=2 + new_attempts)])


def test_attempts_decrease_is_a_violation() -> None:
    before = [_t("A1", attempts=2)]
    assert validate_transition(before, [_with(before[0], attempts=1)])


@pytest.mark.parametrize(
    ("src", "dst"),
    [(s, d) for s in ("blocked-owner", "done", "failed") for d in sorted(VALID_STATUSES) if s != d],
)
def test_terminal_status_cannot_change(src: str, dst: str) -> None:
    before = [_t("A1", src, note="abc1234")]
    after = [_with(before[0], status=dst, note="abc1234")]
    assert validate_transition(before, after)


@pytest.mark.parametrize("note", ["done", "", "ABC1234", "abc123"])
def test_done_without_valid_hash_is_a_violation(note: str) -> None:
    before = [_t("A1", "in_progress")]
    assert validate_transition(before, [_with(before[0], status="done", note=note)])


def test_already_done_row_does_not_need_a_new_hash_check() -> None:
    before = [_t("A1", "done", note="legacy")]
    assert validate_transition(before, list(before)) == []


def test_multiple_violations_yield_multiple_entries() -> None:
    before = [_t("A1", attempts=0), _t("A2", "done", note="abc1234")]
    after = [
        _with(before[0], title="x", attempts=3),
        _with(before[1], status="todo"),
    ]
    assert len(validate_transition(before, after)) >= 3


def test_violation_entries_are_strings() -> None:
    before = [_t("A1")]
    result = validate_transition(before, [_with(before[0], title="x")])
    assert all(isinstance(v, str) and v for v in result)


# --- bump_attempts -----------------------------------------------------------------------------


def test_bump_attempts_increments_only_target_row() -> None:
    rows = ["| A1 | t1 | todo | — | 0 | n1 |", "| A2 | t2 | todo | A1 | 2 | n2 |"]
    expected = _doc(["| A1 | t1 | todo | — | 0 | n1 |", "| A2 | t2 | todo | A1 | 3 | n2 |"])
    assert bump_attempts(_doc(rows), "A2") == expected


def test_bump_attempts_matches_backticked_id() -> None:
    text = _doc(["| `A1` | t | todo | — | 4 | |"])
    assert bump_attempts(text, "A1") == _doc(["| `A1` | t | todo | — | 5 | |"])


def test_bump_attempts_preserves_crlf() -> None:
    text = _doc(["| A1 | t | todo | — | 0 | |"], eol="\r\n")
    result = bump_attempts(text, "A1")
    assert result == _doc(["| A1 | t | todo | — | 1 | |"], eol="\r\n")
    assert "\n" not in result.replace("\r\n", "")


def test_bump_attempts_unknown_id_raises() -> None:
    with pytest.raises(ValueError):
        bump_attempts(_doc(["| A1 | t | todo | — | 0 | |"]), "ZZ")


def test_bump_attempts_ignores_same_id_in_later_table() -> None:
    later = "\n## Log\n\n| ID | x | y | z | Attempts | w |\n|---|---|---|---|---|---|\n"
    later += "| A1 | other | todo | — | 7 | |\n"
    text = _doc(["| A1 | t | todo | — | 0 | |"], after=later)
    expected = _doc(["| A1 | t | todo | — | 1 | |"], after=later)
    assert bump_attempts(text, "A1") == expected


def test_bump_attempts_does_not_find_id_only_present_in_later_table() -> None:
    later = "\n## Log\n\n| ID | x | y | z | Attempts | w |\n|---|---|---|---|---|---|\n"
    later += "| B9 | other | todo | — | 7 | |\n"
    with pytest.raises(ValueError):
        bump_attempts(_doc(["| A1 | t | todo | — | 0 | |"], after=later), "B9")


def test_bump_attempts_round_trips_through_parse_queue() -> None:
    text = _doc(["| A1 | t | todo | — | 1 | |", "| A2 | t | todo | — | 1 | |"])
    tasks = parse_queue(bump_attempts(text, "A1"))
    assert [t.attempts for t in tasks] == [2, 1]


# --- set_task_status ---------------------------------------------------------------------------


def test_set_task_status_with_note_replaces_status_and_note() -> None:
    text = _doc(["| A1 | t1 | todo | — | 0 | old |", "| A2 | t2 | todo | — | 0 | keep |"])
    tasks = parse_queue(set_task_status(text, "A1", "done", "shipped abc1234"))
    assert tasks[0].status == "done"
    assert tasks[0].note == "shipped abc1234"
    assert (tasks[0].title, tasks[0].attempts, tasks[0].depends_on) == ("t1", 0, ())
    assert tasks[1] == Task("A2", "t2", "todo", (), 0, "keep")


def test_set_task_status_without_note_keeps_note() -> None:
    text = _doc(["| A1 | t1 | todo | — | 2 | keep me |"])
    tasks = parse_queue(set_task_status(text, "A1", "in_progress"))
    assert tasks[0].status == "in_progress"
    assert tasks[0].note == "keep me"
    assert tasks[0].attempts == 2


def test_set_task_status_other_lines_are_untouched() -> None:
    rows = ["| A1 | t1 | todo | — | 0 | a |", "| A2 | t2 | todo | A1 | 1 | b |"]
    text = _doc(rows, after="\ntrailing prose\n")
    before_lines = text.splitlines()
    after_lines = set_task_status(text, "A1", "failed", "gave up").splitlines()
    assert len(after_lines) == len(before_lines)
    diff = [i for i, (a, b) in enumerate(zip(before_lines, after_lines, strict=True)) if a != b]
    assert len(diff) == 1
    assert "A1" in after_lines[diff[0]]


def test_set_task_status_accepts_backticked_id() -> None:
    text = _doc(["| `A1` | t | todo | — | 0 | |"])
    assert parse_queue(set_task_status(text, "A1", "blocked-owner", "need key"))[0].status == (
        "blocked-owner"
    )


@pytest.mark.parametrize("status", sorted(VALID_STATUSES))
def test_set_task_status_accepts_every_valid_status(status: str) -> None:
    text = _doc(["| A1 | t | todo | — | 0 | |"])
    assert parse_queue(set_task_status(text, "A1", status))[0].status == status


@pytest.mark.parametrize("status", ["DONE", "finished", "", "blocked_owner"])
def test_set_task_status_rejects_invalid_status(status: str) -> None:
    with pytest.raises(ValueError):
        set_task_status(_doc(["| A1 | t | todo | — | 0 | |"]), "A1", status)


def test_set_task_status_unknown_id_raises() -> None:
    with pytest.raises(ValueError):
        set_task_status(_doc(["| A1 | t | todo | — | 0 | |"]), "ZZ", "done", "abc1234")


def test_set_task_status_preserves_crlf() -> None:
    text = _doc(["| A1 | t | todo | — | 0 | |", "| A2 | t | todo | — | 0 | |"], eol="\r\n")
    result = set_task_status(text, "A1", "in_progress", "started")
    assert "\r\n" in result
    assert "\n" not in result.replace("\r\n", "")
    assert parse_queue(result)[0].status == "in_progress"


def test_set_task_status_ignores_same_id_in_later_table() -> None:
    later = "\n## Log\n\n| ID | x | y | z | Attempts | w |\n|---|---|---|---|---|---|\n"
    later += "| A1 | other | todo | — | 7 | |\n"
    text = _doc(["| A1 | t | todo | — | 0 | |"], after=later)
    result = set_task_status(text, "A1", "failed", "nope")
    assert result.endswith(later)
    assert parse_queue(result)[0].status == "failed"


def test_set_then_validate_roundtrip_is_consistent() -> None:
    text = _doc(["| A1 | t | in_progress | — | 1 | |"])
    after = set_task_status(text, "A1", "done", "abc1234")
    assert validate_transition(parse_queue(text), parse_queue(after)) == []
    bad = set_task_status(text, "A1", "done", "no hash")
    assert validate_transition(parse_queue(text), parse_queue(bad))
