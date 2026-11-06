"""Unit tests for scripts/autonomous/queue_file.py (docs/TASKS.md queue + status line)."""

from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "autonomous"))

import queue_file
from queue_file import (
    VALID_STATUSES,
    Task,
    next_runnable,
    parse_queue,
    queue_state,
    set_status_line,
)


HEADER = "| ID | Task | Status | Depends on | Attempts | Note |\n|---|---|---|---|---|---|\n"


def _doc(rows: list[str], *, before: str = "") -> str:
    return f"# Tasks\n\n{before}## Queue\n\n{HEADER}" + "\n".join(rows) + "\n"


def _t(
    id_: str,
    status: str = "todo",
    deps: tuple[str, ...] = (),
    attempts: int = 0,
) -> Task:
    return Task(
        id=id_, title=f"title {id_}", status=status, depends_on=deps, attempts=attempts, note=""
    )


# --- parse_queue -------------------------------------------------------------------------------


def test_valid_statuses_set() -> None:
    assert set(VALID_STATUSES) == {"todo", "in_progress", "blocked-owner", "done", "failed"}


def test_parse_basic_rows() -> None:
    text = _doc(
        [
            "| `A1` | First task | done | — | 1 | shipped |",
            "| A2 | Second task | todo | A1 | 0 | |",
            "| A3 | Third | blocked-owner | A1, A2 | 2 | needs owner |",
        ]
    )
    tasks = parse_queue(text)
    assert [t.id for t in tasks] == ["A1", "A2", "A3"]
    assert tasks[0] == Task("A1", "First task", "done", (), 1, "shipped")
    assert tasks[1].depends_on == ("A1",)
    assert tasks[1].note == ""
    assert tasks[2].depends_on == ("A1", "A2")
    assert tasks[2].attempts == 2


@pytest.mark.parametrize("dep", ["—", "-", "", "  "])
def test_no_dependency_markers(dep: str) -> None:
    tasks = parse_queue(_doc([f"| A1 | t | todo | {dep} | 0 | |"]))
    assert tasks[0].depends_on == ()


def test_backticked_ids_are_stripped() -> None:
    tasks = parse_queue(_doc(["| `A1` | t | todo | `A0`, `B0` | 0 | |"]))
    assert tasks[0].id == "A1"
    assert tasks[0].depends_on == ("A0", "B0")


def test_only_first_table_after_queue_heading_is_parsed() -> None:
    text = _doc(["| A1 | t | todo | — | 0 | |"]) + (
        "\n## Other\n\n" + HEADER + "| Z9 | other | done | — | 0 | |\n"
    )
    assert [t.id for t in parse_queue(text)] == ["A1"]


def test_tables_before_queue_heading_are_ignored() -> None:
    before = HEADER + "| Z9 | decoy | done | — | 0 | |\n\n"
    text = _doc(["| A1 | t | todo | — | 0 | |"], before=before)
    assert [t.id for t in parse_queue(text)] == ["A1"]


def test_crlf_input_parses() -> None:
    text = _doc(["| A1 | t | todo | — | 0 | n |"]).replace("\n", "\r\n")
    tasks = parse_queue(text)
    assert len(tasks) == 1
    assert tasks[0].note == "n"


def test_empty_queue_returns_empty_list() -> None:
    assert parse_queue(f"## Queue\n\n{HEADER}") == []


def test_missing_heading_raises() -> None:
    with pytest.raises(ValueError):
        parse_queue(HEADER + "| A1 | t | todo | — | 0 | |\n")


def test_unknown_status_raises() -> None:
    with pytest.raises(ValueError):
        parse_queue(_doc(["| A1 | t | wip | — | 0 | |"]))


@pytest.mark.parametrize("attempts", ["x", "1.5", "two"])
def test_non_int_attempts_raises(attempts: str) -> None:
    with pytest.raises(ValueError):
        parse_queue(_doc([f"| A1 | t | todo | — | {attempts} | |"]))


def test_task_is_frozen() -> None:
    task = _t("A1")
    with pytest.raises(dataclasses.FrozenInstanceError):
        task.status = "done"  # type: ignore[misc]


# --- next_runnable / queue_state ---------------------------------------------------------------


def test_next_runnable_prefers_in_progress_over_earlier_todo() -> None:
    tasks = [_t("A1"), _t("A2", "in_progress", attempts=1), _t("A3", "in_progress")]
    chosen = next_runnable(tasks)
    assert chosen is not None
    assert chosen.id == "A2"


def test_next_runnable_first_todo_with_done_deps() -> None:
    tasks = [_t("A1", "done"), _t("A2", deps=("A1",)), _t("A3")]
    chosen = next_runnable(tasks)
    assert chosen is not None
    assert chosen.id == "A2"


def test_next_runnable_skips_todo_with_unsatisfied_dep() -> None:
    tasks = [_t("A1", "todo", ("A2",)), _t("A2", "todo", ("A3",)), _t("A3")]
    chosen = next_runnable(tasks)
    assert chosen is not None
    assert chosen.id == "A3"


@pytest.mark.parametrize("dep_status", ["todo", "in_progress", "blocked-owner", "failed"])
def test_dep_must_be_done(dep_status: str) -> None:
    tasks = [_t("A1", dep_status, attempts=3), _t("A2", deps=("A1",))]
    assert next_runnable(tasks) is None


def test_unknown_dependency_id_is_unsatisfied() -> None:
    tasks = [_t("A1", deps=("GHOST",))]
    assert next_runnable(tasks) is None
    assert queue_state(tasks) == "blocked"


def test_attempts_exhausted_skips_task() -> None:
    tasks = [_t("A1", attempts=3), _t("A2", attempts=2)]
    chosen = next_runnable(tasks)
    assert chosen is not None
    assert chosen.id == "A2"


def test_in_progress_with_exhausted_attempts_falls_through_to_todo() -> None:
    tasks = [_t("A1", "in_progress", attempts=3), _t("A2")]
    chosen = next_runnable(tasks)
    assert chosen is not None
    assert chosen.id == "A2"


def test_max_attempts_parameter_is_honoured() -> None:
    tasks = [_t("A1", attempts=1)]
    assert next_runnable(tasks, max_attempts=1) is None
    assert next_runnable(tasks, max_attempts=2) is not None


def test_next_runnable_none_for_empty_and_terminal() -> None:
    assert next_runnable([]) is None
    assert next_runnable([_t("A1", "done"), _t("A2", "failed"), _t("A3", "blocked-owner")]) is None


def test_queue_state_complete_when_empty_or_all_done() -> None:
    assert queue_state([]) == "complete"
    assert queue_state([_t("A1", "done"), _t("A2", "done")]) == "complete"


def test_queue_state_runnable() -> None:
    assert queue_state([_t("A1", "done"), _t("A2", deps=("A1",))]) == "runnable"


def test_queue_state_blocked_by_owner_task() -> None:
    assert queue_state([_t("A1", "done"), _t("A2", "blocked-owner")]) == "blocked"


def test_queue_state_blocked_by_todo_behind_owner_task() -> None:
    tasks = [_t("A1", "blocked-owner"), _t("A2", deps=("A1",))]
    assert queue_state(tasks) == "blocked"


def test_queue_state_blocked_when_attempts_exhausted() -> None:
    assert queue_state([_t("A1", attempts=3)]) == "blocked"
    assert queue_state([_t("A1", attempts=3)], max_attempts=4) == "runnable"


def test_queue_state_failed_task_is_not_complete() -> None:
    assert queue_state([_t("A1", "done"), _t("A2", "failed")]) == "blocked"


# --- set_status_line ---------------------------------------------------------------------------

PROGRESS = (
    "# Progress\n\n"
    "## Current State\n\n"
    "- **Owner**: me\n"
    "- **Autonomous run**: old line\n"
    "- **Next**: stuff\n\n"
    "## History\n\n"
    "- **Autonomous run**: historic, must stay\n"
)


def test_replaces_existing_bullet_in_current_state() -> None:
    out = set_status_line(PROGRESS, "running A2")
    assert "- **Autonomous run**: running A2\n" in out
    assert "old line" not in out
    assert out.count("- **Autonomous run**:") == 2  # current state + untouched history


def test_does_not_touch_same_named_bullet_in_later_section() -> None:
    out = set_status_line(PROGRESS, "new")
    assert out.endswith("- **Autonomous run**: historic, must stay\n")


def test_inserts_as_first_bullet_when_missing() -> None:
    text = "# P\n\n## Current State\n\n- **Owner**: me\n- **Next**: x\n\n## Later\n\ntext\n"
    out = set_status_line(text, "idle")
    lines = out.splitlines()
    bullets = [ln for ln in lines if ln.startswith("- ")]
    assert bullets[0] == "- **Autonomous run**: idle"
    assert bullets[1:3] == ["- **Owner**: me", "- **Next**: x"]
    assert out.count("Autonomous run") == 1


def test_idempotent() -> None:
    once = set_status_line(PROGRESS, "same")
    assert set_status_line(once, "same") == once
    fresh = "## Current State\n\n- **Owner**: me\n"
    once = set_status_line(fresh, "same")
    assert set_status_line(once, "same") == once
    assert once.count("Autonomous run") == 1


def test_preserves_other_content() -> None:
    out = set_status_line(PROGRESS, "x")
    for fragment in ("# Progress", "- **Owner**: me", "- **Next**: stuff", "## History"):
        assert fragment in out


def test_status_line_missing_heading_raises() -> None:
    with pytest.raises(ValueError):
        set_status_line("# Progress\n\n- **Owner**: me\n", "x")


def test_preserves_crlf_line_endings() -> None:
    crlf = PROGRESS.replace("\n", "\r\n")
    out = set_status_line(crlf, "crlf run")
    assert "- **Autonomous run**: crlf run\r\n" in out
    assert "\n" not in out.replace("\r\n", "")
    inserted = set_status_line("## Current State\r\n\r\n- **Owner**: me\r\n", "ins")
    assert "\n" not in inserted.replace("\r\n", "")
    assert "- **Autonomous run**: ins\r\n" in inserted


def test_module_exposes_public_api() -> None:
    for name in ("VALID_STATUSES", "Task", "parse_queue", "next_runnable", "queue_state"):
        assert hasattr(queue_file, name)
