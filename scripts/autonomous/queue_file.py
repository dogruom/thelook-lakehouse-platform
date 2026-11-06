"""Parse the ``## Queue`` table of docs/TASKS.md and maintain the PROGRESS.md status line."""

from __future__ import annotations

import re
from dataclasses import dataclass
from itertools import pairwise
from typing import TYPE_CHECKING, Literal


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


VALID_STATUSES: frozenset[str] = frozenset(
    {"todo", "in_progress", "blocked-owner", "done", "failed"}
)

_QUEUE_HEADING = re.compile(r"^##\s+Queue\s*$")
_CURRENT_STATE_HEADING = re.compile(r"^##\s+Current State\b")
_ANY_HEADING = re.compile(r"^#{1,6}\s")
_SEPARATOR_CELL = re.compile(r"^:?-+:?$")
_CELL_SPLIT = re.compile(r"(?<!\\)\|")
_NO_DEPENDENCY = {"", "-", "\N{EM DASH}", "\N{EN DASH}"}
_STATUS_PREFIX = "- **Autonomous run**:"


@dataclass(frozen=True)
class Task:
    """One row of the queue table."""

    id: str
    title: str
    status: str
    depends_on: tuple[str, ...]
    attempts: int
    note: str


def _split_row(line: str) -> list[str]:
    body = line.strip()
    if body.startswith("|"):
        body = body[1:]
    if body.endswith("|") and not body.endswith("\\|"):
        body = body[:-1]
    return [cell.replace("\\|", "|").strip() for cell in _CELL_SPLIT.split(body)]


def _unquote(value: str) -> str:
    return value.strip().strip("`").strip()


def _parse_row(cells: list[str], line_no: int) -> Task:
    if len(cells) < 6:
        raise ValueError(f"queue row {line_no}: expected 6 columns, got {len(cells)}")
    task_id = _unquote(cells[0])
    if not task_id:
        raise ValueError(f"queue row {line_no}: empty task ID")
    status = _unquote(cells[2])
    if status not in VALID_STATUSES:
        raise ValueError(f"queue row {line_no} ({task_id}): unknown status {status!r}")
    deps = tuple(
        dep for dep in (_unquote(part) for part in cells[3].split(",")) if dep not in _NO_DEPENDENCY
    )
    try:
        attempts = int(cells[4])
    except ValueError as exc:
        raise ValueError(f"queue row {line_no} ({task_id}): bad attempts {cells[4]!r}") from exc
    if attempts < 0:
        raise ValueError(f"queue row {line_no} ({task_id}): negative attempts {attempts}")
    return Task(
        id=task_id,
        title=cells[1],
        status=status,
        depends_on=deps,
        attempts=attempts,
        note=" | ".join(cells[5:]),
    )


def parse_queue(text: str) -> list[Task]:
    """Return the tasks of the first table after the ``## Queue`` heading."""
    lines = text.splitlines()
    start = next((i for i, ln in enumerate(lines) if _QUEUE_HEADING.match(ln.strip())), None)
    if start is None:
        raise ValueError("heading '## Queue' not found")
    table: list[tuple[int, str]] = []
    for offset, line in enumerate(lines[start + 1 :], start=start + 2):
        stripped = line.strip()
        if stripped.startswith("|"):
            table.append((offset, stripped))
        elif table or _ANY_HEADING.match(stripped):
            break
    if not table:
        return []
    rows = table[1:]  # first row is the header
    if rows and all(_SEPARATOR_CELL.match(c) for c in _split_row(rows[0][1]) if c):
        rows = rows[1:]
    return [_parse_row(_split_row(row), line_no) for line_no, row in rows]


def next_runnable(tasks: Sequence[Task], *, max_attempts: int = 3) -> Task | None:
    """First in-progress task, else first todo task whose dependencies are all done."""
    for task in tasks:
        if task.status == "in_progress" and task.attempts < max_attempts:
            return task
    done = {task.id for task in tasks if task.status == "done"}
    for task in tasks:
        if (
            task.status == "todo"
            and task.attempts < max_attempts
            and all(dep in done for dep in task.depends_on)
        ):
            return task
    return None


def queue_state(
    tasks: Sequence[Task], *, max_attempts: int = 3
) -> Literal["runnable", "complete", "blocked"]:
    """Classify the queue: complete, runnable, or blocked (waiting on owner or failures)."""
    if all(task.status == "done" for task in tasks):
        return "complete"
    if next_runnable(tasks, max_attempts=max_attempts) is not None:
        return "runnable"
    return "blocked"


def _line_ending(line: str) -> str:
    if line.endswith("\r\n"):
        return "\r\n"
    if line.endswith("\n"):
        return "\n"
    return ""


def set_status_line(progress_text: str, line: str) -> str:
    """Set the ``- **Autonomous run**:`` bullet inside the ``## Current State`` section."""
    parts = progress_text.splitlines(keepends=True)
    heading = next(
        (i for i, ln in enumerate(parts) if _CURRENT_STATE_HEADING.match(ln.rstrip("\r\n"))),
        None,
    )
    if heading is None:
        raise ValueError("heading '## Current State' not found")
    end = len(parts)
    for i in range(heading + 1, len(parts)):
        if _ANY_HEADING.match(parts[i]):
            end = i
            break
    newline = "\r\n" if "\r\n" in progress_text else "\n"
    bullet = f"{_STATUS_PREFIX} {line}"

    for i in range(heading + 1, end):
        if parts[i].startswith(_STATUS_PREFIX):
            parts[i] = bullet + _line_ending(parts[i])
            return "".join(parts)
    for i in range(heading + 1, end):
        if parts[i].startswith("- "):
            parts.insert(i, bullet + newline)
            return "".join(parts)

    # No bullets in the section: place the bullet after the heading and its blank lines.
    if not _line_ending(parts[heading]):
        parts[heading] += newline
    pos = heading + 1
    while pos < end and not parts[pos].strip():
        pos += 1
    parts.insert(pos, bullet + newline)
    if pos == heading + 1:
        parts.insert(pos, newline)  # blank line between heading and bullet
    return "".join(parts)


# --------------------------------------------------------------------------- run-time guards

COMMIT_HASH = re.compile(r"\b[0-9a-f]{7,40}\b")
_OWNER_ONLY_STATUSES = frozenset({"blocked-owner", "done", "failed"})


def validate_transition(before: Sequence[Task], after: Sequence[Task]) -> list[str]:
    """Return human-readable violations of the queue rules between two snapshots (empty = ok).

    The agent may only move rows todo/in_progress forward; the runner owns ``Attempts`` and the
    owner owns ``blocked-owner`` / ``done`` / ``failed`` rows between runs.
    """
    before_ids = [task.id for task in before]
    after_ids = [task.id for task in after]
    if before_ids != after_ids:
        removed = [i for i in before_ids if i not in after_ids]
        added = [i for i in after_ids if i not in before_ids]
        detail = f"removed={removed} added={added}" if removed or added else "order changed"
        return [f"queue rows added, removed or reordered ({detail})"]
    violations: list[str] = []
    for old, new in zip(before, after, strict=True):
        if new.title != old.title:
            violations.append(f"{old.id}: title changed")
        if new.depends_on != old.depends_on:
            violations.append(f"{old.id}: depends_on changed")
        if new.attempts != old.attempts:
            violations.append(
                f"{old.id}: attempts changed {old.attempts} -> {new.attempts} (runner-owned)"
            )
        if old.status in _OWNER_ONLY_STATUSES and new.status != old.status:
            violations.append(
                f"{old.id}: status {old.status} -> {new.status} (only the owner may change it)"
            )
        if new.status == "done" and old.status != "done" and not COMMIT_HASH.search(new.note):
            violations.append(f"{old.id}: marked done without a commit hash in the note")
    return violations


def _queue_row_lines(parts: list[str]) -> list[int]:
    """Indexes (into ``parts``) of the data rows of the first table after ``## Queue``."""
    start = next((i for i, ln in enumerate(parts) if _QUEUE_HEADING.match(ln.strip())), None)
    if start is None:
        raise ValueError("heading '## Queue' not found")
    table: list[int] = []
    for i in range(start + 1, len(parts)):
        stripped = parts[i].strip()
        if stripped.startswith("|"):
            table.append(i)
        elif table or _ANY_HEADING.match(stripped):
            break
    rows = table[1:]  # first row is the header
    if rows and all(_SEPARATOR_CELL.match(c) for c in _split_row(parts[rows[0]]) if c):
        rows = rows[1:]
    return rows


def _cell_spans(body: str) -> list[tuple[int, int]]:
    """Raw (start, end) offsets of each cell of a table row, mirroring ``_split_row``."""
    pipes = [m.start() for m in _CELL_SPLIT.finditer(body)]
    if not pipes:
        return []
    spans = [(a + 1, b) for a, b in pairwise(pipes)]
    if body[pipes[-1] + 1 :].strip():
        spans.append((pipes[-1] + 1, len(body)))
    return spans


def _replace_span(body: str, span: tuple[int, int], new_text: str) -> str:
    start, end = span
    raw = body[start:end]
    if raw.strip():
        lead = raw[: len(raw) - len(raw.lstrip())]
        trail = raw[len(raw.rstrip()) :]
    else:
        lead = trail = " "
    return body[:start] + lead + new_text + trail + body[end:]


def _edit_row(text: str, task_id: str, edit: Callable[[str, list[tuple[int, int]]], str]) -> str:
    parts = text.splitlines(keepends=True)
    wanted = _unquote(task_id)
    for index in _queue_row_lines(parts):
        cells = _split_row(parts[index])
        if not cells or _unquote(cells[0]) != wanted:
            continue
        ending = _line_ending(parts[index])
        body = parts[index][: len(parts[index]) - len(ending)]
        spans = _cell_spans(body)
        if len(spans) < 6:
            raise ValueError(f"queue row for {task_id!r}: expected 6 columns, got {len(spans)}")
        parts[index] = edit(body, spans) + ending
        return "".join(parts)
    raise ValueError(f"task {task_id!r} not found in the queue table")


def bump_attempts(text: str, task_id: str) -> str:
    """Increment the Attempts cell of ``task_id``; every other byte is preserved."""

    def edit(body: str, spans: list[tuple[int, int]]) -> str:
        raw = body[spans[4][0] : spans[4][1]].strip()
        if not raw.isdigit():
            raise ValueError(f"task {task_id!r}: bad attempts {raw!r}")
        return _replace_span(body, spans[4], str(int(raw) + 1))

    return _edit_row(text, task_id, edit)


def set_task_status(text: str, task_id: str, status: str, note: str | None = None) -> str:
    """Set the Status cell (and the Note cell when ``note`` is given) of ``task_id``."""
    if status not in VALID_STATUSES:
        raise ValueError(f"unknown status {status!r}")

    def edit(body: str, spans: list[tuple[int, int]]) -> str:
        result = body
        if note is not None:  # edit the later cell first so earlier offsets stay valid
            clean = " ".join(note.replace("|", "\\|").splitlines())
            result = _replace_span(result, (spans[5][0], spans[-1][1]), clean)
        raw = body[spans[2][0] : spans[2][1]].strip()
        value = f"`{status}`" if raw.startswith("`") else status
        return _replace_span(result, spans[2], value)

    return _edit_row(text, task_id, edit)
