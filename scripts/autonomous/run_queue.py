"""Unattended queue runner: executes docs/TASKS.md items one at a time through ``claude -p``.

Usage (repo root)::

    python scripts/autonomous/run_queue.py [--dry-run] [--max-iterations 20] ...

Exit codes:
    0  queue complete, max iterations reached (paused), or --dry-run
    2  configuration error (docs/TASKS.md missing/invalid, claude executable not found)
    3  STOP file present (.claude/runs/STOP)
    4  nothing runnable: waiting for the owner (see docs/TASKS.md Owner approvals)
    5  usage limit resets beyond the 24 h horizon
    6  too many consecutive iteration failures
    7  run halted by a guard: run-supervisor HALT (or no verdict: fail closed), queue
       transition violation, guardrail files changed, TASKS.md unparseable, git failure
  130  interrupted (Ctrl-C)

A halt writes .claude/runs/STOP; the owner reviews it and deletes it
(``Remove-Item .claude/runs/STOP``) before restarting.

Never passes --dangerously-skip-permissions and never prints the environment.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import limits
import queue_file


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_DIR = REPO_ROOT / ".claude" / "runs"
STOP_FILE = RUN_DIR / "STOP"
LOG_FILE = RUN_DIR / "run_queue.log"
TASKS_FILE = REPO_ROOT / "docs" / "TASKS.md"
PROGRESS_FILE = REPO_ROOT / "docs" / "PROGRESS.md"

PROMPT = (
    "Run the autonomous-run skill: execute exactly one runnable item from docs/TASKS.md end to "
    "end (task id hint: {task_id}), keep docs/TASKS.md and docs/PROGRESS.md current, then stop."
)
SUPERVISOR_INSTRUCTIONS = (
    "Audit the autonomous run: read docs/TASKS.md, the Current State of docs/PROGRESS.md and "
    "the newest files in .claude/runs/ (use Read/Grep/Glob; you have no shell). The git and CI "
    "facts below were collected by the runner. "
    "Report per your checklist and end with exactly one line `VERDICT: CONTINUE` or "
    "`VERDICT: HALT — <reason>`."
)
FACT_LIMIT_CHARS = 4000
GIT_TIMEOUT_S = 60
LIMIT_WAIT_CAP_S = 86400
GUARDRAIL_PATHS = (
    ".claude/settings.json",
    ".claude/settings.local.json",
    ".claude/hooks",
    ".claude/agents",
    ".claude/skills",
    ".claude/rules",
    "CLAUDE.md",
    "scripts/autonomous",
    ".github",
)
SUPERVISOR_TIMEOUT_S = 900
VERDICT_RE = re.compile(r"^VERDICT:\s*(CONTINUE|HALT)\b")
SLEEP_CHUNK_S = 60

EXIT_OK = 0
EXIT_CONFIG = 2
EXIT_STOP = 3
EXIT_BLOCKED = 4
EXIT_LIMIT_HORIZON = 5
EXIT_FAILURES = 6
EXIT_SUPERVISOR = 7
EXIT_INTERRUPTED = 130

_ES_CONTINUOUS = 0x80000000
_ES_SYSTEM_REQUIRED = 0x00000001


# --------------------------------------------------------------------------- helpers


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _set_execution_state(flags: int) -> None:
    """Keep Windows awake (no-op elsewhere)."""
    if sys.platform != "win32":
        return
    import ctypes

    ctypes.windll.kernel32.SetThreadExecutionState(flags)


def _child_env() -> dict[str, str]:
    env = dict(os.environ)
    env["LAKEHOUSE_AUTORUN"] = "1"  # lets the guard hook protect config from the agent
    return env


def _kill_tree(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    if sys.platform == "win32":
        with contextlib.suppress(OSError, subprocess.SubprocessError):
            subprocess.run(
                ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
                capture_output=True,
                check=False,
                timeout=30,
            )
    if proc.poll() is None:
        with contextlib.suppress(OSError):
            proc.kill()


_LOG_ONLY_PREFIXES = ("running", "ok ·", "attempts ·", "supervisor check")


def write_status(state: str, *, write: bool = True) -> None:
    """Record the run state in the run log; durable states also go to PROGRESS.md.

    Routine per-iteration states (running / ok / supervisor check) stay in the log so that
    PROGRESS.md only changes when the owner needs to know (waiting, failed, halted, done).
    """
    stamp = _utc_now().strftime("%Y-%m-%dT%H:%MZ")
    line = f"{stamp} · {state}"
    print(f"[run_queue] {line}", flush=True)
    if not write:
        return
    try:
        RUN_DIR.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8", newline="\n") as log:
            log.write(json.dumps({"ts": stamp, "state": state}, ensure_ascii=False) + "\n")
    except OSError as exc:
        print(f"[run_queue] cannot append run log: {exc}", file=sys.stderr)
    if state.startswith(_LOG_ONLY_PREFIXES):
        return
    try:
        with PROGRESS_FILE.open(encoding="utf-8", newline="") as fh:
            text = fh.read()
        updated = queue_file.set_status_line(text, line)
        if updated != text:
            with PROGRESS_FILE.open("w", encoding="utf-8", newline="") as fh:
                fh.write(updated)
    except (OSError, ValueError) as exc:
        print(f"[run_queue] cannot update PROGRESS.md status: {exc}", file=sys.stderr)


def _read_tasks() -> list[queue_file.Task]:
    text = TASKS_FILE.read_text(encoding="utf-8")
    return queue_file.parse_queue(text)


def _build_command(claude: str, task_id: str) -> list[str]:
    return [
        claude,
        "-p",
        PROMPT.format(task_id=task_id),
        "--output-format",
        "stream-json",
        "--verbose",
        "--permission-mode",
        "dontAsk",
    ]


def _run_claude(cmd: list[str], out_path: Path, err_path: Path, timeout_s: int) -> tuple[int, bool]:
    """Run one iteration, streaming stdout/stderr to files. Returns (exit code, timed out)."""
    with (
        out_path.open("w", encoding="utf-8", newline="\n") as out_f,
        err_path.open("w", encoding="utf-8", newline="\n") as err_f,
    ):
        proc = subprocess.Popen(
            cmd,
            cwd=REPO_ROOT,
            env=_child_env(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        timed_out = threading.Event()

        def _expire() -> None:
            timed_out.set()
            _kill_tree(proc)

        def _drain_stderr() -> None:
            if proc.stderr is None:
                return
            for err_line in proc.stderr:
                err_f.write(err_line)
                err_f.flush()

        timer = threading.Timer(timeout_s, _expire)
        timer.daemon = True
        drain = threading.Thread(target=_drain_stderr, daemon=True)
        timer.start()
        drain.start()
        try:
            if proc.stdout is None:
                raise RuntimeError("stdout pipe not available")
            for line in proc.stdout:
                out_f.write(line)
                out_f.flush()
            code = proc.wait()
        finally:
            timer.cancel()
            _kill_tree(proc)
            drain.join(timeout=5)
    return code, timed_out.is_set()


def _read_lines(path: Path) -> list[str]:
    try:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []


def _first_line(text: str) -> str:
    for line in text.splitlines():
        if line.strip():
            return line.strip()
    return "no details"


def _parse_supervisor(stdout: str) -> tuple[str | None, str]:
    """Return (verdict or None, result text) from the supervisor's JSON output."""
    try:
        data = json.loads(stdout)
    except ValueError:
        return None, "supervisor output is not valid JSON"
    if isinstance(data, list):
        data = next(
            (item for item in reversed(data) if isinstance(item, dict) and "result" in item), None
        )
    if not isinstance(data, dict):
        return None, "supervisor output has no result object"
    result = data.get("result")
    if not isinstance(result, str):
        return None, "supervisor output has no result text"
    if data.get("is_error") is True:
        return None, f"supervisor reported an error: {_first_line(result)}"
    verdict: str | None = None
    for line in result.splitlines():
        match = VERDICT_RE.match(line.strip())
        if match is not None:
            verdict = match.group(1)
    return verdict, result


def _halt_reason(verdict: str | None, result: str) -> str:
    if verdict is None:
        return f"no verdict ({_first_line(result)})"
    for line in reversed(result.splitlines()):
        stripped = line.strip()
        if VERDICT_RE.match(stripped):
            reason = re.split(r"\s[\N{EM DASH}\N{EN DASH}-]\s?", stripped, maxsplit=1)
            if len(reason) == 2 and reason[1].strip():
                return reason[1].strip()
            break
    return _first_line(result)


def _run_tool(argv: list[str]) -> tuple[int, str]:
    """Run a fixed argv (no shell, 60 s timeout). Returns (exit code or -1, stdout)."""
    try:
        proc = subprocess.run(
            argv,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            timeout=GIT_TIMEOUT_S,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return -1, ""
    return proc.returncode, proc.stdout


def _git(*args: str) -> tuple[int, str]:
    return _run_tool([shutil.which("git") or "git", *args])


def _git_head() -> str | None:
    code, out = _git("rev-parse", "HEAD")
    head = out.strip()
    return head if code == 0 and head else None


def _guardrail_changes(run_head: str) -> tuple[list[str], str | None]:
    """Files under the guardrail paths changed since ``run_head``. Returns (files, error)."""
    paths = ["--", *GUARDRAIL_PATHS]
    code, diff_out = _git("diff", "--name-only", run_head, *paths)
    if code != 0:
        return [], "git diff failed"
    code, status_out = _git("status", "--porcelain", "--untracked-files=all", *paths)
    if code != 0:
        return [], "git status failed"
    files = [ln.strip() for ln in diff_out.splitlines() if ln.strip()]
    files += [ln[3:].strip() for ln in status_out.splitlines() if ln.strip()]
    return list(dict.fromkeys(files)), None


def _truncate(text: str) -> str:
    text = text.strip()
    if len(text) <= FACT_LIMIT_CHARS:
        return text or "(empty)"
    return text[:FACT_LIMIT_CHARS] + "\n... (truncated)"


def _supervisor_prompt(run_head: str) -> str:
    """Instruction text plus the facts collected by the runner (the supervisor has no shell)."""
    git = shutil.which("git") or "git"
    gh = shutil.which("gh")
    sections: list[tuple[str, list[str] | None]] = [
        ("git log --oneline -15", [git, "log", "--oneline", "-15"]),
        ("git status --short --branch", [git, "status", "--short", "--branch"]),
        (f"git diff --stat {run_head}..HEAD", [git, "diff", "--stat", f"{run_head}..HEAD"]),
        ("gh run list --limit 5", [gh, "run", "list", "--limit", "5"] if gh else None),
    ]
    blocks = ["Facts collected by the runner:"]
    for title, argv in sections:
        code, out = _run_tool(argv) if argv else (-1, "")
        if code != 0:
            body = "gh unavailable" if title.startswith("gh") else f"unavailable (exit {code})"
        else:
            body = _truncate(out)
        blocks.append(f"\n### {title}\n{body}")
    try:
        newest = sorted(
            (p for p in RUN_DIR.iterdir() if p.is_file()),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )[:3]
    except OSError:
        newest = []
    listing = "\n".join(str(p) for p in newest) or "(none)"
    blocks.append(f"\n### Newest files in .claude/runs/ (read them with Read)\n{listing}")
    return SUPERVISOR_INSTRUCTIONS + "\n\n" + "\n".join(blocks)


def _run_supervisor(claude: str, run_head: str) -> tuple[bool, str]:
    """Run the supervisor agent. Returns (continue?, reason). Fails closed."""
    cmd = [
        claude,
        "-p",
        _supervisor_prompt(run_head),
        "--agent",
        "run-supervisor",
        "--permission-mode",
        "dontAsk",
        "--output-format",
        "json",
    ]
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=REPO_ROOT,
            env=_child_env(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except OSError as exc:
        return False, f"cannot start supervisor: {exc}"
    try:
        stdout, stderr = proc.communicate(timeout=SUPERVISOR_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        _kill_tree(proc)
        proc.communicate()
        return False, f"supervisor timed out after {SUPERVISOR_TIMEOUT_S}s"
    finally:
        _kill_tree(proc)
    if proc.returncode != 0:
        return False, f"supervisor exit {proc.returncode}: {_first_line(stderr or stdout)}"
    verdict, result = _parse_supervisor(stdout)
    if verdict == "CONTINUE":
        return True, result
    return False, result if verdict == "HALT" else f"no verdict: {result}"


def _write_stop(text: str) -> None:
    try:
        RUN_DIR.mkdir(parents=True, exist_ok=True)
        STOP_FILE.write_text(text, encoding="utf-8")
    except OSError as exc:
        print(f"[run_queue] cannot write STOP file: {exc}", file=sys.stderr)


def _halt(reason: str, details: list[str] | None = None) -> int:
    """Write the STOP file and a halted status; returns the guard exit code."""
    _write_stop("\n".join([reason, *(details or [])]) + "\n")
    write_status(f"halted · {reason}")
    return EXIT_SUPERVISOR


def _halt_by_supervisor(claude: str, run_head: str) -> int | None:
    """Run the supervisor; return an exit code when the run must stop, else None."""
    write_status("supervisor check")
    proceed, text = _run_supervisor(claude, run_head)
    if proceed:
        return None
    _write_stop(text)
    verdict_line = next(
        (ln for ln in reversed(text.splitlines()) if VERDICT_RE.match(ln.strip())), ""
    )
    reason = _halt_reason("HALT" if verdict_line else None, text)
    write_status(f"halted · run-supervisor: {reason}")
    return EXIT_SUPERVISOR


def _sleep_interruptibly(seconds: int) -> bool:
    """Sleep in <=60 s chunks. Returns False when the STOP file appeared."""
    deadline = time.monotonic() + seconds
    while True:
        if STOP_FILE.exists():
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return True
        time.sleep(min(SLEEP_CHUNK_S, remaining))


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run docs/TASKS.md items through claude -p.",
        epilog=(
            "A STOP file left by a halt (.claude/runs/STOP) must be deleted by the owner "
            "after review (PowerShell: Remove-Item .claude\\runs\\STOP) before restarting."
        ),
    )
    parser.add_argument("--max-iterations", type=int, default=20)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--supervise-every", type=int, default=3)
    parser.add_argument("--iteration-timeout-s", type=int, default=10800)
    parser.add_argument("--max-consecutive-failures", type=int, default=3)
    parser.add_argument("--claude", default="claude")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


# --------------------------------------------------------------------------- main loop


def _dry_run(claude: str, max_attempts: int) -> int:
    if STOP_FILE.exists():
        print("halted · STOP file present")
        return EXIT_STOP
    try:
        tasks = _read_tasks()
    except (OSError, ValueError) as exc:
        print(f"cannot read docs/TASKS.md: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    state = queue_file.queue_state(tasks, max_attempts=max_attempts)
    if state == "complete":
        print("complete · queue empty")
        return EXIT_OK
    task = queue_file.next_runnable(tasks, max_attempts=max_attempts)
    if task is None:
        print("waiting for owner · see docs/TASKS.md Owner approvals")
        return EXIT_BLOCKED
    print(f"next task: {task.id} ({task.title})")
    print("would run:", subprocess.list2cmdline(_build_command(claude, task.id)))
    return EXIT_OK


@dataclass
class _RunState:
    iterations: int = 0
    failures: int = 0
    since_supervision: int = 0
    limit_wait_total_s: int = 0
    run_head: str = ""
    supervisor_pending: bool = False  # an iteration ran since the last supervisor check


def _next_task(
    max_attempts: int,
) -> tuple[queue_file.Task | None, list[queue_file.Task], int | None]:
    """Return (task, queue snapshot, None) when one is runnable, else (None, [], exit code)."""
    if STOP_FILE.exists():
        write_status("halted · STOP file present")
        return None, [], EXIT_STOP
    try:
        tasks = _read_tasks()
    except (OSError, ValueError) as exc:
        write_status(f"halted · invalid docs/TASKS.md: {exc}")
        return None, [], EXIT_CONFIG
    state = queue_file.queue_state(tasks, max_attempts=max_attempts)
    if state == "complete":
        write_status("complete · queue empty")
        return None, [], EXIT_OK
    task = queue_file.next_runnable(tasks, max_attempts=max_attempts)
    if state == "blocked" or task is None:
        write_status("waiting for owner · see docs/TASKS.md Owner approvals")
        return None, [], EXIT_BLOCKED
    return task, tasks, None


def _wait_for_limit(hit: limits.LimitHit, run: _RunState) -> int | None:
    """Sleep until the limit resets. Returns an exit code when the run must stop."""
    now = _utc_now()
    wait = limits.plan_wait(hit, now)
    kind = hit.limit_type or "unknown"
    if wait is None:
        write_status(f"halted · {kind} resets beyond 24 h")
        return EXIT_LIMIT_HORIZON
    if run.limit_wait_total_s + wait > LIMIT_WAIT_CAP_S:
        write_status("halted · usage limit not cleared within 24 h")
        return EXIT_LIMIT_HORIZON
    run.limit_wait_total_s += wait
    until = (now + timedelta(seconds=wait)).astimezone().isoformat(timespec="minutes")
    write_status(f"waiting for usage-limit reset until {until} ({kind}, source={hit.source})")
    if not _sleep_interruptibly(wait):
        write_status("halted · STOP file present")
        return EXIT_STOP
    return None


def _record_result(
    args: argparse.Namespace,
    run: _RunState,
    claude: str,
    task_id: str,
    code: int,
    timed_out: bool,
    no_progress: bool,
) -> int | None:
    """Update counters and run the supervisor when due. Returns an exit code to stop."""
    run.iterations += 1
    if code == 0 and not timed_out and not no_progress:
        run.failures = 0
        run.since_supervision += 1
        write_status(f"ok · {task_id} · exit 0")
        if run.since_supervision < args.supervise_every:
            return None
    else:
        run.failures += 1
        if code == 0 and not timed_out:
            detail = "no progress"
        else:
            detail = f"exit {code}{' (timeout)' if timed_out else ''}"
        write_status(f"failed · {task_id} · {detail}")
        if run.failures >= args.max_consecutive_failures:
            return EXIT_FAILURES
    run.since_supervision = 0
    run.supervisor_pending = False
    return _halt_by_supervisor(claude, run.run_head)


def _record_run_start(head: str) -> None:
    """Write the HEAD commit at run start for the run-supervisor (the runner keeps it too)."""
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"head": head, "started_at": _utc_now().isoformat(timespec="seconds")}
    (RUN_DIR / "run_start.json").write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _bump_picked_row(task_id: str, row: queue_file.Task, max_attempts: int) -> None:
    """Runner-owned bookkeeping: count the attempt; mark the row failed at the cap."""
    with TASKS_FILE.open(encoding="utf-8", newline="") as fh:
        text = fh.read()
    updated = queue_file.bump_attempts(text, task_id)
    attempts = row.attempts + 1
    capped = attempts >= max_attempts and row.status != "failed"
    if capped:
        note = f"{row.note} · runner: max attempts" if row.note else "runner: max attempts"
        updated = queue_file.set_task_status(updated, task_id, "failed", note=note)
    with TASKS_FILE.open("w", encoding="utf-8", newline="") as fh:
        fh.write(updated)
    suffix = " -> failed (max attempts)" if capped else ""
    write_status(f"attempts · {task_id} · {attempts}/{max_attempts}{suffix}")


class _Stop(Exception):
    """Raised inside an iteration to end the run with an exit code."""

    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


def _check_guardrails(run: _RunState) -> None:
    """Deterministic tamper check: the agent must never alter its own guardrails."""
    changed, error = _guardrail_changes(run.run_head)
    if error is not None:
        raise _Stop(_halt(f"guardrail check failed ({error}); failing closed"))
    if changed:
        raise _Stop(
            _halt(f"guardrail files changed: {changed[0]}", ["Changed since run start:", *changed])
        )


def _check_queue(
    before: list[queue_file.Task], head_before: str, task_id: str
) -> tuple[queue_file.Task, bool]:
    """Validate the queue transition. Returns (picked row after the run, no progress)."""
    try:
        after = _read_tasks()
    except (OSError, ValueError) as exc:
        raise _Stop(_halt("TASKS.md unparseable after iteration", [str(exc)])) from exc
    violations = queue_file.validate_transition(before, after)
    if violations:
        raise _Stop(_halt(f"queue transition violation: {violations[0]}", violations))
    head_after = _git_head()
    if head_after is None:
        raise _Stop(_halt("cannot read git HEAD after iteration"))
    row = next(item for item in after if item.id == task_id)  # ids verified identical
    return row, after == before and head_after == head_before


def _one_iteration(
    args: argparse.Namespace,
    run: _RunState,
    claude: str,
    task: queue_file.Task,
    before: list[queue_file.Task],
) -> None:
    """Run one claude iteration and all runner-side guards. Raises _Stop to end the run."""
    head_before = _git_head()
    if head_before is None:
        raise _Stop(_halt("cannot read git HEAD before iteration"))
    stamp = _utc_now().strftime("%Y%m%dT%H%M%SZ")
    safe_id = re.sub(r"[^A-Za-z0-9._-]", "_", task.id)
    out_path = RUN_DIR / f"{stamp}-{safe_id}.jsonl"
    err_path = out_path.with_suffix(".err")
    write_status(f"running · {task.id}")
    try:
        code, timed_out = _run_claude(
            _build_command(claude, task.id), out_path, err_path, args.iteration_timeout_s
        )
    except OSError as exc:
        write_status(f"halted · cannot start claude: {exc}")
        raise _Stop(EXIT_CONFIG) from exc
    run.supervisor_pending = True
    _check_guardrails(run)

    # Always scan: the CLI exit code on a usage limit is not verified, and a needless
    # wait is cheaper than burning retries against an active limit.
    hit = limits.detect_limit(_read_lines(out_path) + _read_lines(err_path))
    if hit is not None:
        stop = _wait_for_limit(hit, run)
        if stop is not None:
            raise _Stop(stop)
        return  # a limit wait is neither a failure nor an iteration
    run.limit_wait_total_s = 0

    row, no_progress = _check_queue(before, head_before, task.id)
    if row.status not in {"done", "blocked-owner"}:
        try:
            _bump_picked_row(task.id, row, args.max_attempts)
        except (OSError, ValueError) as exc:
            raise _Stop(_halt("cannot update Attempts in docs/TASKS.md", [str(exc)])) from exc
    stop = _record_result(args, run, claude, task.id, code, timed_out, no_progress)
    if stop is not None:
        raise _Stop(stop)


def _iterate(args: argparse.Namespace, run: _RunState, claude: str) -> int:
    """Run queue iterations until a stop condition; returns the exit code."""
    try:
        while run.iterations < args.max_iterations:
            task, before, exit_code = _next_task(args.max_attempts)
            if task is None:
                return EXIT_OK if exit_code is None else exit_code
            _one_iteration(args, run, claude, task, before)
    except _Stop as stop:
        return stop.code
    write_status("paused · max iterations reached")
    return EXIT_OK


# Exit paths on which the supervisor gets one more look (its HALT only writes STOP).
_FINAL_SUPERVISED_EXITS = frozenset({EXIT_OK, EXIT_BLOCKED, EXIT_LIMIT_HORIZON, EXIT_FAILURES})


def _loop(args: argparse.Namespace) -> int:
    claude = shutil.which(args.claude) or args.claude
    run = _RunState()
    head = _git_head()
    if head is None:
        write_status("halted · cannot read git HEAD at run start")
        return EXIT_CONFIG
    dirty, error = _guardrail_changes(head)
    if error is not None or dirty:
        # Pre-flight: uncommitted guardrail edits would be flagged as tampering after the first
        # iteration; refuse to start instead of burning one.
        detail = error or f"uncommitted guardrail files: {', '.join(dirty[:5])}"
        write_status(f"halted · pre-flight: {detail} (commit or revert them first)")
        return EXIT_CONFIG
    run.run_head = head
    _record_run_start(head)
    code = _iterate(args, run, claude)
    if run.supervisor_pending and code in _FINAL_SUPERVISED_EXITS:
        final = _halt_by_supervisor(claude, run.run_head)
        if final is not None:
            write_status(
                f"supervisor check · final check halted; STOP written for the next start "
                f"(exit {code} unchanged)"
            )
    return code


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.dry_run:
        return _dry_run(args.claude, args.max_attempts)
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    _set_execution_state(_ES_CONTINUOUS | _ES_SYSTEM_REQUIRED)
    try:
        return _loop(args)
    except KeyboardInterrupt:
        write_status("halted · interrupted")
        return EXIT_INTERRUPTED
    finally:
        _set_execution_state(_ES_CONTINUOUS)


if __name__ == "__main__":
    sys.exit(main())
