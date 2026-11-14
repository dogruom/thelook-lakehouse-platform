"""Run a command with a hard timeout, streaming its output (W4.D22.2).

Used for ``spark-submit`` and ``dbt``. The child is started in its own process group (POSIX), so a
timeout stops the whole tree - JVM driver and Python driver included - instead of leaving a second
writer on a checkpoint behind. Exit code 124 means "timed out" (as in ``scripts/submit_job.py``).
"""

from __future__ import annotations

import collections
import os
import signal
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from types import FrameType

EXIT_TIMEOUT: Final = 124
KILL_GRACE_S: Final = 10.0
WAIT_SLICE_S: Final = 0.2
TAIL_LINES: Final = 400


@dataclass(frozen=True)
class RunResult:
    """Exit code and the last ``TAIL_LINES`` output lines (stdout and stderr merged)."""

    code: int
    tail: tuple[str, ...]

    @property
    def timed_out(self) -> bool:
        return self.code == EXIT_TIMEOUT


def _signal_group(proc: subprocess.Popen[str], sig: int) -> None:
    killpg = getattr(os, "killpg", None)  # POSIX only
    getpgid = getattr(os, "getpgid", None)
    try:
        if killpg is not None and getpgid is not None:
            killpg(getpgid(proc.pid), sig)
        elif sig == signal.SIGTERM:
            proc.terminate()
        else:
            proc.kill()
    except (ProcessLookupError, PermissionError, OSError):
        pass  # already gone


def _forward_signals(proc: subprocess.Popen[str]) -> Callable[[], None]:
    """On SIGTERM / SIGINT stop the child's process group, then run the previous handler.

    Airflow ends a task (timeout, clear, kill) by signalling the task process; without this the
    Spark driver would survive it and a retry would become a second writer on the same checkpoint.
    Only the main thread can install handlers; elsewhere this is a no-op. Returns the function that
    restores the previous handlers.
    """
    if threading.current_thread() is not threading.main_thread():
        return lambda: None
    previous: dict[int, Any] = {}

    def handler(signum: int, frame: FrameType | None) -> None:
        _signal_group(proc, signal.SIGTERM)
        old = previous[signum]
        if callable(old):
            old(signum, frame)
        elif old == signal.SIG_DFL:
            signal.signal(signum, signal.SIG_DFL)
            signal.raise_signal(signum)

    for sig in (signal.SIGTERM, signal.SIGINT):
        previous[sig] = signal.signal(sig, handler)

    def restore() -> None:
        for sig, old in previous.items():
            signal.signal(sig, old if old is not None else signal.SIG_DFL)

    return restore


def _stop_group(proc: subprocess.Popen[str]) -> None:
    """SIGTERM the group, then SIGKILL after the grace period; always reaps the child."""
    _signal_group(proc, signal.SIGTERM)
    try:
        proc.wait(timeout=KILL_GRACE_S)
    except subprocess.TimeoutExpired:
        _signal_group(proc, getattr(signal, "SIGKILL", signal.SIGTERM))
        proc.wait()


def run_streaming(
    cmd: Sequence[str],
    timeout_s: float,
    *,
    on_line: Callable[[str], None] | None = None,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
) -> RunResult:
    """Run ``cmd`` (no shell); returns :class:`RunResult`, code 124 after a timeout kill.

    SIGTERM / SIGINT received by the caller are forwarded to the child's process group (main
    thread only) and the previous handlers are restored on exit.
    """
    proc = subprocess.Popen(
        list(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=cwd,
        env=None if env is None else dict(env),
        start_new_session=True,
    )
    tail: collections.deque[str] = collections.deque(maxlen=TAIL_LINES)

    def pump() -> None:
        assert proc.stdout is not None
        for raw in proc.stdout:
            line = raw.rstrip("\n")
            tail.append(line)
            if on_line is not None:
                on_line(line)

    reader = threading.Thread(target=pump, daemon=True)
    reader.start()
    restore = _forward_signals(proc)
    timed_out = False
    code = 0
    try:
        # Short waits: Python signal handlers run between them (also on Windows).
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                code = proc.wait(timeout=min(WAIT_SLICE_S, max(deadline - time.monotonic(), 0.0)))
                break
            except subprocess.TimeoutExpired:
                if time.monotonic() >= deadline:
                    timed_out = True
                    _stop_group(proc)
                    break
    except BaseException:  # a forwarded signal raised in the handler: do not leave the child
        _stop_group(proc)
        raise
    finally:
        restore()
    reader.join(timeout=5)
    return RunResult(EXIT_TIMEOUT if timed_out else code, tuple(tail))


def tcp_reachable(host: str, port: int, timeout_s: float = 3.0) -> bool:
    """True when a TCP connection to ``host:port`` succeeds (pre-flight, e.g. Thrift Server)."""
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except OSError:
        return False
