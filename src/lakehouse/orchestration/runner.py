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
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

EXIT_TIMEOUT: Final = 124
KILL_GRACE_S: Final = 10.0
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


def run_streaming(
    cmd: Sequence[str],
    timeout_s: float,
    *,
    on_line: Callable[[str], None] | None = None,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
) -> RunResult:
    """Run ``cmd`` (no shell); returns :class:`RunResult`, code 124 after a timeout kill."""
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
    try:
        code = proc.wait(timeout=timeout_s)
        timed_out = False
    except subprocess.TimeoutExpired:
        timed_out = True
        _signal_group(proc, signal.SIGTERM)
        try:
            proc.wait(timeout=KILL_GRACE_S)
        except subprocess.TimeoutExpired:
            _signal_group(proc, getattr(signal, "SIGKILL", signal.SIGTERM))
            proc.wait()
        code = EXIT_TIMEOUT
    reader.join(timeout=5)
    return RunResult(EXIT_TIMEOUT if timed_out else code, tuple(tail))


def tcp_reachable(host: str, port: int, timeout_s: float = 3.0) -> bool:
    """True when a TCP connection to ``host:port`` succeeds (pre-flight, e.g. Thrift Server)."""
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except OSError:
        return False
