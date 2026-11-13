"""Streaming runner: exit codes, tail, timeout kill, TCP pre-flight (offline, local processes)."""

from __future__ import annotations

import signal
import socket
import sys

import pytest

from lakehouse.orchestration import runner


def test_streams_lines_and_returns_the_exit_code() -> None:
    seen: list[str] = []
    result = runner.run_streaming(
        [sys.executable, "-c", "import sys; print('a'); print('b', file=sys.stderr); sys.exit(3)"],
        30,
        on_line=seen.append,
    )
    assert result.code == 3
    assert sorted(result.tail) == ["a", "b"]
    assert sorted(seen) == ["a", "b"]
    assert not result.timed_out


def test_timeout_kills_the_process_and_returns_124() -> None:
    result = runner.run_streaming([sys.executable, "-c", "import time; time.sleep(60)"], 1)
    assert result.code == runner.EXIT_TIMEOUT
    assert result.timed_out


def test_tail_is_bounded() -> None:
    code = f"for i in range({runner.TAIL_LINES + 50}): print(i)"
    result = runner.run_streaming([sys.executable, "-c", code], 30)
    assert len(result.tail) == runner.TAIL_LINES
    assert result.tail[-1] == str(runner.TAIL_LINES + 49)


def test_tcp_reachable() -> None:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        assert runner.tcp_reachable("127.0.0.1", port, 2.0)
    assert not runner.tcp_reachable("127.0.0.1", port, 0.5)


class _Stop(Exception):
    """Raised by the stand-in 'previous' SIGTERM handler of the forwarding test."""


def test_sigterm_is_forwarded_to_the_child_and_handlers_are_restored() -> None:
    import threading
    import time

    def previous(_signum: int, _frame: object) -> None:
        raise _Stop

    old_term = signal.signal(signal.SIGTERM, previous)
    old_int = signal.getsignal(signal.SIGINT)
    started = time.monotonic()
    timer = threading.Timer(1.0, lambda: signal.raise_signal(signal.SIGTERM))
    timer.start()
    try:
        with pytest.raises(_Stop):
            runner.run_streaming([sys.executable, "-c", "import time; time.sleep(60)"], 120)
        # run_streaming reaped the child (SIGTERM to its group) instead of waiting for it
        assert time.monotonic() - started < 30
        assert signal.getsignal(signal.SIGTERM) is previous
        assert signal.getsignal(signal.SIGINT) is old_int
    finally:
        timer.cancel()
        signal.signal(signal.SIGTERM, old_term)


def test_signal_handlers_are_not_touched_outside_the_main_thread() -> None:
    import threading

    seen: list[object] = []

    def work() -> None:
        before = signal.getsignal(signal.SIGTERM)
        runner.run_streaming([sys.executable, "-c", "pass"], 30)
        seen.append(signal.getsignal(signal.SIGTERM) is before)

    thread = threading.Thread(target=work)
    thread.start()
    thread.join()
    assert seen == [True]
