"""Streaming runner: exit codes, tail, timeout kill, TCP pre-flight (offline, local processes)."""

from __future__ import annotations

import socket
import sys

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
