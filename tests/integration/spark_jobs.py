"""Helpers to run Spark jobs through ``scripts/submit_job.py`` and read their JSON log output.

The host-level subprocess timeout is always larger than the job's ``--timeout-seconds`` so the
wrapper normally ends a hung job and stops its driver inside the Spark master container. That only
holds while the wrapper itself survives: on Windows, killing it (``Popen.kill``) is a
``TerminateProcess`` that skips its cleanup and would orphan the driver. A :class:`StreamingJob`
started with a ``namespace`` therefore stops the job's in-container processes (found by namespace
in ``ps -eo pid,args``, no shell) before it kills the wrapper.
"""

from __future__ import annotations

import json
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest


if TYPE_CHECKING:
    from collections.abc import Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
SUBMIT = REPO_ROOT / "scripts" / "submit_job.py"
SPARK_CONTAINER = "crm_spark_master"
DEFAULT_TIMEOUT_S = 600
HOST_SLACK_S = 120  # subprocess timeout = job timeout + slack
EXIT_NO_CAPACITY = 3  # submit_job: nothing was started, safe to retry
CAPACITY_RETRIES = 12
CAPACITY_RETRY_WAIT_S = 5.0
TAIL_CHARS = 3000


def _command(args: Sequence[str], timeout_s: int) -> list[str]:
    # --skip-build / --timeout-seconds are options of submit_job and precede the job name.
    return [sys.executable, str(SUBMIT), "--timeout-seconds", str(timeout_s), *args]


def submit(args: Sequence[str], timeout_s: int = DEFAULT_TIMEOUT_S) -> tuple[int, str]:
    """Run ``submit_job.py <args>`` to completion; returns (exit code, stderr + stdout).

    Exit 3 (no worker capacity, e.g. executors of a just-killed driver still being released)
    means the job never started and is retried for a short while.
    """
    output = ""
    code = -1
    for attempt in range(CAPACITY_RETRIES):
        result = subprocess.run(
            _command(args, timeout_s),
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_s + HOST_SLACK_S,
            check=False,
        )
        # stderr (Spark log4j noise) first: the tail then ends with the job's JSON log lines,
        # including ``job.failed`` and its exception, instead of the last Spark INFO lines.
        code, output = result.returncode, result.stderr + "\n" + result.stdout
        if code != EXIT_NO_CAPACITY:
            break
        if attempt < CAPACITY_RETRIES - 1:
            time.sleep(CAPACITY_RETRY_WAIT_S)
    return code, output


def run_ok(args: Sequence[str], timeout_s: int = DEFAULT_TIMEOUT_S) -> str:
    """Like :func:`submit` but asserts exit 0 and returns the output."""
    code, output = submit(args, timeout_s)
    assert code == 0, f"exit {code}; output tail:\n{output[-TAIL_CHARS:]}"
    return output


def _records(output: str) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for line in output.splitlines():
        start = line.find("{")
        if start < 0:
            continue
        try:
            record = json.loads(line[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            found.append(record)
    return found


def log_events(output: str, event: str) -> list[dict[str, Any]]:
    """All JSON log records of ``output`` whose ``event`` equals ``event``, in order."""
    return [r for r in _records(output) if r.get("event") == event]


def metrics(output: str) -> dict[str, Any]:
    """Metrics of the (last) ``job.succeeded`` line; fails the test when there is none."""
    done = log_events(output, "job.succeeded")
    assert done, f"no job.succeeded line; output tail:\n{output[-TAIL_CHARS:]}"
    return dict(done[-1]["metrics"])


def kill_job_processes(namespace: str, *, require: bool = True) -> list[int]:
    """SIGKILL the driver processes of a job in the Spark master container, by namespace.

    Matches ``ps -eo pid,args`` lines that contain ``namespace`` and a submit/driver marker
    (``job_entry.py``, ``SparkSubmit``, ``spark-submit``). With ``require`` (default) it fails the
    test when nothing is found or docker fails; otherwise it is a best-effort cleanup.
    """
    ps = subprocess.run(
        ["docker", "exec", SPARK_CONTAINER, "ps", "-eo", "pid,args"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if ps.returncode != 0:
        assert not require, ps.stderr[-500:]
        return []
    pids: list[int] = []
    for line in ps.stdout.splitlines()[1:]:
        parts = line.split(None, 1)
        if len(parts) < 2 or not parts[0].isdigit():
            continue
        args = parts[1]
        if namespace in args and (
            "job_entry.py" in args or "SparkSubmit" in args or "spark-submit" in args
        ):
            pids.append(int(parts[0]))
    if not pids:
        assert not require, "no driver process of the job found in the Spark master container"
        return []
    kill = subprocess.run(
        ["docker", "exec", SPARK_CONTAINER, "kill", "-9", *map(str, pids)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert not require or kill.returncode == 0, kill.stderr[-500:]
    return pids


class StreamingJob:
    """A job started with Popen whose output lines are read by a background thread."""

    def __init__(self, args: Sequence[str], timeout_s: int, namespace: str | None = None) -> None:
        self._namespace = namespace
        self._lines: list[str] = []
        self._queue: queue.Queue[str | None] = queue.Queue()
        self._deadline = time.monotonic() + timeout_s + HOST_SLACK_S
        self._proc = subprocess.Popen(
            _command(args, timeout_s),
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self._thread = threading.Thread(target=self._pump, daemon=True)
        self._thread.start()

    def _pump(self) -> None:
        assert self._proc.stdout is not None
        for line in self._proc.stdout:
            self._lines.append(line)
            self._queue.put(line)
        self._queue.put(None)

    @property
    def output(self) -> str:
        return "".join(self._lines)

    def wait_for_event(self, event: str, timeout_s: float) -> dict[str, Any] | None:
        """First new log record named ``event`` (lines are consumed), or None on end/timeout."""
        end = time.monotonic() + timeout_s
        while (remaining := end - time.monotonic()) > 0:
            try:
                line = self._queue.get(timeout=min(remaining, 1.0))
            except queue.Empty:
                continue
            if line is None:
                self._queue.put(None)
                return None
            for record in _records(line):
                if record.get("event") == event:
                    return record
        return None

    def wait(self, timeout_s: float | None = None) -> tuple[int, str]:
        """Wait for the process to end; returns (exit code, full output)."""
        limit = self._deadline - time.monotonic() if timeout_s is None else timeout_s
        try:
            code = self._proc.wait(timeout=max(limit, 1.0))
        except subprocess.TimeoutExpired:
            self.close()
            pytest.fail(f"job did not end in time; output tail:\n{self.output[-TAIL_CHARS:]}")
        self._thread.join(timeout=30)
        return code, self.output

    def wait_until_done(self, timeout_s: float) -> int | None:
        """Exit code if the process ends within ``timeout_s``, else None (it keeps running)."""
        try:
            return self._proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            return None

    def close(self) -> None:
        """Stop the job if still running (safety net for failing tests).

        With a namespace the in-container driver is killed first: killing the host wrapper alone
        skips its own cleanup (``TerminateProcess`` on Windows) and would orphan the driver.
        """
        if self._proc.poll() is None:
            if self._namespace:
                kill_job_processes(self._namespace, require=False)
            self._proc.kill()
            self._proc.wait(timeout=30)


def submit_streaming(
    args: Sequence[str], timeout_s: int = DEFAULT_TIMEOUT_S, namespace: str | None = None
) -> StreamingJob:
    """Start ``submit_job.py <args>`` without blocking; see :class:`StreamingJob`.

    Pass the job's ``--namespace`` as ``namespace`` so that ``close()`` can stop its driver.
    """
    return StreamingJob(args, timeout_s, namespace)


def exact_offsets(servers: str, topic: str) -> dict[int, tuple[int, int]]:
    """``{partition: (low, high)}`` watermarks of every partition of ``topic``."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": servers,
            "group.id": f"it-offsets-{time.monotonic_ns()}",
            "enable.auto.commit": False,
        }
    )
    try:
        meta = consumer.list_topics(topic, timeout=15)
        partitions = sorted(meta.topics[topic].partitions)
        result: dict[int, tuple[int, int]] = {}
        for partition in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=15)
            result[partition] = (int(low), int(high))
        return result
    finally:
        consumer.close()
