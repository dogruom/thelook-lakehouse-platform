#!/usr/bin/env python3
"""Package the code and run a Spark job on the local standalone cluster.

Usage (repo root, core profile running):
    python scripts/submit_job.py [--skip-build] [--timeout-seconds N] <job> [job args...]

Zips ``src/lakehouse`` into the content-addressed ``dist/lakehouse_app-<sha12>.zip`` (``--py-files``
takes ``.zip``), stages ``job_entry.py`` next to it and runs ``spark-submit`` inside the master
container. ``dist/`` is a bind mount at ``/opt/spark-apps/dist``. No credentials are passed on the
command line; the containers get their configuration from their own environment.

Pre-flight checks (best-effort guards, not locks): the master has free worker capacity and no
driver with the same job and arguments is already running. With ``--timeout-seconds`` a job that
overruns is killed INSIDE the container (killing only the host ``docker exec`` client would leave a
live driver, and the next run would be a second writer on the same checkpoint).

Exit codes: spark-submit's return code; 1 ``--skip-build`` without a matching zip; 2 master
container not running; 3 no worker capacity; 4 same driver already running; 124 timeout (driver
stop confirmed); 125 timeout where the driver stop could NOT be confirmed (check
``docker exec crm_spark_master ps -eo pid,args`` before starting another run); 130 interrupted.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
import zipfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = REPO_ROOT / "dist"
CONTAINER = "crm_spark_master"
MASTER = "spark://spark-master:7077"
MASTER_JSON_URL = "http://localhost:8181/json/"
REMOTE_DIST = "/opt/spark-apps/dist"
ZIP_PREFIX = "lakehouse_app-"
ENTRY = "job_entry.py"
MARKER_CONF = "spark.lakehouse.submit_marker"
KEEP_ZIPS = 5
MIN_FREE_CORES = 1
MIN_FREE_MEMORY_MB = 512
KILL_GRACE_SECONDS = 10.0
FIND_DRIVER_SECONDS = 15.0
PS_COMMAND = f"docker exec {CONTAINER} ps -eo pid,args"
SOURCE_SUFFIXES = frozenset({".py", ".json"})
_ZIP_DATE_TIME = (1980, 1, 1, 0, 0, 0)
_EXIT_ZIP_MISSING = 1
_EXIT_NOT_RUNNING = 2
_EXIT_NO_CAPACITY = 3
_EXIT_DUPLICATE = 4
_EXIT_TIMEOUT = 124
_EXIT_UNCONFIRMED = 125
_EXIT_INTERRUPTED = 130


# ---- packaging -------------------------------------------------------------------------------


def _collect_sources(src_dir: Path) -> list[tuple[str, bytes]]:
    """``(posix path relative to src_dir, bytes)`` of ``src/lakehouse`` ``*.py``/``*.json``."""
    package = src_dir / "lakehouse"
    items: list[tuple[str, bytes]] = []
    for path in package.rglob("*"):
        if not path.is_file() or "__pycache__" in path.parts or path.suffix not in SOURCE_SUFFIXES:
            continue
        items.append((path.relative_to(src_dir).as_posix(), path.read_bytes()))
    return sorted(items, key=lambda item: item[0])


def _digest(items: Sequence[tuple[str, bytes]]) -> str:
    sha = hashlib.sha256()
    for rel, data in items:
        sha.update(rel.encode("utf-8"))
        sha.update(b"\0")
        sha.update(data)
        sha.update(b"\0")
    return sha.hexdigest()


def source_hash(src_dir: Path) -> str:
    """SHA-256 over the sorted relative posix paths and bytes of ``src_dir/lakehouse``."""
    return _digest(_collect_sources(src_dir))


def zip_name(digest: str) -> str:
    return f"{ZIP_PREFIX}{digest[:12]}.zip"


def _replace_with_retry(tmp: Path, target: Path, attempts: int = 5) -> None:
    """``Path.replace`` that retries ``PermissionError`` (Windows: a scanner may hold the file)."""
    for attempt in range(attempts):
        try:
            tmp.replace(target)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(0.2 * (attempt + 1))


def _write_zip(items: Sequence[tuple[str, bytes]], target: Path) -> None:
    tmp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    try:
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zf:
            for rel, data in items:
                info = zipfile.ZipInfo(rel, date_time=_ZIP_DATE_TIME)
                info.create_system = 3  # same bytes on every host OS
                info.external_attr = 0o644 << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                zf.writestr(info, data)
        _replace_with_retry(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)


def _prune_zips(dist_dir: Path, keep: Path, ps_output: str | None, count: int = KEEP_ZIPS) -> None:
    """Keep the ``count`` newest zips (and always ``keep``); delete the rest, best-effort.

    ``ps_output`` is ``ps -eo pid,args`` of the master container. A zip whose name appears in it
    belongs to a running job and is never deleted; without it (``None``) nothing is deleted.
    """
    if ps_output is None:
        return
    zips = sorted(
        dist_dir.glob(f"{ZIP_PREFIX}*.zip"),
        key=lambda p: (p.stat().st_mtime, p.name),
        reverse=True,
    )
    for old in [p for p in zips if p != keep][max(count - 1, 0) :]:
        if old.name in ps_output:
            continue
        with contextlib.suppress(OSError):  # best-effort: a locked old zip is harmless
            old.unlink()


def build_app_zip(repo_root: Path, dist_dir: Path) -> Path:
    """Zip ``src/lakehouse`` into ``dist/lakehouse_app-<sha12>.zip`` (deterministic, reused)."""
    items = _collect_sources(repo_root / "src")
    if not items:
        raise FileNotFoundError(f"no sources under {repo_root / 'src' / 'lakehouse'}")
    dist_dir.mkdir(parents=True, exist_ok=True)
    target = dist_dir / zip_name(_digest(items))
    if target.exists():
        with contextlib.suppress(OSError):
            os.utime(target)  # a reused zip counts as the newest one
    else:
        _write_zip(items, target)
    return target


def stage_entry(repo_root: Path, dist_dir: Path) -> Path:
    """Copy ``job_entry.py`` into ``dist_dir`` atomically."""
    dist_dir.mkdir(parents=True, exist_ok=True)
    target = dist_dir / ENTRY
    tmp = dist_dir / f".{ENTRY}.{os.getpid()}.tmp"
    try:
        tmp.write_bytes((repo_root / "scripts" / "spark" / ENTRY).read_bytes())
        _replace_with_retry(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    return target


def existing_app_zip(repo_root: Path, dist_dir: Path) -> Path | None:
    """The zip in ``dist_dir`` that matches the current sources, or ``None``."""
    target = dist_dir / zip_name(source_hash(repo_root / "src"))
    return target if target.is_file() and (dist_dir / ENTRY).is_file() else None


# ---- command and pre-flight ------------------------------------------------------------------


def build_submit_command(
    job: str,
    job_args: Sequence[str],
    *,
    app_zip: str,
    marker: str,
    container: str = CONTAINER,
    master: str = MASTER,
) -> list[str]:
    """The ``docker exec ... spark-submit`` command line (no env passthrough, no secrets)."""
    remote = PurePosixPath(REMOTE_DIST)
    return [
        "docker",
        "exec",
        container,
        "/opt/spark/bin/spark-submit",
        "--master",
        master,
        "--deploy-mode",
        "client",
        "--conf",
        "spark.driver.host=spark-master",
        "--conf",
        "spark.cores.max=1",
        "--conf",
        "spark.executor.memory=512m",
        "--conf",
        f"{MARKER_CONF}={marker}",
        "--py-files",
        str(remote / app_zip),
        str(remote / ENTRY),
        job,
        *job_args,
    ]


def parse_worker_capacity(master_json: str) -> tuple[int, int]:
    """Summed ``(coresfree, memoryfree MB)`` of the ALIVE workers in the master's ``/json/``."""
    data = json.loads(master_json)
    cores = 0
    memory = 0
    for worker in data.get("workers", []):
        if worker.get("state") == "ALIVE":
            cores += int(worker.get("coresfree", 0))
            memory += int(worker.get("memoryfree", 0))
    return cores, memory


def _is_entry_token(token: str) -> bool:
    return token == ENTRY or token.endswith(f"/{ENTRY}")


def find_duplicate_drivers(ps_output: str, job: str, job_args: Sequence[str]) -> list[int]:
    """PIDs of processes running the same job with the same arguments.

    ``ps_output`` is ``ps -eo pid,args`` from the master container. Tokens are compared exactly
    (no regex, no substring): a process matches when its tokens contain ``job_entry.py`` followed
    immediately by ``job`` and then exactly ``job_args`` and nothing else.

    This is a best-effort guard, not a lock: two submissions racing within the same moment can
    both pass it, and a process whose arguments contain spaces is split by whitespace.
    """
    wanted = [job, *job_args]
    found: list[int] = []
    for line in ps_output.splitlines():
        tokens = line.split()
        if len(tokens) < 2 or not tokens[0].isdigit():
            continue  # header or blank line
        args = tokens[1:]
        if any(_is_entry_token(tok) and args[i + 1 :] == wanted for i, tok in enumerate(args)):
            found.append(int(tokens[0]))
    return found


# ---- inside-container process control --------------------------------------------------------


def _docker_exec(
    container: str, command: Sequence[str], timeout: float = 30
) -> subprocess.CompletedProcess[str] | None:
    """Run a command in the container; ``None`` when docker itself cannot be used."""
    try:
        return subprocess.run(
            ["docker", "exec", container, *command],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def _process_table(ps_output: str) -> list[tuple[int, int, list[str]]]:
    """``(pid, ppid, arg tokens)`` rows of ``ps -eo pid,ppid,args``."""
    rows: list[tuple[int, int, list[str]]] = []
    for line in ps_output.splitlines():
        tokens = line.split()
        if len(tokens) >= 2 and tokens[0].isdigit() and tokens[1].isdigit():
            rows.append((int(tokens[0]), int(tokens[1]), tokens[2:]))
    return rows


def find_driver_pids(ps_output: str, marker: str) -> list[int]:
    """PIDs of the JVM carrying the submit marker and all of its descendants (python driver)."""
    wanted = f"{MARKER_CONF}={marker}"
    rows = _process_table(ps_output)
    pids = {pid for pid, _, args in rows if wanted in args}
    grew = True
    while grew:
        children = {pid for pid, ppid, _ in rows if ppid in pids and pid not in pids}
        grew = bool(children)
        pids |= children
    return sorted(pids)


def kill_driver(
    marker: str,
    container: str = CONTAINER,
    *,
    grace_seconds: float = KILL_GRACE_SECONDS,
    find_seconds: float = FIND_DRIVER_SECONDS,
    poll_seconds: float = 0.5,
) -> bool:
    """Stop the driver of the submission ``marker`` inside the container; True only if confirmed.

    Finds the JVM holding the marker and its descendants with ``ps -eo pid,ppid,args`` (no shell,
    no ``pkill -f``), retrying up to ``find_seconds`` because a very early timeout may precede the
    JVM's start. It sends SIGTERM, waits up to ``grace_seconds`` and SIGKILLs the survivors. The
    result is True only when a final process listing shows none of those PIDs. Every other outcome
    (``ps``/``kill`` unusable, marker never found, survivors) prints a warning and returns False.
    """

    def snapshot() -> str | None:
        proc = _docker_exec(container, ["ps", "-eo", "pid,ppid,args"])
        return proc.stdout if proc is not None and proc.returncode == 0 else None

    def alive_among(pids: Sequence[int]) -> list[int] | None:
        table = snapshot()
        if table is None:
            return None
        present = {pid for pid, _, _ in _process_table(table)}
        return [pid for pid in pids if pid in present]

    def attempt() -> str | None:
        """Stop the driver; return the reason it could not be confirmed, or ``None``."""
        pids: list[int] = []
        find_deadline = time.monotonic() + find_seconds
        while True:
            table = snapshot()
            if table is None:
                return no_listing
            pids = find_driver_pids(table, marker)
            if pids or time.monotonic() >= find_deadline:
                break
            time.sleep(poll_seconds)
        if not pids:
            return "no process carrying the submit marker was found"
        if _docker_exec(container, ["kill", "-TERM", *map(str, pids)]) is None:
            return "cannot signal processes in the container"
        deadline = time.monotonic() + grace_seconds
        alive: list[int] | None = pids
        while alive and time.monotonic() < deadline:
            time.sleep(poll_seconds)
            alive = alive_among(alive)
        if alive:
            _docker_exec(container, ["kill", "-KILL", *map(str, alive)])
            time.sleep(poll_seconds)
            alive = alive_among(alive)
            if alive:
                return f"pids {alive} survived SIGKILL"
        return no_listing if alive is None else None

    no_listing = "cannot list processes in the container"
    reason = attempt()
    if reason is None:
        return True
    print(
        f"submit_job: warning: driver stop NOT confirmed ({reason}); check manually: {PS_COMMAND}",
        file=sys.stderr,
    )
    return False


def _stop_client(proc: subprocess.Popen[bytes]) -> None:
    """Terminate the host ``docker exec`` client (the driver itself is handled separately)."""
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def run_submit(cmd: Sequence[str], marker: str, timeout_seconds: float | None) -> int:
    """Run ``cmd`` with its stdout/stderr passed straight through (live, unchanged).

    Returns the exit code of ``cmd``. On timeout or Ctrl+C the driver is killed inside the
    container first, then the host client: 124 = timeout and stop confirmed, 125 = timeout and
    stop NOT confirmed, 130 = interrupted (the message says whether the stop was confirmed).
    """
    proc = subprocess.Popen(list(cmd))  # inherits stdout/stderr: lines appear while it runs
    interrupted = False
    try:
        return proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        print(
            f"submit_job: timeout after {timeout_seconds}s; stopping the driver in {CONTAINER}",
            file=sys.stderr,
        )
    except KeyboardInterrupt:
        interrupted = True
        print(f"submit_job: interrupted; stopping the driver in {CONTAINER}", file=sys.stderr)
    try:
        confirmed = kill_driver(marker)
    finally:
        _stop_client(proc)
    if interrupted:
        print(
            "submit_job: the driver stop was confirmed"
            if confirmed
            else "submit_job: the driver stop was NOT confirmed (see the warning above)",
            file=sys.stderr,
        )
        return _EXIT_INTERRUPTED
    return _EXIT_TIMEOUT if confirmed else _EXIT_UNCONFIRMED


# ---- main ------------------------------------------------------------------------------------


def _master_running(container: str = CONTAINER) -> bool:
    proc = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Running}}", container],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode == 0 and proc.stdout.strip() == "true"


def _warn(message: str) -> None:
    print(f"submit_job: warning: {message}", file=sys.stderr)


def _preflight(job: str, job_args: Sequence[str]) -> tuple[int, str | None]:
    """``(exit code, ps output)``: code 0 means go. Unreadable status = warn and go on.

    The ``ps -eo pid,args`` output is returned (``None`` when unavailable) so that zip pruning can
    see which zips a running job uses.
    """
    ps = _docker_exec(CONTAINER, ["ps", "-eo", "pid,args"])
    ps_output = ps.stdout if ps is not None and ps.returncode == 0 else None
    if ps_output is None:
        _warn("cannot list processes in the master container; duplicate-driver check skipped")
    else:
        duplicates = find_duplicate_drivers(ps_output, job, job_args)
        if duplicates:
            print(
                f"submit_job: the same driver ({job} with identical arguments) is already "
                f"running in {CONTAINER} (pids {duplicates}); wait for it or stop it",
                file=sys.stderr,
            )
            return _EXIT_DUPLICATE, ps_output

    state = _docker_exec(CONTAINER, ["curl", "-fsS", MASTER_JSON_URL])
    if state is None or state.returncode != 0:
        _warn("cannot read the Spark master status; capacity check skipped")
        return 0, ps_output
    try:
        cores, memory = parse_worker_capacity(state.stdout)
    except (ValueError, TypeError, AttributeError):
        _warn("unreadable Spark master status; capacity check skipped")
        return 0, ps_output
    if cores < MIN_FREE_CORES or memory < MIN_FREE_MEMORY_MB:
        print(
            f"submit_job: no worker capacity: free cores {cores} (need >= {MIN_FREE_CORES}), "
            f"free memory {memory} MB (need >= {MIN_FREE_MEMORY_MB}); a running job holds it",
            file=sys.stderr,
        )
        return _EXIT_NO_CAPACITY, ps_output
    return 0, ps_output


def _positive_number(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not a number") from None
    if number <= 0:
        raise argparse.ArgumentTypeError(f"must be > 0, got {value}")
    return number


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the code package and spark-submit a job.")
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="reuse the zip in dist/ (fails unless it matches the current sources)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=_positive_number,
        default=None,
        help="kill the driver after this many seconds (default: no limit)",
    )
    parser.add_argument("job", help="job name, for example hello_delta")
    parser.add_argument("job_args", nargs=argparse.REMAINDER, help="arguments passed to the job")
    ns = parser.parse_args(argv)

    if not _master_running():
        print(
            f"{CONTAINER} is not running; start it: docker compose --profile core up -d",
            file=sys.stderr,
        )
        return _EXIT_NOT_RUNNING

    if ns.skip_build:
        app_zip = existing_app_zip(REPO_ROOT, DIST_DIR)
        if app_zip is None:
            print(
                f"submit_job: --skip-build: no {ZIP_PREFIX}<hash>.zip for the current sources "
                f"in {DIST_DIR}; run once without --skip-build",
                file=sys.stderr,
            )
            return _EXIT_ZIP_MISSING
    else:
        app_zip = build_app_zip(REPO_ROOT, DIST_DIR)
        stage_entry(REPO_ROOT, DIST_DIR)

    code, ps_output = _preflight(ns.job, ns.job_args)
    if code:
        return code  # a refused submit changes nothing in dist/
    _prune_zips(DIST_DIR, app_zip, ps_output)

    marker = uuid.uuid4().hex
    cmd = build_submit_command(ns.job, ns.job_args, app_zip=app_zip.name, marker=marker)
    return run_submit(cmd, marker, ns.timeout_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
