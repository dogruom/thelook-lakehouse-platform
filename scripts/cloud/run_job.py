#!/usr/bin/env python3
"""Start an AWS Glue job run, wait for it and report state, duration and estimated cost (D-103).

Usage (AWS credentials from boto3's default chain)::

    python scripts/cloud/run_job.py <job> [--arg=VALUE ...] [--args-json JSON_LIST]
                                    [--artifact-sha SHA] [--no-wait] [--timeout-seconds N]
                                    [--record]
    python scripts/cloud/run_job.py --sequence cloud [--artifact-sha SHA] [--record]

The Glue job is ``$LAKEHOUSE_GLUE_PREFIX-<job>`` (default prefix ``crm-lakehouse-dev``) in
``$AWS_REGION`` (default ``eu-central-1``). ``--artifact-sha`` points the run at
``s3://$LAKEHOUSE_AWS_BUCKET/artifacts/<sha>/<wheel>`` instead of ``artifacts/current/``;
the job's own arguments (its ``--lakehouse_args``) come from repeatable ``--arg=VALUE`` (one list
element each, PowerShell-safe: ``--arg=--table --arg=raw_orders``; write ``--arg=`` with ``=``
so a value starting with ``--`` is not read as an option) or, alternatively, ``--args-json`` (a
JSON list of strings, e.g. ``["--table","raw_orders"]``); the two are mutually exclusive.

Exit codes: 0 only when the run SUCCEEDED (or with ``--no-wait`` once it started); 1 FAILED /
STOPPED / TIMEOUT / ERROR / EXPIRED, a wait timeout or an AWS error; 2 usage. On failure Glue's
``ErrorMessage`` is printed. The estimated cost is ``max(ExecutionTime, 60 s) x workers x 1 DPU x
USD 0.44 / 3600`` (G.1X = 1 DPU per worker): an estimate, not the bill. ``--record`` appends one
row per finished run to ``docs/cloud_runs.csv``.

``--sequence cloud`` runs, stopping at the first failure: ``register_tables`` (points the Glue
catalog at the seeded Bronze tables of D-101; ``dlq_gate`` reads Bronze by catalog name and fails
without it), ``bronze_batch``, ``dlq_gate`` (all four tables), the five Silver jobs, ``reconcile``
and ``metrics_report`` (``--output-uri s3://$LAKEHOUSE_AWS_BUCKET/ops/reports``, so the report
exists before teardown; the bucket variable is therefore required for the sequence).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = REPO_ROOT / "docs" / "cloud_runs.csv"
CSV_HEADER = (
    "date",
    "job",
    "run_id",
    "state",
    "execution_seconds",
    "workers",
    "estimated_usd",
    "artifact_sha",
)
WHEEL_NAME = "multitenant_crm_lakehouse-0.1.0-py3-none-any.whl"
DEFAULT_REGION = "eu-central-1"
DEFAULT_PREFIX = "crm-lakehouse-dev"
DPU_HOUR_USD = 0.44
DPU_PER_WORKER = 1  # G.1X
MIN_BILLED_SECONDS = 60
DEFAULT_WORKERS = 2
POLL_SECONDS = 20.0
DEFAULT_WAIT_SECONDS = 3600.0
TERMINAL_STATES = frozenset({"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"})
JOB_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,62}\Z")
SHA_PATTERN = re.compile(r"^[0-9a-f]{7,40}\Z")
BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\Z")
DLQ_TABLES = ("raw_customers", "raw_clickstream", "raw_orders", "raw_products")
_SEQUENCE_JOBS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("register_tables", ()),
    ("bronze_batch", ()),
    ("dlq_gate", tuple(arg for table in DLQ_TABLES for arg in ("--table", table))),
    ("silver_customers", ()),
    ("silver_customers_history", ()),
    ("silver_orders", ()),
    ("silver_products", ()),
    ("silver_clickstream", ()),
    ("reconcile", ()),
)
_EXIT_FAILED = 1


class RunResult(NamedTuple):
    job: str
    run_id: str
    state: str
    execution_seconds: int
    workers: int
    estimated_usd: float
    error: str | None


def cloud_sequence(bucket: str) -> list[tuple[str, tuple[str, ...]]]:
    """The cloud job order; the final ``metrics_report`` writes under the data bucket's ops/."""
    report = ("metrics_report", ("--output-uri", f"s3://{bucket}/ops/reports"))
    return [*_SEQUENCE_JOBS, report]


def estimate_cost_usd(execution_seconds: float, workers: int) -> float:
    """Estimated Glue cost: billed seconds (minimum 60) x DPUs x USD 0.44 per DPU-hour."""
    billed = max(execution_seconds, MIN_BILLED_SECONDS)
    return round(billed * workers * DPU_PER_WORKER * DPU_HOUR_USD / 3600, 4)


def build_arguments(
    *, artifact_sha: str | None, bucket: str | None, args: Sequence[str] | None
) -> dict[str, str]:
    """Per-run Glue ``Arguments`` overrides (only what differs from the job defaults)."""
    overrides: dict[str, str] = {}
    if artifact_sha:
        overrides["--additional-python-modules"] = (
            f"s3://{bucket}/artifacts/{artifact_sha}/{WHEEL_NAME}"
        )
    if args:
        overrides["--lakehouse_args"] = json.dumps(list(args))
    return overrides


def parse_args_json(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError("--args-json must be a JSON list of strings") from None
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("--args-json must be a JSON list of strings")
    return value


def append_record(path: Path, result: RunResult, artifact_sha: str, *, today: str) -> None:
    """Append one CSV row (creates the file with its header when missing or empty)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        if new:
            writer.writerow(CSV_HEADER)
        writer.writerow(
            [
                today,
                result.job,
                result.run_id,
                result.state,
                result.execution_seconds,
                result.workers,
                f"{result.estimated_usd:.4f}",
                artifact_sha,
            ]
        )


def run_one(
    glue: Any,
    *,
    glue_job: str,
    job: str,
    arguments: dict[str, str],
    wait: bool,
    timeout_seconds: float,
    poll_seconds: float = POLL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> RunResult | None:
    """Start the run and (optionally) wait for a terminal state.

    Returns ``None`` for ``--no-wait`` and for a wait that ran out of time (message printed).
    """
    kwargs: dict[str, Any] = {"JobName": glue_job}
    if arguments:
        kwargs["Arguments"] = arguments
    run_id = glue.start_job_run(**kwargs)["JobRunId"]
    print(f"{job}: started run {run_id}", flush=True)
    if not wait:
        return None
    deadline = clock() + timeout_seconds
    last_state = ""
    while True:
        run = glue.get_job_run(JobName=glue_job, RunId=run_id)["JobRun"]
        state = str(run.get("JobRunState", "UNKNOWN"))
        if state != last_state:
            print(f"{job}: {state}", flush=True)
            last_state = state
        if state in TERMINAL_STATES:
            break
        if clock() >= deadline:
            print(
                f"{job}: still {state} after {timeout_seconds:.0f}s; run {run_id} is NOT stopped "
                "(the job's own timeout applies); check the Glue console",
                file=sys.stderr,
            )
            return None
        sleep(poll_seconds)
    seconds = int(run.get("ExecutionTime") or 0)
    workers = int(run.get("NumberOfWorkers") or run.get("MaxCapacity") or DEFAULT_WORKERS)
    return RunResult(
        job,
        run_id,
        state,
        seconds,
        workers,
        estimate_cost_usd(seconds, workers),
        run.get("ErrorMessage"),
    )


def _report(result: RunResult) -> None:
    print(
        f"{result.job}: run_id={result.run_id} state={result.state} "
        f"execution={result.execution_seconds}s workers={result.workers} "
        f"estimated_cost=USD {result.estimated_usd:.4f}"
    )
    if result.state != "SUCCEEDED" and result.error:
        print(f"{result.job}: ErrorMessage: {result.error}", file=sys.stderr)


def _positive(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not a number") from None
    if number <= 0:
        raise argparse.ArgumentTypeError(f"must be > 0, got {value}")
    return number


def _validate(
    parser: argparse.ArgumentParser, ns: argparse.Namespace
) -> tuple[list[str] | None, str]:
    """Cross-option checks (``parser.error`` exits 2); returns ``(job args, data bucket)``."""
    if bool(ns.job) == bool(ns.sequence):
        parser.error("give exactly one of <job> or --sequence cloud")
    if ns.job and not JOB_PATTERN.match(ns.job):
        parser.error("invalid job name")
    if ns.args_json is not None and ns.arg:
        parser.error("--arg and --args-json are mutually exclusive")
    if ns.sequence and (ns.args_json is not None or ns.arg):
        parser.error("--arg/--args-json apply to a single job, not to --sequence")
    if ns.no_wait and (ns.record or ns.sequence):
        parser.error("--no-wait cannot be combined with --record or --sequence")
    if ns.artifact_sha and not SHA_PATTERN.match(ns.artifact_sha):
        parser.error("--artifact-sha must be 7-40 lowercase hex characters")
    try:
        job_args = list(ns.arg) if ns.arg else parse_args_json(ns.args_json)
    except ValueError as exc:
        parser.error(str(exc))
    bucket = os.environ.get("LAKEHOUSE_AWS_BUCKET", "")
    if (ns.artifact_sha or ns.sequence) and not BUCKET_PATTERN.match(bucket):
        parser.error("--artifact-sha and --sequence need LAKEHOUSE_AWS_BUCKET (data bucket name)")
    return job_args, bucket


def main(
    argv: Sequence[str] | None = None,
    *,
    glue: Any = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> int:
    parser = argparse.ArgumentParser(description="Run a Glue job and report cost.")
    parser.add_argument("job", nargs="?", help="job name, for example silver_orders")
    parser.add_argument("--sequence", choices=["cloud"], help="run the whole cloud order")
    parser.add_argument(
        "--arg",
        action="append",
        metavar="VALUE",
        help="one job argument (repeatable; use --arg=--table so a leading -- is a value)",
    )
    parser.add_argument("--args-json", help="JSON list of strings, the job's own arguments")
    parser.add_argument("--artifact-sha", help="use artifacts/<sha>/ instead of artifacts/current/")
    parser.add_argument("--no-wait", action="store_true", help="start the run and return")
    parser.add_argument("--timeout-seconds", type=_positive, default=DEFAULT_WAIT_SECONDS)
    parser.add_argument("--record", action="store_true", help="append to docs/cloud_runs.csv")
    ns = parser.parse_args(argv)  # argparse errors exit with code 2 (usage)

    job_args, bucket = _validate(parser, ns)

    prefix = os.environ.get("LAKEHOUSE_GLUE_PREFIX") or DEFAULT_PREFIX
    plan = cloud_sequence(bucket) if ns.sequence else [(ns.job, tuple(job_args or ()))]
    sha_label = ns.artifact_sha or "current"
    total = 0.0
    try:
        if glue is None:
            import boto3

            glue = boto3.client("glue", region_name=os.environ.get("AWS_REGION") or DEFAULT_REGION)
        for job, args in plan:
            result = run_one(
                glue,
                glue_job=f"{prefix}-{job}",
                job=job,
                arguments=build_arguments(artifact_sha=ns.artifact_sha, bucket=bucket, args=args),
                wait=not ns.no_wait,
                timeout_seconds=ns.timeout_seconds,
                sleep=sleep,
                clock=clock,
            )
            if ns.no_wait:
                return 0
            if result is None:
                return _EXIT_FAILED
            _report(result)
            total += result.estimated_usd
            if ns.record:
                today = datetime.now(UTC).date().isoformat()
                append_record(CSV_PATH, result, sha_label, today=today)
            if result.state != "SUCCEEDED":
                return _EXIT_FAILED
    except Exception as exc:  # AWS/credential errors: type and message, never the credentials
        print(f"run_job: {type(exc).__name__}: {exc}", file=sys.stderr)
        return _EXIT_FAILED
    if len(plan) > 1:
        print(f"sequence done: {len(plan)} jobs, estimated total USD {total:.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
