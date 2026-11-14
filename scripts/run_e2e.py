#!/usr/bin/env python3
"""One-command end-to-end run: generate -> produce/land -> Bronze -> DLQ -> Silver -> reconcile -> dbt.

Usage (repo root, ``core`` and ``sql`` profiles running, host venv active)::

    python scripts/run_e2e.py [--seed N] [--skip-generate] [--from-step NAME] [--dry-run]

Every step is a subprocess of an existing, separately documented command (generator CLI,
``scripts/submit_job.py``, ``dbt build``); this script only sequences them, prints one status line
per step and stops at the first non-zero exit (exit code = that step's code). All steps are
idempotent on the shared catalog: a second run with the same seed delivers the same messages and
files again, and the pipeline counts them as duplicates (at-least-once delivery + idempotent
outcome; not an exactly-once claim). ``--skip-generate`` re-runs only the processing steps; ``land`` writes to S3 from the host and needs
S3 credentials in the shell (``--skip-land`` otherwise).
W4.D24.1. Airflow runs the same chain as a DAG (``dags/lakehouse_pipeline.py``, W4.D22).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import NamedTuple


REPO_ROOT = Path(__file__).resolve().parents[1]
SPARK_JOBS = (
    "bronze_stream",
    "bronze_batch",
    "dlq_gate",
    "silver_customers",
    "silver_customers_history",
    "silver_orders",
    "silver_products",
    "silver_clickstream",
    "reconcile",
)
JOB_TIMEOUT_S = 900
# dlq_gate gates only the Kafka tables by default (D-077); the file tables must be named, otherwise
# reconcile reports the orders contract rejects as missing from the DLQ (V-28).
JOB_ARGS: dict[str, tuple[str, ...]] = {
    "dlq_gate": (
        "--table", "raw_customers", "--table", "raw_clickstream",
        "--table", "raw_orders", "--table", "raw_products",
    ),
}  # fmt: skip


class Step(NamedTuple):
    name: str
    argv: tuple[str, ...]
    cwd: Path


def plan(
    *,
    python: str,
    dbt: str,
    seed: int | None,
    skip_generate: bool,
    from_step: str | None = None,
    skip_land: bool = False,
) -> list[Step]:
    """The ordered steps of one run (pure; no side effects)."""
    seed_args = ("--seed", str(seed)) if seed is not None else ()
    steps: list[Step] = []
    if not skip_generate:
        steps.append(
            Step(
                "produce",
                (
                    python,
                    "-m",
                    "lakehouse.generator",
                    "produce",
                    "--brands",
                    "3",
                    "--events",
                    "5000",
                    "--cdc-events",
                    "1000",
                    *seed_args,
                ),
                REPO_ROOT,
            )  # fmt: skip
        )
    if not skip_generate and not skip_land:
        steps.append(
            Step(
                "land",
                (python, "-m", "lakehouse.generator", "land", "--brands", "3", *seed_args),
                REPO_ROOT,
            )
        )
    for index, job in enumerate(SPARK_JOBS):
        skip_build = () if index == 0 else ("--skip-build",)
        steps.append(
            Step(
                job,
                (
                    python,
                    "scripts/submit_job.py",
                    *skip_build,
                    "--timeout-seconds",
                    str(JOB_TIMEOUT_S),
                    job,
                    *JOB_ARGS.get(job, ()),
                ),
                REPO_ROOT,
            )  # fmt: skip
        )
    steps.append(
        Step(
            "dbt_build",
            (dbt, "build", "--select", "staging", "marts", "--profiles-dir", "profiles"),
            REPO_ROOT / "dbt_project",
        )
    )
    if from_step is not None:
        names = [s.name for s in steps]
        if from_step not in names:
            raise ValueError(f"unknown step {from_step!r}; choose one of {names}")
        steps = steps[names.index(from_step) :]
    return steps


def _default_dbt() -> str:
    exe = "dbt.exe" if sys.platform == "win32" else "dbt"
    folder = "Scripts" if sys.platform == "win32" else "bin"
    return str(REPO_ROOT / ".venv-dbt" / folder / exe)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seed", type=int, default=None, help="overrides GEN_SEED")
    parser.add_argument("--skip-generate", action="store_true", help="processing steps only")
    parser.add_argument(
        "--skip-land",
        action="store_true",
        help="skip landing orders/products to S3 (needs S3 credentials in the shell)",
    )
    parser.add_argument("--from-step", default=None, help="start at this step (resume)")
    parser.add_argument("--dry-run", action="store_true", help="print the plan, run nothing")
    parser.add_argument("--dbt", default=_default_dbt(), help="dbt executable (.venv-dbt)")
    args = parser.parse_args(argv)
    try:
        steps = plan(
            python=sys.executable,
            dbt=args.dbt,
            seed=args.seed,
            skip_generate=args.skip_generate,
            from_step=args.from_step,
            skip_land=args.skip_land,
        )
    except ValueError as exc:
        parser.error(str(exc))
    if args.dry_run:
        for i, step in enumerate(steps, 1):
            print(f"{i:2d}. {step.name}: {' '.join(step.argv)}  (cwd {step.cwd.name})")
        return 0
    started = time.monotonic()
    for i, step in enumerate(steps, 1):
        t0 = time.monotonic()
        print(f"[{i}/{len(steps)}] {step.name} ...", flush=True)
        proc = subprocess.run(  # fixed argv built by plan()
            step.argv, cwd=step.cwd, capture_output=True, text=True, check=False
        )
        took = time.monotonic() - t0
        if proc.returncode != 0:
            print(f"[{i}/{len(steps)}] {step.name} FAILED rc={proc.returncode} ({took:.0f}s)")
            print("\n".join((proc.stdout + proc.stderr).splitlines()[-20:]))
            print(f"resume with: python scripts/run_e2e.py --skip-generate --from-step {step.name}")
            return proc.returncode
        print(f"[{i}/{len(steps)}] {step.name} ok ({took:.0f}s)", flush=True)
    print(f"e2e: {len(steps)} steps ok in {time.monotonic() - started:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
