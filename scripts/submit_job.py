#!/usr/bin/env python3
"""Package the code and run a Spark job on the local standalone cluster.

Usage (repo root, core profile running):
    python scripts/submit_job.py [--skip-build] <job> [job args...]

Builds a wheel, ships it as ``dist/lakehouse_app.zip`` (a wheel is a zip; Spark's ``--py-files``
takes ``.zip``) and runs ``spark-submit`` inside the master container. ``dist/`` is a bind mount
at ``/opt/spark-apps/dist``. No credentials are passed on the command line; the containers get
their configuration from their own environment.

Exit codes: spark-submit's return code, 2 when the master container is not running.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = REPO_ROOT / "dist"
CONTAINER = "crm_spark_master"
MASTER = "spark://spark-master:7077"
REMOTE_DIST = "/opt/spark-apps/dist"
APP_ZIP = "lakehouse_app.zip"
ENTRY = "job_entry.py"
_EXIT_NOT_RUNNING = 2


def find_wheel(dist_dir: Path) -> Path:
    """Return the single wheel in ``dist_dir``; never guess between several."""
    wheels = sorted(dist_dir.glob("*.whl"))
    if not wheels:
        raise LookupError(f"no wheel found in {dist_dir}")
    if len(wheels) > 1:
        names = ", ".join(w.name for w in wheels)
        raise LookupError(f"more than one wheel in {dist_dir}: {names}")
    return wheels[0]


def build_submit_command(
    job: str,
    job_args: Sequence[str],
    *,
    container: str = CONTAINER,
    master: str = MASTER,
    app_zip: str = APP_ZIP,
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
        "--py-files",
        str(remote / app_zip),
        str(remote / ENTRY),
        job,
        *job_args,
    ]


def stale_modules(wheel_members: Iterable[str], source_modules: Iterable[str]) -> list[str]:
    """Wheel ``lakehouse/**/*.py`` members that no longer exist in the source tree.

    Both inputs are posix paths relative to ``src`` (for example ``lakehouse/common/spark.py``).
    """
    present = set(source_modules)
    return sorted(
        member
        for member in set(wheel_members)
        if member.startswith("lakehouse/") and member.endswith(".py") and member not in present
    )


def _source_modules(repo_root: Path) -> list[str]:
    src = repo_root / "src"
    return [p.relative_to(src).as_posix() for p in (src / "lakehouse").rglob("*.py")]


def build_artifacts(repo_root: Path, dist_dir: Path) -> Path:
    """Build the wheel and stage ``lakehouse_app.zip`` + ``job_entry.py`` in ``dist_dir``."""
    dist_dir.mkdir(exist_ok=True)
    for stale in (*dist_dir.glob("*.whl"), dist_dir / APP_ZIP, dist_dir / ENTRY):
        stale.unlink(missing_ok=True)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            ".",
            "--no-deps",
            "--wheel-dir",
            str(dist_dir),
            "-q",
        ],
        check=True,
        cwd=repo_root,
    )
    wheel = find_wheel(dist_dir)
    with zipfile.ZipFile(wheel) as zf:
        stale = stale_modules(zf.namelist(), _source_modules(repo_root))
    if stale:
        raise RuntimeError(
            f"wheel contains modules that no longer exist in src: {', '.join(stale)}. "
            "A stale build/ directory is the likely cause; delete build/ manually and retry."
        )
    app_zip = dist_dir / APP_ZIP
    shutil.copyfile(wheel, app_zip)
    shutil.copyfile(repo_root / "scripts" / "spark" / ENTRY, dist_dir / ENTRY)
    return app_zip


def _master_running(container: str = CONTAINER) -> bool:
    proc = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Running}}", container],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode == 0 and proc.stdout.strip() == "true"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the code package and spark-submit a job.")
    parser.add_argument("--skip-build", action="store_true", help="reuse the artefacts in dist/")
    parser.add_argument("job", help="job name, for example hello_delta")
    parser.add_argument("job_args", nargs=argparse.REMAINDER, help="arguments passed to the job")
    ns = parser.parse_args(argv)

    if not _master_running():
        print(
            f"{CONTAINER} is not running; start it: docker compose --profile core up -d",
            file=sys.stderr,
        )
        return _EXIT_NOT_RUNNING
    if not ns.skip_build:
        build_artifacts(REPO_ROOT, DIST_DIR)
    return subprocess.run(build_submit_command(ns.job, ns.job_args), check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
