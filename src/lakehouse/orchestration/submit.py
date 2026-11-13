"""Spark submission from the Airflow container, client mode (W4.D22.2, ADR-0010).

The Airflow scheduler container holds a Spark client (the Spark 3.5.9 distribution of the cluster
image, Java 17) and runs the driver itself; executors run on the standalone worker. The code ships
the way ``scripts/submit_job.py`` ships it from the host: ``src/lakehouse`` is zipped into a
content-addressed ``lakehouse_app-<sha12>.zip`` (same digest algorithm, so the same sources give
the same name) and passed with ``--py-files`` next to the staged ``job_entry.py``. The driver runs
in the Airflow container, so the executors must reach it: ``spark.driver.host`` is the container's
service name and ``spark.driver.bindAddress`` is ``0.0.0.0``.

Credentials and the PII key are never put on the command line; ``spark-submit`` inherits the
container environment (S3 credentials, ``PII_HMAC_KEY``, catalog settings).
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import re
import zipfile
from typing import TYPE_CHECKING, Final


if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

SPARK_SUBMIT: Final = "/opt/spark/bin/spark-submit"
MASTER: Final = "spark://spark-master:7077"
ZIP_PREFIX: Final = "lakehouse_app-"
ENTRY_NAME: Final = "job_entry.py"
MARKER_CONF: Final = "spark.lakehouse.submit_marker"
SOURCE_SUFFIXES: Final = frozenset({".py", ".json"})
DRIVER_MEMORY: Final = "768m"
EXECUTOR_MEMORY: Final = "512m"
#: Identical to ``scripts/spark/job_entry.py`` (a unit test compares the two).
ENTRY_SOURCE: Final = '''"""spark-submit primary resource: runs a Spark job by name; the code comes from --py-files."""

from __future__ import annotations

import sys

from lakehouse.common.jobs import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
'''
_ZIP_DATE_TIME = (1980, 1, 1, 0, 0, 0)
_JOB_RE = re.compile(r"^[a-z][a-z0-9_]{1,62}\Z")
_HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}\Z")


def collect_sources(src_dir: Path) -> list[tuple[str, bytes]]:
    """``(posix path relative to src_dir, bytes)`` of ``src_dir/lakehouse`` ``*.py``/``*.json``."""
    package = src_dir / "lakehouse"
    items: list[tuple[str, bytes]] = []
    for path in package.rglob("*"):
        if not path.is_file() or "__pycache__" in path.parts or path.suffix not in SOURCE_SUFFIXES:
            continue
        items.append((path.relative_to(src_dir).as_posix(), path.read_bytes()))
    return sorted(items, key=lambda item: item[0])


def digest(items: Sequence[tuple[str, bytes]]) -> str:
    """SHA-256 over the sorted relative paths and bytes (same algorithm as ``submit_job.py``)."""
    sha = hashlib.sha256()
    for rel, data in items:
        sha.update(rel.encode("utf-8"))
        sha.update(b"\0")
        sha.update(data)
        sha.update(b"\0")
    return sha.hexdigest()


def zip_name(hexdigest: str) -> str:
    return f"{ZIP_PREFIX}{hexdigest[:12]}.zip"


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
        tmp.replace(target)
    finally:
        tmp.unlink(missing_ok=True)


def stage_app(src_dir: Path, dist_dir: Path) -> tuple[Path, Path]:
    """Build (or reuse) the content-addressed zip and stage the entry file in ``dist_dir``.

    Returns ``(zip path, entry path)``. ``FileNotFoundError`` when there are no sources.
    """
    items = collect_sources(src_dir)
    if not items:
        raise FileNotFoundError(f"no sources under {src_dir / 'lakehouse'}")
    dist_dir.mkdir(parents=True, exist_ok=True)
    target = dist_dir / zip_name(digest(items))
    if target.exists():
        with contextlib.suppress(OSError):
            os.utime(target)
    else:
        _write_zip(items, target)
    entry = dist_dir / ENTRY_NAME
    if not entry.is_file() or entry.read_text(encoding="utf-8") != ENTRY_SOURCE:
        tmp = dist_dir / f".{ENTRY_NAME}.{os.getpid()}.tmp"
        try:
            tmp.write_text(ENTRY_SOURCE, encoding="utf-8", newline="\n")
            tmp.replace(entry)
        finally:
            tmp.unlink(missing_ok=True)
    return target, entry


def build_submit_command(
    job: str,
    job_args: Sequence[str],
    *,
    app_zip: str,
    entry: str,
    driver_host: str,
    marker: str,
    master: str = MASTER,
) -> list[str]:
    """The ``spark-submit`` argument list (client mode; no credentials, no shell)."""
    if not _JOB_RE.match(job):
        raise ValueError(f"invalid job name {job!r}")
    if not _HOST_RE.match(driver_host):
        raise ValueError("invalid driver host")
    return [
        SPARK_SUBMIT,
        "--master",
        master,
        "--deploy-mode",
        "client",
        "--conf",
        f"spark.driver.host={driver_host}",
        "--conf",
        "spark.driver.bindAddress=0.0.0.0",
        "--conf",
        f"spark.driver.memory={DRIVER_MEMORY}",
        "--conf",
        "spark.cores.max=1",
        "--conf",
        f"spark.executor.memory={EXECUTOR_MEMORY}",
        "--conf",
        f"{MARKER_CONF}={marker}",
        "--py-files",
        app_zip,
        entry,
        job,
        *job_args,
    ]


def dlq_rate_gate_args(brand_id: str, max_dlq_rate: float) -> list[str]:
    """Arguments of one per-brand ``dlq_rate_gate`` run."""
    return ["--brand", brand_id, "--max-dlq-rate", repr(float(max_dlq_rate))]


def dbt_build_command(
    dbt_bin: str, select: Sequence[str], *, target_path: str, log_path: str
) -> list[str]:
    """``dbt build --select <select...>`` with writable target/log dirs (the project is mounted)."""
    return [
        dbt_bin,
        "build",
        "--select",
        *select,
        "--profiles-dir",
        "profiles",
        "--target-path",
        target_path,
        "--log-path",
        log_path,
    ]
