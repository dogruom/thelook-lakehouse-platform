"""Read lakehouse Delta tables from the host without S3 credentials (integration helpers).

Host-side S3 access has no credentials, so the table directory is copied out in a one-off
``seaweedfs-init`` container (same pattern as ``conftest.purge_it_namespaces``) and read with
pyarrow. The ``add``/``remove`` actions of the Delta log name the live parquet files, so this
works for append-only Bronze and for MERGE-updated Silver alike. Only ``it_<hex8>`` namespaces
are ever read, so the session purge removes them.
"""

from __future__ import annotations

import contextlib
import json
import subprocess
import uuid
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

from .spark_jobs import REPO_ROOT


if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

DOCKER_TIMEOUT_S = 600
_AWS = 'aws --endpoint-url "http://$S3_HOST:$S3_PORT" --no-cli-pager --only-show-errors s3 cp'


def download_table(layer: str, table: str, namespace: str, dest: Path) -> None:
    """Copy ``s3://<bucket>/<layer>/<namespace>/<table>`` into ``dest`` (created if needed)."""
    dest.mkdir(parents=True, exist_ok=True)
    script = f'{_AWS} "s3://$LAKEHOUSE_BUCKET/{layer}/{namespace}/{table}" /data --recursive'
    proc = subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "core",
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "-v",
            f"{dest.resolve().as_posix()}:/data",
            "--entrypoint",
            "/bin/sh",
            "seaweedfs-init",
            "-c",
            script,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=DOCKER_TIMEOUT_S,
        check=False,
    )
    assert (
        proc.returncode == 0
    ), f"container aws failed (exit {proc.returncode}): {proc.stderr[-400:]}"


def live_parquet_files(root: Path) -> list[Path]:
    """Parquet files that the Delta log of ``root`` adds and does not remove."""
    live: dict[str, Path] = {}
    for log in sorted((root / "_delta_log").glob("*.json")):
        for line in log.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            action = json.loads(line)
            if "add" in action:
                rel = unquote(action["add"]["path"])
                live[rel] = root / rel
            elif "remove" in action:
                live.pop(unquote(action["remove"]["path"]), None)
    return list(live.values())


def read_rows(root: Path) -> list[dict[str, Any]]:
    """All live rows of a downloaded Delta table (hive partition values are merged in)."""
    import pyarrow.parquet as pq

    rows: list[dict[str, Any]] = []
    for file in live_parquet_files(root):
        part = {
            seg.split("=", 1)[0]: unquote(seg.split("=", 1)[1])
            for seg in file.relative_to(root).parts[:-1]
            if "=" in seg
        }
        for row in pq.read_table(file).to_pylist():
            rows.append({**part, **row})
    return rows


@contextlib.contextmanager
def temp_topics(servers: str, *prefixes: str, partitions: int = 3) -> Iterator[list[str]]:
    """Fresh topics ``it_<prefix>_<uuid8>`` (module-scope friendly); deleted on exit."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": servers})
    names = [f"it_{p}_{uuid.uuid4().hex[:8]}" for p in prefixes]
    created: list[str] = []
    try:
        futures = admin.create_topics(
            [NewTopic(n, num_partitions=partitions, replication_factor=1) for n in names]
        )
        for name, future in futures.items():
            future.result(timeout=30)
            created.append(name)
        yield names
    finally:
        if created:
            for future in admin.delete_topics(created, operation_timeout=30).values():
                with contextlib.suppress(Exception):  # best-effort cleanup
                    future.result(timeout=30)
