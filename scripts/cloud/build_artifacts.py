#!/usr/bin/env python3
"""Build the lakehouse wheel and publish it with ``glue_entry.py`` to the data bucket (D-103).

Usage (repo root, clean git tree, AWS credentials from the default boto3 chain)::

    python scripts/cloud/build_artifacts.py [--allow-dirty] [--no-upload]

1. ``pip wheel . --no-deps -w dist/cloud/<sha12>`` (``sha12`` = first 12 characters of ``git rev-parse HEAD``);
   a tree with uncommitted changes to tracked files is refused unless ``--allow-dirty`` (the
   artifact folder name must identify the code that is in it).
2. Unless ``--no-upload``: upload the wheel and ``glue_entry.py`` to
   ``s3://$LAKEHOUSE_AWS_BUCKET/artifacts/<sha12>/`` and copy both to ``artifacts/current/``,
   which the Glue jobs' default arguments point at. Prints the S3 URIs.

Credentials come only from boto3's default chain (profile, ``aws login``, role); this script never
takes an endpoint or keys. Exit codes: 0 ok, 1 build/upload failed, 2 usage or dirty tree.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
ENTRY_FILE = Path(__file__).resolve().with_name("glue_entry.py")
DIST_ROOT = REPO_ROOT / "dist" / "cloud"
ARTIFACT_PREFIX = "artifacts"
BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\Z")
_EXIT_FAILED = 1
_EXIT_USAGE = 2


def _git(*args: str) -> str:
    proc = subprocess.run(
        ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=True, encoding="utf-8"
    )
    return proc.stdout.strip()


def git_sha12() -> str:
    """First 12 characters of HEAD (``--short`` may return more when ambiguous; CI uses ``cut``)."""
    return _git("rev-parse", "HEAD")[:12]


def tree_is_dirty() -> bool:
    """Uncommitted changes to tracked files (untracked files do not enter the version)."""
    return bool(_git("status", "--porcelain", "--untracked-files=no"))


def build_wheel(out_dir: Path) -> Path:
    """Build the wheel of the current tree into ``out_dir``; returns its path."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("*.whl"):
        old.unlink()
    cmd = [sys.executable, "-m", "pip", "wheel", ".", "--no-deps", "-w", str(out_dir)]
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)
    wheels = sorted(out_dir.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"expected exactly one wheel in {out_dir}, found {len(wheels)}")
    return wheels[0]


def artifact_keys(sha: str, wheel_name: str) -> dict[str, str]:
    """``{local role: key}`` of the versioned artifacts."""
    return {
        "wheel": f"{ARTIFACT_PREFIX}/{sha}/{wheel_name}",
        "entry": f"{ARTIFACT_PREFIX}/{sha}/{ENTRY_FILE.name}",
    }


def upload_artifacts(s3: Any, bucket: str, sha: str, wheel: Path, entry: Path) -> list[str]:
    """Upload to ``artifacts/<sha>/`` and copy to ``artifacts/current/``; returns every URI."""
    keys = artifact_keys(sha, wheel.name)
    uris: list[str] = []
    for local, key in ((wheel, keys["wheel"]), (entry, keys["entry"])):
        s3.upload_file(str(local), bucket, key)
        current = f"{ARTIFACT_PREFIX}/current/{local.name}"
        s3.copy_object(Bucket=bucket, Key=current, CopySource={"Bucket": bucket, "Key": key})
        uris += [f"s3://{bucket}/{key}", f"s3://{bucket}/{current}"]
    return uris


def main(argv: Sequence[str] | None = None, *, s3: Any = None) -> int:
    parser = argparse.ArgumentParser(description="Build the wheel and upload the Glue artifacts.")
    parser.add_argument("--allow-dirty", action="store_true", help="build despite local changes")
    parser.add_argument("--no-upload", action="store_true", help="build only")
    ns = parser.parse_args(argv)

    bucket = os.environ.get("LAKEHOUSE_AWS_BUCKET", "")
    if not ns.no_upload and not BUCKET_PATTERN.match(bucket):
        print("build_artifacts: set LAKEHOUSE_AWS_BUCKET to the data bucket name", file=sys.stderr)
        return _EXIT_USAGE
    try:
        sha = git_sha12()
        if tree_is_dirty() and not ns.allow_dirty:
            print(
                "build_artifacts: the git tree has uncommitted changes; commit them or pass "
                "--allow-dirty (the artifact would not match its sha)",
                file=sys.stderr,
            )
            return _EXIT_USAGE
        wheel = build_wheel(DIST_ROOT / sha)
    except (subprocess.CalledProcessError, RuntimeError, OSError) as exc:
        print(f"build_artifacts: build failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return _EXIT_FAILED
    print(f"built {wheel}")
    if ns.no_upload:
        return 0
    try:
        if s3 is None:
            import boto3

            s3 = boto3.client("s3")  # default credential chain only
        uris = upload_artifacts(s3, bucket, sha, wheel, ENTRY_FILE)
    except Exception as exc:  # botocore errors carry no secrets; report type and message
        print(f"build_artifacts: upload failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return _EXIT_FAILED
    print(f"artifact sha: {sha}")
    for uri in uris:
        print(uri)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
