#!/usr/bin/env python3
"""Build the lakehouse wheel and publish it with ``glue_entry.py`` to the data bucket (D-103).

Usage (repo root, clean git tree)::

    python scripts/cloud/build_artifacts.py [--allow-dirty]        # build + upload (local use)
    # The one-step mode runs pip in the process that then uploads with your credentials;
    # prefer the two steps below (the deploy workflow uses them, D-110).
    python scripts/cloud/build_artifacts.py --no-upload            # build only, no AWS access
    python scripts/cloud/build_artifacts.py --upload-only          # upload what --no-upload built

1. Build: ``pip wheel . --no-deps -w dist/cloud/<sha12>`` (``sha12`` = first 12 characters of
   ``git rev-parse HEAD``), ``glue_entry.py`` copied next to it and a ``manifest.json`` with the
   SHA-256 of both. A tree with uncommitted changes to tracked files is refused unless
   ``--allow-dirty`` (the artifact folder name must identify the code that is in it). Prints the
   artifact sha.
2. Upload: ``s3://$LAKEHOUSE_AWS_BUCKET/artifacts/<sha12>/`` (a ``crm-lakehouse-*`` bucket), then a
   copy of both files to ``artifacts/current/``, which the Glue jobs' default arguments point at.
   ``--upload-only`` runs no pip: it refuses when the built files are missing or their hash does
   not match the manifest (and the entry script the repository's). CI therefore builds before it
   assumes the AWS role and only uploads afterwards, so no build dependency runs with credentials.

Credentials come only from boto3's default chain (profile, ``aws login``, role); this script never
takes an endpoint or keys, and refuses to run when ``AWS_ENDPOINT_URL*`` is set. Region:
``--region`` (default ``$AWS_REGION`` or eu-central-1). Exit codes: 0 ok, 1 build/upload failed,
2 usage or dirty tree.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from lakehouse.common.aws import is_data_bucket
from lakehouse.orchestration.cloud import DEFAULT_REGION, validate_cloud_environment


if TYPE_CHECKING:
    from collections.abc import Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
ENTRY_FILE = Path(__file__).resolve().with_name("glue_entry.py")
DIST_ROOT = REPO_ROOT / "dist" / "cloud"
ARTIFACT_PREFIX = "artifacts"
MANIFEST_NAME = "manifest.json"
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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_manifest(wheel: Path, sha: str, entry: Path) -> Path:
    """Copy ``entry`` next to ``wheel`` and record both SHA-256 values; returns the manifest."""
    copy = wheel.parent / entry.name
    shutil.copyfile(entry, copy)
    manifest = wheel.parent / MANIFEST_NAME
    files = {wheel.name: _sha256(wheel), copy.name: _sha256(copy)}
    manifest.write_text(json.dumps({"git_sha12": sha, "files": files}, indent=2), encoding="utf-8")
    return manifest


def verify_built(out_dir: Path, sha: str, entry: Path) -> tuple[Path, Path]:
    """The built wheel and entry script of ``out_dir``; ``RuntimeError`` when missing or altered.

    The wheel must match the manifest; the entry script must match the manifest and the
    repository's ``entry`` file. Nothing is built or installed here.
    """
    manifest_path = out_dir / MANIFEST_NAME
    if not manifest_path.is_file():
        raise RuntimeError(f"no build in {out_dir} (run with --no-upload first)")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        recorded: dict[str, str] = manifest["files"]
        built_sha = manifest["git_sha12"]
    except (ValueError, KeyError, TypeError):
        raise RuntimeError(f"unreadable manifest {manifest_path}") from None
    if built_sha != sha:
        raise RuntimeError(f"build is for {built_sha}, HEAD is {sha}; rebuild")
    wheels = sorted(out_dir.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"expected exactly one wheel in {out_dir}, found {len(wheels)}")
    built_entry = out_dir / entry.name
    if not built_entry.is_file():
        raise RuntimeError(f"{built_entry} is missing; rebuild")
    for path in (wheels[0], built_entry):
        if recorded.get(path.name) != _sha256(path):
            raise RuntimeError(f"{path.name} does not match the manifest hash; rebuild")
    if _sha256(built_entry) != _sha256(entry):
        raise RuntimeError(f"{built_entry.name} differs from {entry}; rebuild")
    return wheels[0], built_entry


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


def _build_or_verify(ns: argparse.Namespace, sha: str) -> tuple[Path, Path]:
    """``(wheel, entry)`` to upload: verified from an earlier build, or built now."""
    if ns.upload_only:
        return verify_built(DIST_ROOT / sha, sha, ENTRY_FILE)
    wheel = build_wheel(DIST_ROOT / sha)
    if ns.no_upload:
        write_manifest(wheel, sha, ENTRY_FILE)
    return wheel, ENTRY_FILE


def _config_error(ns: argparse.Namespace, bucket: str, s3: Any) -> str | None:
    """First upload-configuration problem (message), else ``None``; builds need none."""
    if ns.no_upload:
        return None
    if not is_data_bucket(bucket):
        return "set LAKEHOUSE_AWS_BUCKET to the crm-lakehouse-* data bucket name"
    if s3 is None:  # a real client will be built from the environment
        try:
            validate_cloud_environment(os.environ)
        except ValueError as exc:
            return str(exc)
    return None


def main(argv: Sequence[str] | None = None, *, s3: Any = None) -> int:
    parser = argparse.ArgumentParser(description="Build the wheel and upload the Glue artifacts.")
    parser.add_argument("--allow-dirty", action="store_true", help="build despite local changes")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--no-upload", action="store_true", help="build only (no AWS access)")
    mode.add_argument(
        "--upload-only",
        action="store_true",
        help="upload the files --no-upload built (no pip); refused when missing or altered",
    )
    parser.add_argument(
        "--region", default=os.environ.get("AWS_REGION") or DEFAULT_REGION, help="AWS region"
    )
    ns = parser.parse_args(argv)

    bucket = os.environ.get("LAKEHOUSE_AWS_BUCKET", "")
    problem = _config_error(ns, bucket, s3)
    if problem:
        print(f"build_artifacts: {problem}", file=sys.stderr)
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
        wheel, entry = _build_or_verify(ns, sha)
    except (subprocess.CalledProcessError, RuntimeError, OSError) as exc:
        print(f"build_artifacts: build failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return _EXIT_FAILED
    if not ns.upload_only:
        print(f"built {wheel}")
    if ns.no_upload:
        print(f"artifact sha: {sha}")
        return 0
    try:
        if s3 is None:
            import boto3

            s3 = boto3.client("s3", region_name=ns.region)  # default credential chain only
        uris = upload_artifacts(s3, bucket, sha, wheel, entry)
    except Exception as exc:  # botocore errors carry no secrets; report type and message
        print(f"build_artifacts: upload failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return _EXIT_FAILED
    print(f"artifact sha: {sha}")
    for uri in uris:
        print(uri)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
