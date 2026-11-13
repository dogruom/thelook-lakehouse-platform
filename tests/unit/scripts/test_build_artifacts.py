"""``scripts/cloud/build_artifacts.py``: dirty-tree refusal, wheel build, upload (moto)."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

import boto3
import pytest
from moto import mock_aws

from .conftest import load_script


if TYPE_CHECKING:
    from pathlib import Path
    from types import ModuleType

BUCKET = "crm-lakehouse-dev-123456789012"
SHA = "abcdef012345"
WHEEL = "multitenant_crm_lakehouse-0.1.0-py3-none-any.whl"


@pytest.fixture(scope="module")
def build() -> ModuleType:
    return load_script("cloud/build_artifacts")


@pytest.fixture
def patched(build: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, bool]:
    state = {"dirty": False}
    wheel = tmp_path / "dist" / WHEEL
    wheel.parent.mkdir()
    wheel.write_bytes(b"wheel-bytes")
    monkeypatch.setattr(build, "git_sha12", lambda: SHA)
    monkeypatch.setattr(build, "tree_is_dirty", lambda: state["dirty"])
    monkeypatch.setattr(build, "build_wheel", lambda _out: wheel)
    monkeypatch.setenv("LAKEHOUSE_AWS_BUCKET", BUCKET)
    return state


def test_dirty_tree_is_refused(
    build: ModuleType, patched: dict[str, bool], capsys: pytest.CaptureFixture[str]
) -> None:
    patched["dirty"] = True
    assert build.main(["--no-upload"]) == 2
    assert "uncommitted changes" in capsys.readouterr().err


def test_allow_dirty_builds_without_upload(
    build: ModuleType, patched: dict[str, bool], capsys: pytest.CaptureFixture[str]
) -> None:
    patched["dirty"] = True
    assert build.main(["--allow-dirty", "--no-upload"]) == 0
    assert WHEEL in capsys.readouterr().out


def test_upload_needs_the_bucket_variable(
    build: ModuleType, patched: dict[str, bool], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("LAKEHOUSE_AWS_BUCKET")
    assert build.main([]) == 2


def test_upload_writes_versioned_and_current_objects(
    build: ModuleType, patched: dict[str, bool], capsys: pytest.CaptureFixture[str]
) -> None:
    with mock_aws():
        s3 = boto3.client(
            "s3", region_name="eu-central-1", aws_access_key_id="x", aws_secret_access_key="x"
        )
        s3.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-central-1"}
        )
        assert build.main([], s3=s3) == 0
        keys = sorted(o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET)["Contents"])
        body = s3.get_object(Bucket=BUCKET, Key=f"artifacts/current/{WHEEL}")["Body"].read()
    assert keys == [
        f"artifacts/{SHA}/glue_entry.py",
        f"artifacts/{SHA}/{WHEEL}",
        "artifacts/current/glue_entry.py",
        f"artifacts/current/{WHEEL}",
    ]
    assert body == b"wheel-bytes"
    out = capsys.readouterr().out
    assert f"s3://{BUCKET}/artifacts/{SHA}/{WHEEL}" in out
    assert f"s3://{BUCKET}/artifacts/current/glue_entry.py" in out


def test_build_wheel_runs_pip_wheel_no_deps(
    build: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seen: list[list[str]] = []

    def fake_run(cmd: list[str], **_kw: object) -> None:
        seen.append(cmd)
        (tmp_path / "out" / WHEEL).write_bytes(b"w")

    monkeypatch.setattr(subprocess, "run", fake_run)
    wheel = build.build_wheel(tmp_path / "out")
    assert wheel.name == WHEEL
    assert seen[0][1:4] == ["-m", "pip", "wheel"]
    assert "--no-deps" in seen[0]


def test_build_failure_exits_1(
    build: ModuleType, patched: dict[str, bool], monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(_out: object) -> None:
        raise subprocess.CalledProcessError(1, "pip")

    monkeypatch.setattr(build, "build_wheel", boom)
    assert build.main(["--no-upload"]) == 1


def test_wheel_name_constant_matches_pyproject(build: ModuleType) -> None:
    run_job = load_script("cloud/run_job")
    assert run_job.WHEEL_NAME == WHEEL
    text = (build.REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'name = "multitenant-crm-lakehouse"' in text
    assert 'version = "0.1.0"' in text
