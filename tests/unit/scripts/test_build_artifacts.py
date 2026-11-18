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
    for name in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_GLUE", "AWS_ENDPOINT_URL_STS"):
        monkeypatch.delenv(name, raising=False)
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


def test_foreign_bucket_is_refused(
    build: ModuleType,
    patched: dict[str, bool],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for bad in ("some-other-bucket", "lakehouse", "crm-lakehouse-"):
        monkeypatch.setenv("LAKEHOUSE_AWS_BUCKET", bad)
        assert build.main([]) == 2
        assert build.main(["--upload-only"]) == 2
    assert "crm-lakehouse-" in capsys.readouterr().err


def test_local_endpoint_variable_is_refused(
    build: ModuleType,
    patched: dict[str, bool],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://seaweedfs:9000")
    assert build.main(["--upload-only"]) == 2
    assert "AWS_ENDPOINT_URL" in capsys.readouterr().err
    assert build.main(["--no-upload"]) == 0  # a build never touches AWS


def test_s3_client_gets_the_region(
    build: ModuleType, patched: dict[str, bool], monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: dict[str, object] = {}

    def fake_client(service: str, **kwargs: object) -> object:
        seen.update(service=service, **kwargs)
        raise RuntimeError("stop before any upload")

    monkeypatch.setattr(boto3, "client", fake_client)
    assert build.main(["--region", "eu-west-1"]) == 1
    assert seen == {"service": "s3", "region_name": "eu-west-1"}


@pytest.fixture
def built(build: ModuleType, patched: dict[str, bool], tmp_path: Path, monkeypatch) -> Path:  # type: ignore[no-untyped-def]
    """A finished ``--no-upload`` build in a temp dist root; returns its directory."""
    monkeypatch.setattr(build, "DIST_ROOT", tmp_path / "root")
    out = build.DIST_ROOT / SHA
    out.mkdir(parents=True)
    wheel = out / WHEEL
    wheel.write_bytes(b"wheel-bytes")
    monkeypatch.setattr(build, "build_wheel", lambda _out: wheel)
    assert build.main(["--no-upload"]) == 0
    return out


def test_no_upload_writes_entry_copy_and_manifest(
    build: ModuleType, built: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (built / "glue_entry.py").read_bytes() == build.ENTRY_FILE.read_bytes()
    assert (built / "manifest.json").is_file()
    capsys.readouterr()
    assert build.main(["--no-upload"]) == 0
    assert f"artifact sha: {SHA}" in capsys.readouterr().out


def test_upload_only_uploads_the_built_files_without_building(
    build: ModuleType, built: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_build(_out: object) -> None:
        raise AssertionError("--upload-only must not build")

    monkeypatch.setattr(build, "build_wheel", no_build)
    with mock_aws():
        s3 = boto3.client(
            "s3", region_name="eu-central-1", aws_access_key_id="x", aws_secret_access_key="x"
        )
        s3.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-central-1"}
        )
        assert build.main(["--upload-only"], s3=s3) == 0
        body = s3.get_object(Bucket=BUCKET, Key=f"artifacts/{SHA}/{WHEEL}")["Body"].read()
    assert body == b"wheel-bytes"


def test_upload_only_refuses_without_a_build(
    build: ModuleType,
    patched: dict[str, bool],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(build, "DIST_ROOT", tmp_path / "empty")
    assert build.main(["--upload-only"], s3=object()) == 1
    assert "no build" in capsys.readouterr().err


@pytest.mark.parametrize("victim", [WHEEL, "glue_entry.py", "manifest.json"])
def test_upload_only_refuses_altered_or_missing_files(
    build: ModuleType,
    built: Path,
    victim: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    target = built / victim
    target.write_bytes(b"tampered")
    assert build.main(["--upload-only"], s3=object()) == 1  # nothing is uploaded
    assert capsys.readouterr().err
    target.unlink()
    assert build.main(["--upload-only"], s3=object()) == 1


def test_upload_only_refuses_a_build_of_another_commit(
    build: ModuleType, built: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(build, "git_sha12", lambda: "fedcba987654")
    assert build.main(["--upload-only"], s3=object()) == 1


def test_no_upload_and_upload_only_are_exclusive(build: ModuleType) -> None:
    with pytest.raises(SystemExit) as exit_info:
        build.main(["--no-upload", "--upload-only"])
    assert exit_info.value.code == 2


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
