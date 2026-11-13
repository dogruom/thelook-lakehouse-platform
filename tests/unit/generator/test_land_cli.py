"""``python -m lakehouse.generator land`` in-process: dry-run files, S3 target (moto), errors."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any

import boto3
import pytest
from moto import mock_aws

from lakehouse.generator import __main__ as cli


if TYPE_CHECKING:
    from pathlib import Path


SMALL = ["--brands", "2", "--batches", "2", "--orders-per-batch", "5", "--cdc-events", "200"]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if key.startswith("GEN_"):
            monkeypatch.delenv(key)


def _files(root: Path, suffix: str) -> list[Path]:
    return sorted(p for p in root.rglob(f"*{suffix}") if p.is_file())


def _orders(root: Path) -> list[bytes]:
    return [p.read_bytes() for p in _files(root, ".ndjson") if "orders" in p.parts]


def test_dry_run_writes_orders_products_and_manifests(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "landing"
    assert cli.main(["land", *SMALL, "--dry-run", "--out-dir", str(out)]) == 0
    assert "landed 4 order batches and 2 product files to" in capsys.readouterr().out
    ndjson = _files(out, ".ndjson")
    manifests = _files(out, ".manifest.json")
    assert len(ndjson) == 6 == len(manifests)  # 4 order batches + 2 product files
    assert len([p for p in ndjson if "products" in p.parts]) == 2
    assert len({p.relative_to(out).parts[1] for p in ndjson}) == 2  # brand dirs under landing/
    for data_file in ndjson:
        manifest_file = data_file.with_name(data_file.name.replace(".ndjson", ".manifest.json"))
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        assert manifest["line_count"] == data_file.read_bytes().count(b"\n")


def test_dry_run_prints_ground_truth_counts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cli.main(["land", *SMALL, "--dry-run", "--out-dir", str(tmp_path)])
    assert "orders." in capsys.readouterr().out


def test_dry_run_is_deterministic_for_a_seed(tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    for out in (a, b):
        cli.main(["land", *SMALL, "--seed", "7", "--dry-run", "--out-dir", str(out)])
    rel = [p.relative_to(a) for p in _files(a, "")]
    assert rel == [p.relative_to(b) for p in _files(b, "")]
    assert all((a / n).read_bytes() == (b / n).read_bytes() for n in rel)


def test_different_seed_changes_order_bytes(tmp_path: Path) -> None:
    """Negative: the determinism comparison can fail."""
    a, b = tmp_path / "a", tmp_path / "b"
    cli.main(["land", *SMALL, "--seed", "1", "--dry-run", "--out-dir", str(a)])
    cli.main(["land", *SMALL, "--seed", "2", "--dry-run", "--out-dir", str(b)])
    assert _orders(a) != _orders(b)


@pytest.mark.parametrize("flag", ["--batches", "--orders-per-batch", "--cdc-events"])
def test_non_positive_sizes_are_rejected(
    flag: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    argv = ["land", *SMALL, "--dry-run", "--out-dir", str(tmp_path), flag, "0"]
    with pytest.raises(SystemExit) as info:
        cli.main(argv)
    assert info.value.code == 2
    assert f"{flag} must be >= 1" in capsys.readouterr().err
    assert _files(tmp_path, "") == []


@pytest.mark.parametrize("brands", ["0", "999"])
def test_brands_out_of_range_is_rejected(brands: str, tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as info:
        cli.main(["land", "--brands", brands, "--dry-run", "--out-dir", str(tmp_path)])
    assert info.value.code == 2


def test_value_error_from_generation_exits_2(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def boom(*_a: Any, **_k: Any) -> Any:
        raise ValueError("bad plan")

    monkeypatch.setattr(cli, "generate_orders", boom)
    assert cli.main(["land", *SMALL, "--dry-run", "--out-dir", str(tmp_path)]) == 2
    assert "error: bad plan" in capsys.readouterr().err


def test_infeasible_defect_plan_exits_1(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def boom(*_a: Any, **_k: Any) -> Any:
        raise cli.InfeasibleDefectPlan("too few rows")

    monkeypatch.setattr(cli, "generate_cdc", boom)
    assert cli.main(["land", *SMALL, "--dry-run", "--out-dir", str(tmp_path)]) == 1
    assert "infeasible defect plan" in capsys.readouterr().err
    assert _files(tmp_path, "") == []


def test_s3_target_lands_objects_in_the_configured_bucket(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "lake-cli")
    with mock_aws():
        client = boto3.client(
            "s3", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x"
        )
        client.create_bucket(Bucket="lake-cli")
        monkeypatch.setattr(cli, "s3_client_from_env", lambda _endpoint: client)
        assert cli.main(["land", *SMALL]) == 0
        keys = [o["Key"] for o in client.list_objects_v2(Bucket="lake-cli")["Contents"]]
    assert len(keys) == 12  # (4 order batches + 2 product files) x (data + manifest)
    assert all(k.startswith("landing/") for k in keys)
    assert "s3://lake-cli/landing" in capsys.readouterr().out


def test_target_aws_uses_aws_client_and_aws_bucket_never_the_local_endpoint(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("LAKEHOUSE_AWS_BUCKET", "crm-lakehouse-dev-123")
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "local-bucket")
    monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
    with mock_aws():
        client = boto3.client(
            "s3", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x"
        )
        client.create_bucket(Bucket="crm-lakehouse-dev-123")

        def local_client(_endpoint: str | None) -> Any:
            raise AssertionError("the local S3 client must not be used for --target aws")

        monkeypatch.setattr(cli, "s3_client_from_env", local_client)
        monkeypatch.setattr(cli, "create_aws_s3_client", lambda: client)
        assert cli.main(["land", *SMALL, "--target", "aws"]) == 0
        keys = [
            o["Key"] for o in client.list_objects_v2(Bucket="crm-lakehouse-dev-123")["Contents"]
        ]
    assert len(keys) == 12
    assert "s3://crm-lakehouse-dev-123/landing" in capsys.readouterr().out


@pytest.mark.parametrize("bucket", [None, "lakehouse", "other-bucket"])
def test_target_aws_refuses_missing_or_foreign_bucket(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], bucket: str | None
) -> None:
    if bucket is None:
        monkeypatch.delenv("LAKEHOUSE_AWS_BUCKET", raising=False)
    else:
        monkeypatch.setenv("LAKEHOUSE_AWS_BUCKET", bucket)

    def no_client(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("no client may be created")

    monkeypatch.setattr(cli, "create_aws_s3_client", no_client)
    monkeypatch.setattr(cli, "s3_client_from_env", no_client)
    assert cli.main(["land", *SMALL, "--target", "aws"]) == 2
    assert "LAKEHOUSE_AWS_BUCKET" in capsys.readouterr().err


def test_target_aws_rejects_dry_run(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        cli.main(["land", *SMALL, "--target", "aws", "--dry-run"])
    assert exc.value.code == 2
    assert "--dry-run" in capsys.readouterr().err


def test_blank_endpoint_on_local_target_fails_and_never_reaches_aws(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stray empty ``S3_ENDPOINT`` must not route local landing data to AWS (W5 review)."""
    monkeypatch.setenv("S3_ENDPOINT", "")
    monkeypatch.setenv("LAKEHOUSE_AWS_BUCKET", "crm-lakehouse-dev-123")

    def no_aws(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("the AWS client must only be used with --target aws")

    monkeypatch.setattr(cli, "create_aws_s3_client", no_aws)
    try:
        code = cli.main(["land", *SMALL])
    except ValueError:  # botocore rejects an empty endpoint URL
        code = 1
    assert code != 0
