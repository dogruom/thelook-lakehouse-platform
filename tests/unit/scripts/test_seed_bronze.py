"""``scripts/cloud/seed_bronze.py``: dry run, apply, refuse, verify (moto, two buckets)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import boto3
import pytest
from moto import mock_aws

from .conftest import load_script


if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType

LOCAL = "lakehouse"
AWS = "crm-lakehouse-dev-123456789012"
LOG0 = "_delta_log/00000000000000000000.json"
FILES = {
    "bronze/raw_customers/part-0000.snappy.parquet": b"p" * 100,
    f"bronze/raw_customers/{LOG0}": b"{}\n",
    "bronze/raw_clickstream/part-0000.snappy.parquet": b"c" * 50,
    f"bronze/raw_clickstream/{LOG0}": b"{}\n",
    "bronze/raw_orders/part-9.parquet": b"not seeded",
}


@pytest.fixture(scope="module")
def seed() -> ModuleType:
    return load_script("cloud/seed_bronze")


@pytest.fixture
def buckets(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    monkeypatch.setenv("LAKEHOUSE_AWS_BUCKET", AWS)
    monkeypatch.delenv("LAKEHOUSE_BUCKET", raising=False)
    with mock_aws():
        s3 = boto3.client(
            "s3", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x"
        )
        for name in (LOCAL, AWS):
            s3.create_bucket(Bucket=name)
        for key, body in FILES.items():
            s3.put_object(Bucket=LOCAL, Key=key, Body=body)
        yield s3


def keys(s3: Any, bucket: str) -> list[str]:
    return sorted(o["Key"] for o in s3.list_objects_v2(Bucket=bucket).get("Contents", []))


def test_dry_run_is_the_default_and_writes_nothing(
    seed: ModuleType, buckets: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    assert seed.main([], src=buckets, dst=buckets) == 0
    assert keys(buckets, AWS) == []
    out = capsys.readouterr().out
    assert "raw_customers: source 2 objects, 103 bytes" in out
    assert "dry run" in out


def test_apply_copies_both_tables_and_verifies(
    seed: ModuleType, buckets: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    assert seed.main(["--apply"], src=buckets, dst=buckets) == 0
    assert keys(buckets, AWS) == sorted(k for k in FILES if "raw_orders" not in k)
    body = buckets.get_object(Bucket=AWS, Key="bronze/raw_clickstream/part-0000.snappy.parquet")
    assert body["Body"].read() == b"c" * 50
    assert "copied and verified 2 objects, 103 bytes" in capsys.readouterr().out


def test_table_option_limits_the_copy(seed: ModuleType, buckets: Any) -> None:
    assert seed.main(["--apply", "--table", "raw_customers"], src=buckets, dst=buckets) == 0
    assert all(k.startswith("bronze/raw_customers/") for k in keys(buckets, AWS))


def test_non_empty_target_is_refused_without_replace(
    seed: ModuleType, buckets: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    buckets.put_object(Bucket=AWS, Key="bronze/raw_customers/old.parquet", Body=b"old")
    assert seed.main(["--apply"], src=buckets, dst=buckets) == 1
    assert "refusing" in capsys.readouterr().err
    assert keys(buckets, AWS) == ["bronze/raw_customers/old.parquet"]  # nothing copied at all


def test_replace_deletes_the_old_target_first(seed: ModuleType, buckets: Any) -> None:
    buckets.put_object(Bucket=AWS, Key="bronze/raw_customers/old.parquet", Body=b"old")
    assert seed.main(["--apply", "--replace"], src=buckets, dst=buckets) == 0
    assert "bronze/raw_customers/old.parquet" not in keys(buckets, AWS)
    assert len(keys(buckets, AWS)) == 4


def test_missing_local_table_fails(seed: ModuleType, buckets: Any) -> None:
    buckets.delete_object(Bucket=LOCAL, Key=f"bronze/raw_customers/{LOG0}")
    assert seed.main(["--apply"], src=buckets, dst=buckets) == 1
    assert keys(buckets, AWS) == []


def test_verification_mismatch_exits_1(
    seed: ModuleType, buckets: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(seed, "_copy_object", lambda *_a, **_k: None)  # copies nothing
    assert seed.main(["--apply", "--table", "raw_customers"], src=buckets, dst=buckets) == 1


def test_delta_log_is_copied_last(seed: ModuleType) -> None:
    objs = [
        seed.ObjectInfo("bronze/t/_delta_log/0.json", 1),
        seed.ObjectInfo("bronze/t/part-1.parquet", 1),
        seed.ObjectInfo("bronze/t/part-0.parquet", 1),
    ]
    ordered = [o.key for o in seed.copy_order("bronze/t/", objs)]
    assert ordered == [
        "bronze/t/part-0.parquet",
        "bronze/t/part-1.parquet",
        "bronze/t/_delta_log/0.json",
    ]


def test_directory_placeholders_are_skipped(seed: ModuleType, buckets: Any) -> None:
    buckets.put_object(Bucket=LOCAL, Key="bronze/raw_customers/sub/", Body=b"")
    listed = seed.list_objects(buckets, LOCAL, "bronze/raw_customers/")
    assert all(not o.key.endswith("/") for o in listed)


def test_configuration_errors_exit_2(
    seed: ModuleType, buckets: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("LAKEHOUSE_AWS_BUCKET")
    assert seed.main([], src=buckets, dst=buckets) == 2
    monkeypatch.setenv("LAKEHOUSE_AWS_BUCKET", AWS)
    monkeypatch.delenv("S3_ENDPOINT", raising=False)
    assert seed.main([]) == 2  # no injected client and no local endpoint: never defaults to AWS


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"S3_ACCESS_KEY": "ak"},
        {"S3_SECRET_KEY": "sk"},
        {"S3_ACCESS_KEY": "", "S3_SECRET_KEY": "sk"},
    ],
)
def test_local_client_needs_both_static_keys(
    seed: ModuleType,
    buckets: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    env: dict[str, str],
) -> None:
    monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
    for k in ("S3_ACCESS_KEY", "S3_SECRET_KEY"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setattr(seed, "_local_client", lambda _e: pytest.fail("no local client"))
    assert seed.main([]) == 2
    assert "S3_ACCESS_KEY" in capsys.readouterr().err


def test_delete_objects_errors_fail_the_replace(
    seed: ModuleType, buckets: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    buckets.put_object(Bucket=AWS, Key="bronze/raw_customers/old.parquet", Body=b"old")

    class DenyingDst:
        def __init__(self, inner: Any) -> None:
            self._inner = inner

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

        def delete_objects(self, **kwargs: Any) -> dict[str, Any]:
            keys_ = [o["Key"] for o in kwargs["Delete"]["Objects"]]
            return {"Errors": [{"Key": k, "Code": "AccessDenied"} for k in keys_]}

    assert seed.main(["--apply", "--replace"], src=buckets, dst=DenyingDst(buckets)) == 1
    assert "AccessDenied" in capsys.readouterr().err
    assert keys(buckets, AWS) == ["bronze/raw_customers/old.parquet"]  # nothing copied
