"""S3 landing writer (moto): keys, ordering, integrity, idempotency, retry (W3.D13.1)."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from lakehouse.generator.landing import write_batch, write_products


if TYPE_CHECKING:
    from collections.abc import Iterator


BUCKET = "lake-test"
BRAND = "brand_alpha"
DATE = "2026-01-05"
DATA = b'{"a":1}\n{"a":2}\n{"a":3}\n'
MANIFEST: dict[str, Any] = {"entries": [], "counts": {}, "line_count": 3}


def put_batch(client: Any, batch_no: int = 1, data: bytes = DATA) -> None:
    write_batch(client, BUCKET, BRAND, DATE, batch_no, data, dict(MANIFEST))


def put_products(client: Any, data: bytes = DATA) -> None:
    write_products(client, BUCKET, BRAND, DATE, data, dict(MANIFEST))


@pytest.fixture
def s3() -> Iterator[Any]:
    with mock_aws():
        c = boto3.client(
            "s3", region_name="eu-central-1", aws_access_key_id="x", aws_secret_access_key="x"
        )
        c.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-central-1"}
        )
        yield c


def keys(c: Any) -> list[str]:
    return sorted(o["Key"] for o in c.list_objects_v2(Bucket=BUCKET).get("Contents", []))


def body(c: Any, key: str) -> bytes:
    return c.get_object(Bucket=BUCKET, Key=key)["Body"].read()  # type: ignore[no-any-return]


def test_batch_keys_are_exact_and_five_digit_one_based(s3: Any) -> None:
    put_batch(s3, 1)
    put_batch(s3, 12)
    base = f"landing/{BRAND}/orders/{DATE}"
    assert keys(s3) == sorted(
        [
            f"{base}/batch-00001.ndjson",
            f"{base}/batch-00001.manifest.json",
            f"{base}/batch-00012.ndjson",
            f"{base}/batch-00012.manifest.json",
        ]
    )


def test_products_keys_are_exact(s3: Any) -> None:
    put_products(s3)
    base = f"landing/{BRAND}/products/{DATE}"
    assert keys(s3) == [f"{base}/products.manifest.json", f"{base}/products.ndjson"]


def test_manifest_sha256_and_line_count_match_object_bytes(s3: Any) -> None:
    put_batch(s3, 3)
    base = f"landing/{BRAND}/orders/{DATE}/batch-00003"
    data = body(s3, f"{base}.ndjson")
    manifest = json.loads(body(s3, f"{base}.manifest.json"))
    assert data == DATA
    assert manifest["sha256"] == hashlib.sha256(data).hexdigest()
    assert manifest["line_count"] == data.count(b"\n") == 3
    assert data.endswith(b"\n")
    assert b"\n\n" not in data


def test_manifest_integrity_check_detects_tampering(s3: Any) -> None:
    """Negative: the stored sha256 does not match altered bytes."""
    put_batch(s3, 1)
    base = f"landing/{BRAND}/orders/{DATE}/batch-00001"
    manifest = json.loads(body(s3, f"{base}.manifest.json"))
    assert manifest["sha256"] != hashlib.sha256(DATA + b'{"a":4}\n').hexdigest()


def test_manifest_is_put_after_data_object(s3: Any) -> None:
    order: list[str] = []
    s3.meta.events.register(
        "before-parameter-build.s3.PutObject", lambda params, **_: order.append(params["Key"])
    )
    put_batch(s3, 1)
    put_products(s3)
    assert [k.rsplit("/", 1)[1] for k in order] == [
        "batch-00001.ndjson",
        "batch-00001.manifest.json",
        "products.ndjson",
        "products.manifest.json",
    ]


def test_rerun_overwrites_with_identical_bytes(s3: Any) -> None:
    put_batch(s3, 1)
    put_products(s3)
    first = {k: body(s3, k) for k in keys(s3)}
    put_batch(s3, 1)
    put_products(s3)
    assert {k: body(s3, k) for k in keys(s3)} == first


def test_rerun_with_changed_data_changes_stored_bytes(s3: Any) -> None:
    """Negative: the idempotency comparison can fail when input differs."""
    put_batch(s3, 1)
    before = {k: body(s3, k) for k in keys(s3)}
    put_batch(s3, 1, data=b'{"b":1}\n')
    assert {k: body(s3, k) for k in keys(s3)} != before


class FlakyOnce:
    """Client wrapper whose first put_object raises a transient error, then delegates."""

    def __init__(self, inner: Any) -> None:
        self._inner, self.failed, self.calls = inner, False, 0

    def put_object(self, **kw: Any) -> Any:
        self.calls += 1
        if not self.failed:
            self.failed = True
            raise ClientError({"Error": {"Code": "503", "Message": "SlowDown"}}, "PutObject")
        return self._inner.put_object(**kw)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def test_transient_put_error_is_retried(s3: Any) -> None:
    flaky = FlakyOnce(s3)
    put_batch(flaky, 1)
    assert flaky.failed
    assert flaky.calls >= 3  # failed data put, retried data put, manifest put
    assert len(keys(s3)) == 2
    assert body(s3, f"landing/{BRAND}/orders/{DATE}/batch-00001.ndjson") == DATA


class AlwaysFails:
    def __init__(self) -> None:
        self.calls = 0

    def put_object(self, **_kw: Any) -> Any:
        self.calls += 1
        raise ClientError({"Error": {"Code": "503", "Message": "SlowDown"}}, "PutObject")


def test_exhausted_put_retries_raise_and_no_manifest_is_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("lakehouse.generator.landing.time.sleep", lambda _s: None)
    client = AlwaysFails()
    with pytest.raises(ClientError):
        write_batch(client, BUCKET, BRAND, DATE, 1, DATA, dict(MANIFEST))  # type: ignore[arg-type]
    assert client.calls == 3  # PUT_ATTEMPTS, all on the data object; the manifest never starts


def test_local_writers_use_the_s3_keys_and_manifest_last(tmp_path: Any) -> None:
    from lakehouse.generator.landing import write_batch_local, write_products_local

    final = write_batch_local(tmp_path, BRAND, DATE, 3, DATA, dict(MANIFEST))
    write_products_local(tmp_path, BRAND, DATE, DATA, dict(MANIFEST))
    base = tmp_path / "landing" / BRAND
    assert (base / "orders" / DATE / "batch-00003.ndjson").read_bytes() == DATA
    assert (base / "products" / DATE / "products.ndjson").read_bytes() == DATA
    stored = json.loads((base / "orders" / DATE / "batch-00003.manifest.json").read_text("utf-8"))
    assert stored == final
    assert stored["sha256"] == hashlib.sha256(DATA).hexdigest()
    assert stored["line_count"] == 3


@pytest.mark.parametrize(
    ("env", "expect_keys"),
    [
        ({"S3_ACCESS_KEY": "ak", "S3_SECRET_KEY": "sk"}, True),
        ({"S3_ACCESS_KEY": "ak"}, False),
        ({}, False),
    ],
)
def test_s3_client_from_env_uses_static_keys_only_when_both_set(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str], expect_keys: bool
) -> None:
    from lakehouse.generator.landing import s3_client_from_env

    for k in ("S3_ACCESS_KEY", "S3_SECRET_KEY", "AWS_REGION", "AWS_ACCESS_KEY_ID"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    client = s3_client_from_env("http://localhost:9000")
    assert client.meta.region_name == "us-east-1"
    assert client.meta.endpoint_url == "http://localhost:9000"
    creds = client._request_signer._credentials
    assert (creds is not None and creds.access_key == "ak") is expect_keys


def _clear_aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        monkeypatch.delenv(k, raising=False)


def test_create_aws_s3_client_ignores_local_endpoint_and_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from lakehouse.generator.landing import create_aws_s3_client

    _clear_aws_env(monkeypatch)
    monkeypatch.setenv("S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("S3_SECRET_KEY", "sk")
    monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    client = create_aws_s3_client()
    assert client.meta.region_name == "eu-west-1"
    assert client.meta.endpoint_url == "https://s3.eu-west-1.amazonaws.com"
    creds = client._request_signer._credentials
    assert creds is None or creds.access_key != "ak"  # SeaweedFS keys never go to AWS
    assert client.meta.config.s3 is None  # virtual-hosted addressing, not path-style


def test_create_aws_s3_client_defaults_to_frankfurt(monkeypatch: pytest.MonkeyPatch) -> None:
    from lakehouse.generator.landing import create_aws_s3_client

    _clear_aws_env(monkeypatch)
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    assert create_aws_s3_client().meta.region_name == "eu-central-1"


@pytest.mark.parametrize(
    "bucket", ["crm-lakehouse-dev-123456789012", "crm-lakehouse-x", " crm-lakehouse-dev-1 "]
)
def test_aws_bucket_from_env_accepts_project_buckets(
    monkeypatch: pytest.MonkeyPatch, bucket: str
) -> None:
    from lakehouse.generator.landing import aws_bucket_from_env

    monkeypatch.setenv("LAKEHOUSE_AWS_BUCKET", bucket)
    assert aws_bucket_from_env() == bucket.strip()


@pytest.mark.parametrize(
    "bucket",
    [
        None,
        "",
        "lakehouse",
        "other-bucket",
        "crm-lakehouse-",
        "crm-lakehouse-Dev",
        "x/crm-lakehouse-a",
    ],
)
def test_aws_bucket_from_env_refuses_other_buckets(
    monkeypatch: pytest.MonkeyPatch, bucket: str | None
) -> None:
    from lakehouse.generator.landing import aws_bucket_from_env

    if bucket is None:
        monkeypatch.delenv("LAKEHOUSE_AWS_BUCKET", raising=False)
    else:
        monkeypatch.setenv("LAKEHOUSE_AWS_BUCKET", bucket)
    with pytest.raises(ValueError, match="LAKEHOUSE_AWS_BUCKET"):
        aws_bucket_from_env()
