"""Batch landing of orders and products: deterministic object keys and an idempotent writer.

Object layout (consumed by the Spark batch reader)::

    landing/{brand_id}/orders/{yyyy-mm-dd}/batch-{NNNNN}.ndjson          data
    landing/{brand_id}/orders/{yyyy-mm-dd}/batch-{NNNNN}.manifest.json   "batch complete" marker
    landing/{brand_id}/products/{yyyy-mm-dd}/products.ndjson
    landing/{brand_id}/products/{yyyy-mm-dd}/products.manifest.json

The data object is always written first and the manifest last, so a manifest implies a complete
data object. Keys and bytes are deterministic, hence a retry or a re-run overwrites an object with
identical content (idempotent landing). The S3 client is injected (moto in tests, SeaweedFS or
AWS S3 at runtime); credentials never appear in code or logs.
"""

from __future__ import annotations

import hashlib
import os
import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import boto3
import structlog
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from lakehouse.generator.io import canonical_json


if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from mypy_boto3_s3 import S3Client


log = structlog.get_logger(__name__)

NDJSON_CONTENT_TYPE = "application/x-ndjson"
JSON_CONTENT_TYPE = "application/json"
PUT_ATTEMPTS = 3  # application-level retries on top of the botocore "standard" retry mode
RETRY_BACKOFF_S = 0.2
DEFAULT_AWS_REGION = "eu-central-1"
AWS_BUCKET_RE = re.compile(r"crm-lakehouse-[a-z0-9-]+")


@dataclass(frozen=True)
class LandingFile:
    """A data object (NDJSON bytes) and the manifest dict that is landed next to it."""

    data: bytes
    manifest: dict[str, Any]


def orders_prefix(brand_id: str, batch_date: str) -> str:
    return f"landing/{brand_id}/orders/{batch_date}"


def orders_data_key(brand_id: str, batch_date: str, batch_no: int) -> str:
    return f"{orders_prefix(brand_id, batch_date)}/batch-{batch_no:05d}.ndjson"


def orders_manifest_key(brand_id: str, batch_date: str, batch_no: int) -> str:
    return f"{orders_prefix(brand_id, batch_date)}/batch-{batch_no:05d}.manifest.json"


def products_data_key(brand_id: str, batch_date: str) -> str:
    return f"landing/{brand_id}/products/{batch_date}/products.ndjson"


def products_manifest_key(brand_id: str, batch_date: str) -> str:
    return f"landing/{brand_id}/products/{batch_date}/products.manifest.json"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def manifest_bytes(manifest: dict[str, Any]) -> bytes:
    """Canonical manifest bytes (sorted keys, one trailing newline)."""
    return (canonical_json(manifest) + "\n").encode("utf-8")


def create_s3_client(
    endpoint_url: str | None,
    *,
    region_name: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> S3Client:
    """boto3 S3 client: standard retries (5 attempts) and path-style addressing (SeaweedFS)."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region_name,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            retries={"mode": "standard", "max_attempts": 5},
            s3={"addressing_style": "path"},
        ),
    )


def s3_client_from_env(endpoint_url: str | None) -> S3Client:
    """Client for ``endpoint_url`` with ``S3_ACCESS_KEY``/``S3_SECRET_KEY`` when both are set.

    Without them boto3's default credential chain applies (AWS profile / role).
    """
    access_key = os.environ.get("S3_ACCESS_KEY") or None
    secret_key = os.environ.get("S3_SECRET_KEY") or None
    if not (access_key and secret_key):
        access_key = secret_key = None
    return create_s3_client(
        endpoint_url,
        region_name=os.environ.get("AWS_REGION") or "us-east-1",
        access_key=access_key,
        secret_key=secret_key,
    )


def aws_bucket_from_env() -> str:
    """Data bucket for ``land --target aws``: ``LAKEHOUSE_AWS_BUCKET``, which must be a
    ``crm-lakehouse-*`` bucket (D-102). Raises ``ValueError`` (never echoing a credential)."""
    bucket = os.environ.get("LAKEHOUSE_AWS_BUCKET", "").strip()
    if not AWS_BUCKET_RE.fullmatch(bucket):
        raise ValueError(
            "LAKEHOUSE_AWS_BUCKET must be set to a crm-lakehouse-* bucket "
            f"(pattern {AWS_BUCKET_RE.pattern})"
        )
    return bucket


def create_aws_s3_client() -> S3Client:
    """Client for real AWS S3: boto3 default credential chain, AWS default endpoint,
    virtual-hosted addressing. ``S3_ACCESS_KEY``/``S3_SECRET_KEY``/``S3_ENDPOINT`` (the local
    SeaweedFS settings) are never read; the region is ``AWS_REGION`` (default eu-central-1)."""
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION") or DEFAULT_AWS_REGION,
        config=Config(retries={"mode": "standard", "max_attempts": 5}),
    )


def _put(client: S3Client, bucket: str, key: str, body: bytes, content_type: str) -> None:
    for attempt in range(1, PUT_ATTEMPTS + 1):
        try:
            client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
        except (ClientError, BotoCoreError) as exc:
            if attempt == PUT_ATTEMPTS:
                raise
            log.warning("landing.put_retry", key=key, attempt=attempt, error=type(exc).__name__)
            time.sleep(RETRY_BACKOFF_S * attempt)
        else:
            return


def _land(
    put: Callable[[str, bytes, str], None],
    data_key: str,
    manifest_key: str,
    data: bytes,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    final = {
        **manifest,
        "data_key": data_key,
        "line_count": data.count(b"\n"),
        "sha256": sha256_hex(data),
    }
    put(data_key, data, NDJSON_CONTENT_TYPE)
    put(manifest_key, manifest_bytes(final), JSON_CONTENT_TYPE)  # last: "batch complete"
    log.info(
        "landing.written", brand_id=final.get("brand_id"), key=data_key, lines=final["line_count"]
    )
    return final


def _s3_put(client: S3Client, bucket: str) -> Callable[[str, bytes, str], None]:
    def put(key: str, body: bytes, content_type: str) -> None:
        _put(client, bucket, key, body, content_type)

    return put


def _local_put(root: Path) -> Callable[[str, bytes, str], None]:
    def put(key: str, body: bytes, _content_type: str) -> None:
        write_local(root, key, body)

    return put


def write_batch(
    client: S3Client,
    bucket: str,
    brand_id: str,
    batch_date: str,
    batch_no: int,
    data: bytes,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    """Land one orders batch: data object first, manifest (with sha256, line count) last."""
    return _land(
        _s3_put(client, bucket),
        orders_data_key(brand_id, batch_date, batch_no),
        orders_manifest_key(brand_id, batch_date, batch_no),
        data,
        manifest,
    )


def write_products(
    client: S3Client,
    bucket: str,
    brand_id: str,
    batch_date: str,
    data: bytes,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    """Land one brand's product seed: data object first, manifest last."""
    return _land(
        _s3_put(client, bucket),
        products_data_key(brand_id, batch_date),
        products_manifest_key(brand_id, batch_date),
        data,
        manifest,
    )


def write_batch_local(
    root: Path,
    brand_id: str,
    batch_date: str,
    batch_no: int,
    data: bytes,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    """Dry-run twin of :func:`write_batch`: same keys and bytes under a local directory."""
    return _land(
        _local_put(root),
        orders_data_key(brand_id, batch_date, batch_no),
        orders_manifest_key(brand_id, batch_date, batch_no),
        data,
        manifest,
    )


def write_products_local(
    root: Path, brand_id: str, batch_date: str, data: bytes, manifest: dict[str, Any]
) -> dict[str, Any]:
    """Dry-run twin of :func:`write_products`."""
    return _land(
        _local_put(root),
        products_data_key(brand_id, batch_date),
        products_manifest_key(brand_id, batch_date),
        data,
        manifest,
    )


def write_local(root: Path, key: str, body: bytes) -> Path:
    """Write ``body`` to ``root/key`` (bytes, so Windows does not translate newlines)."""
    path = root.joinpath(*key.split("/"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path
