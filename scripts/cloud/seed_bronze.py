#!/usr/bin/env python3
"""Seed the Kafka-sourced Bronze Delta tables from the local SeaweedFS into AWS S3 (D-101).

Usage (host venv, local stack up, AWS credentials from boto3's default chain)::

    python scripts/cloud/seed_bronze.py [--table raw_customers ...] [--local-bucket NAME]   # plan
    python scripts/cloud/seed_bronze.py --apply [--replace]                                  # copy

AWS has no Kafka in this project (D-101): ``bronze/raw_customers/`` and ``bronze/raw_clickstream/``
(complete Delta table directories, ``_delta_log`` included) are copied object by object from the
local bucket (endpoint ``S3_ENDPOINT``, keys ``S3_ACCESS_KEY`` / ``S3_SECRET_KEY``, both required, never the default chain; bucket ``--local-bucket`` or ``LAKEHOUSE_BUCKET``, default ``lakehouse``)
to ``s3://$LAKEHOUSE_AWS_BUCKET/bronze/<table>/``. The AWS side uses boto3's default chain only.
The source ``S3_ENDPOINT`` must be the host-reachable local endpoint; empty or unset is refused
(that would address AWS itself).

Safety: a dry run is the default (prints object count and bytes per table, writes nothing). A
table whose target prefix already holds objects is refused unless ``--replace`` (which deletes the
target prefix first, then copies). Data objects are copied before ``_delta_log/`` so a partial copy
is never a readable Delta table. After the copy the object count and total bytes of every target
prefix must equal the source, otherwise exit 1. Exit codes: 0 ok, 1 refused / failed / mismatch,
2 usage or configuration.

Does ``reconcile`` work on Glue with seeded Bronze? Yes, with the Silver jobs run on it. The
``scope='batch'`` rows of ``ops.pipeline_metrics`` are written by the Silver jobs (``record_batch``
in ``silver_customers`` / ``silver_orders`` / ``silver_clickstream`` / ``silver_products``), not by
``bronze_stream`` (the ingestion component never writes ops metrics). ``reconcile`` derives the
Bronze side of ``raw_customers`` and ``raw_clickstream`` from the Bronze data itself (distinct
``_source_ref``), which the copy preserves. Silver starts from fresh checkpoints on AWS and reads
all of the seeded Bronze, so every term balances as on a clean local run. Not covered: anything
that only ``bronze_stream`` would have recorded (nothing today). The catalog must know the seeded
tables first: run ``register_tables`` before ``dlq_gate``/Silver (``run_job.py --sequence cloud``
does).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from typing import TYPE_CHECKING, Any, NamedTuple


if TYPE_CHECKING:
    from collections.abc import Sequence

SEED_TABLES = ("raw_customers", "raw_clickstream")
BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\Z")
LAYER = "bronze"
DELTA_LOG = "_delta_log/"
_EXIT_FAILED = 1
_EXIT_USAGE = 2


class ObjectInfo(NamedTuple):
    key: str
    size: int


def table_prefix(table: str) -> str:
    return f"{LAYER}/{table}/"


def list_objects(s3: Any, bucket: str, prefix: str) -> list[ObjectInfo]:
    """Every object under ``prefix`` (directory placeholders, keys ending ``/``, are skipped)."""
    found: list[ObjectInfo] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if not item["Key"].endswith("/"):
                found.append(ObjectInfo(item["Key"], int(item["Size"])))
    return found


def copy_order(prefix: str, objects: Sequence[ObjectInfo]) -> list[ObjectInfo]:
    """Data files first, the ``_delta_log`` last (a half-copied table is then not a table)."""
    log_marker = f"{prefix}{DELTA_LOG}"
    data = [o for o in objects if not o.key.startswith(log_marker)]
    log = [o for o in objects if o.key.startswith(log_marker)]
    return sorted(data) + sorted(log)


def _copy_object(src: Any, dst: Any, src_bucket: str, dst_bucket: str, key: str) -> None:
    body = src.get_object(Bucket=src_bucket, Key=key)["Body"]
    try:
        dst.upload_fileobj(body, dst_bucket, key)
    finally:
        body.close()


def _delete_prefix(s3: Any, bucket: str, objects: Sequence[ObjectInfo]) -> None:
    keys = [{"Key": o.key} for o in objects]
    for start in range(0, len(keys), 1000):
        response = s3.delete_objects(Bucket=bucket, Delete={"Objects": keys[start : start + 1000]})
        errors = response.get("Errors") or []
        if errors:  # delete_objects reports per-key failures with HTTP 200
            codes = sorted({str(e.get("Code", "?")) for e in errors})
            raise RuntimeError(f"delete_objects failed for {len(errors)} keys ({', '.join(codes)})")


def _totals(objects: Sequence[ObjectInfo]) -> tuple[int, int]:
    return len(objects), sum(o.size for o in objects)


def seed(
    src: Any,
    dst: Any,
    *,
    src_bucket: str,
    dst_bucket: str,
    tables: Sequence[str],
    apply: bool,
    replace: bool,
    out: Any = None,
) -> int:
    """Plan or run the copy; returns the exit code (see the module docstring)."""
    out = out or sys.stdout
    plans: list[tuple[str, list[ObjectInfo], list[ObjectInfo]]] = []
    for table in tables:
        prefix = table_prefix(table)
        source = list_objects(src, src_bucket, prefix)
        if not any(o.key.startswith(f"{prefix}{DELTA_LOG}") for o in source):
            print(
                f"seed_bronze: no Delta table at s3://{src_bucket}/{prefix} (local)",
                file=sys.stderr,
            )
            return _EXIT_FAILED
        target = list_objects(dst, dst_bucket, prefix)
        count, size = _totals(source)
        print(
            f"{table}: source {count} objects, {size} bytes; target {len(target)} objects",
            file=out,
        )
        if target and not replace:
            print(
                f"seed_bronze: s3://{dst_bucket}/{prefix} already has {len(target)} objects; "
                "refusing (use --replace to overwrite)",
                file=sys.stderr,
            )
            return _EXIT_FAILED
        plans.append((table, source, target))
    if not apply:
        print("dry run: nothing copied (use --apply)", file=out)
        return 0
    for table, source, target in plans:
        prefix = table_prefix(table)
        if target:
            _delete_prefix(dst, dst_bucket, target)
        for obj in copy_order(prefix, source):
            _copy_object(src, dst, src_bucket, dst_bucket, obj.key)
        copied = list_objects(dst, dst_bucket, prefix)
        if _totals(copied) != _totals(source):
            print(
                f"seed_bronze: verification FAILED for {table}: source {_totals(source)} "
                f"(objects, bytes) vs target {_totals(copied)}",
                file=sys.stderr,
            )
            return _EXIT_FAILED
        print(
            f"{table}: copied and verified {_totals(copied)[0]} objects, {_totals(copied)[1]} bytes",
            file=out,
        )
    return 0


def _local_client(endpoint: str) -> Any:
    from lakehouse.generator.landing import s3_client_from_env

    return s3_client_from_env(endpoint)


def _aws_client() -> Any:
    import boto3

    return boto3.client("s3")  # default credential chain only; never an endpoint or keys


def _tables(values: Sequence[str] | None) -> list[str]:
    return list(dict.fromkeys(values or SEED_TABLES))


def main(argv: Sequence[str] | None = None, *, src: Any = None, dst: Any = None) -> int:
    parser = argparse.ArgumentParser(description="Copy the local Kafka-sourced Bronze to AWS S3.")
    parser.add_argument("--table", action="append", choices=SEED_TABLES, help="default: both")
    parser.add_argument("--local-bucket", default=os.environ.get("LAKEHOUSE_BUCKET") or "lakehouse")
    parser.add_argument("--apply", action="store_true", help="copy (default: dry run)")
    parser.add_argument("--replace", action="store_true", help="overwrite a non-empty target")
    ns = parser.parse_args(argv)

    aws_bucket = os.environ.get("LAKEHOUSE_AWS_BUCKET", "")
    if not BUCKET_PATTERN.match(aws_bucket):
        print("seed_bronze: set LAKEHOUSE_AWS_BUCKET to the AWS data bucket name", file=sys.stderr)
        return _EXIT_USAGE
    if not BUCKET_PATTERN.match(ns.local_bucket):
        print("seed_bronze: invalid --local-bucket", file=sys.stderr)
        return _EXIT_USAGE
    endpoint = (os.environ.get("S3_ENDPOINT") or "").strip()
    if src is None and not endpoint:
        print("seed_bronze: set S3_ENDPOINT to the local object store endpoint", file=sys.stderr)
        return _EXIT_USAGE
    if src is None and not (os.environ.get("S3_ACCESS_KEY") and os.environ.get("S3_SECRET_KEY")):
        # Never fall back to boto3's default chain: that would read the local bucket with AWS keys.
        print(
            "seed_bronze: set S3_ACCESS_KEY and S3_SECRET_KEY (local object store)", file=sys.stderr
        )
        return _EXIT_USAGE
    try:
        return seed(
            src if src is not None else _local_client(endpoint),
            dst if dst is not None else _aws_client(),
            src_bucket=ns.local_bucket,
            dst_bucket=aws_bucket,
            tables=_tables(ns.table),
            apply=ns.apply,
            replace=ns.replace,
        )
    except Exception as exc:  # no credentials in botocore messages; type and message only
        print(f"seed_bronze: {type(exc).__name__}: {exc}", file=sys.stderr)
        return _EXIT_FAILED


if __name__ == "__main__":
    raise SystemExit(main())
