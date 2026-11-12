"""Shared steps of the W3.D17 integration tests (orders Silver and DLQ replay).

Everything that touches the bucket runs in a one-off ``seaweedfs-init`` container (see
``delta_io``); jobs run through ``scripts/submit_job.py``. All objects live under ``it_<hex8>``
namespaces, which the session purge removes.
"""

from __future__ import annotations

import subprocess
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from lakehouse.contracts.kafka import cdc_headers, cdc_key
from lakehouse.contracts.models import CustomerCdcEvent, to_wire
from lakehouse.generator.io import canonical_json
from lakehouse.generator.landing import write_local
from lakehouse.generator.messages import OutboundMessage
from lakehouse.generator.producer import create_producer, produce_messages

from .delta_io import DOCKER_TIMEOUT_S, download_table, read_rows
from .spark_jobs import REPO_ROOT, log_events, metrics, run_ok


if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

Row = dict[str, Any]
MAX_OFFSETS = 1000
MAX_FILES = 1000
_AWS = 'aws --endpoint-url "http://$S3_HOST:$S3_PORT" --no-cli-pager --only-show-errors s3 cp'
_built = [False]  # the first submit builds the job zip, later ones reuse it


def run(args: list[str]) -> str:
    """``run_ok`` that builds the job zip once per session and reuses it afterwards."""
    out = run_ok(args if not _built[0] else ["--skip-build", *args])
    _built[0] = True
    return out


def produce(servers: str, messages: Sequence[OutboundMessage]) -> None:
    stats = produce_messages(list(messages), producer=create_producer(servers))
    assert stats.ok, stats
    assert stats.failed == 0 and stats.delivered == len(messages)


def cdc_message(
    topic: str, brand: str, customer_id: str, seq: int, n: int, action: str = "I"
) -> OutboundMessage:
    """A contract-valid customer CDC message built with the real key/header builders."""
    base = datetime(2026, 3, 1, tzinfo=UTC)
    payload = to_wire(
        CustomerCdcEvent.model_validate(
            {
                "event_id": f"{n:08x}-0000-4000-8000-00000000c0de",
                "brand_id": brand,
                "schema_version": 1,
                "produced_at": base + timedelta(minutes=seq, seconds=1),
                "customer_id": customer_id,
                "first_name": "Ada",
                "last_name": "Lovelace",
                "email": f"{customer_id[:8]}@example.com",
                "phone": "+447700900123",
                "city": f"City-{seq}",
                "country": "DE",
                "loyalty_tier": "Gold",
                "signup_date": date(2025, 1, 1),
                "_cdc_action": action,
                "_cdc_sequence": seq,
                "_cdc_ts": base + timedelta(minutes=seq),
            }
        )
    )
    return OutboundMessage(
        topic=topic,
        key=cdc_key(payload),
        value=canonical_json(payload).encode("utf-8"),
        headers=tuple(cdc_headers(payload)),
        brand_id=brand,
    )


def customers_round(
    servers: str, messages: Sequence[OutboundMessage], namespace: str, cdc_topic: str, click: str
) -> dict[str, Any]:
    """Produce ``messages``, drain them into Bronze, then into ``silver.customers``."""
    produce(servers, messages)
    run(
        [
            "bronze_stream",
            "--namespace",
            namespace,
            "--cdc-topic",
            cdc_topic,
            "--clickstream-topic",
            click,
            "--max-offsets-per-trigger",
            str(MAX_OFFSETS),
        ]
    )
    out = run(
        ["silver_customers", "--namespace", namespace, "--max-files-per-trigger", str(MAX_FILES)]
    )
    assert len(log_events(out, "silver_customers.batch")) == metrics(out)["batches"]
    return dict(metrics(out))


def upload_landing(stage: Path, namespace: str) -> None:
    """Copy ``stage`` (contains ``landing/<brand>/...``) to ``landing/<namespace>/<brand>/...``."""
    script = f'{_AWS} /data/landing "s3://$LAKEHOUSE_BUCKET/landing/{namespace}/" --recursive'
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
            f"{stage.resolve().as_posix()}:/data",
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
    assert proc.returncode == 0, f"landing upload failed: {proc.stderr[-400:]}"


def land(stage: Path, namespace: str, files: dict[str, bytes]) -> None:
    """Write ``{landing key: bytes}`` (keys without namespace) to ``stage`` and upload them."""
    for key, body in files.items():
        write_local(stage, key, body)
    upload_landing(stage, namespace)


def prefixed(namespace: str, key: str) -> str:
    """``landing/<brand>/...`` -> ``landing/<namespace>/<brand>/...`` (the Bronze file key)."""
    return key.replace("landing/", f"landing/{namespace}/", 1)


def snapshot(layer: str, table: str, namespace: str, root: Path, tag: str) -> list[Row]:
    dest = root / f"{tag}_{layer}_{table}"
    download_table(layer, table, namespace, dest)
    return read_rows(dest)


def silver_snapshot(namespace: str, root: Path, tag: str) -> dict[str, list[Row]]:
    return {
        "orders": snapshot("silver", "orders", namespace, root, tag),
        "items": snapshot("silver", "order_items", namespace, root, tag),
        "dlq": snapshot("dead_letter", "dlq_orders", namespace, root, tag),
    }


def violations(
    expected_orders: dict[str, set[str]],
    expected_items: dict[str, int],
    orders: Sequence[Row],
    items: Sequence[Row],
) -> list[str]:
    """Everything wrong with Silver ``orders``/``items`` versus the landed ground truth.

    ``expected_orders``: brand -> accepted order ids; ``expected_items``: brand -> item count.
    """
    problems: list[str] = []
    order_keys = [(r["brand_id"], r["order_id"]) for r in orders]
    if len(set(order_keys)) != len(order_keys):
        problems.append("duplicate (brand_id, order_id) in silver.orders")
    item_keys = [(r["brand_id"], r["order_id"], r["item_id"]) for r in items]
    if len(set(item_keys)) != len(item_keys):
        problems.append("duplicate (brand_id, order_id, item_id) in silver.order_items")
    got: dict[str, set[str]] = {}
    for brand, order_id in order_keys:
        got.setdefault(brand, set()).add(order_id)
    for brand in sorted(set(got) | set(expected_orders)):
        if got.get(brand, set()) != expected_orders.get(brand, set()):
            missing = expected_orders.get(brand, set()) - got.get(brand, set())
            extra = got.get(brand, set()) - expected_orders.get(brand, set())
            problems.append(f"{brand}: orders missing={len(missing)} unexpected={len(extra)}")
    known = set(order_keys)
    problems += [
        f"item {k} has no parent order in its own brand" for k in item_keys if k[:2] not in known
    ]
    per_brand: dict[str, int] = {}
    for brand, _o, _i in item_keys:
        per_brand[brand] = per_brand.get(brand, 0) + 1
    for brand in sorted(set(per_brand) | set(expected_items)):
        if per_brand.get(brand, 0) != expected_items.get(brand, 0):
            problems.append(
                f"{brand}: items {per_brand.get(brand, 0)} != expected {expected_items.get(brand, 0)}"
            )
    return problems
