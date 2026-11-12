"""Shared full-pipeline steps of the W3.D18 integration tests (reconciliation, rerun, isolation).

Seeded CDC and clickstream go through Kafka; products and orders are landed as files. Ground truth
comes from the generator manifests and the landed bytes, never from job output. Everything runs in
``it_<hex8>`` namespaces, which the session purge removes.
"""

from __future__ import annotations

import json
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.clickstream import generate_clickstream
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.landing import products_data_key
from lakehouse.generator.manifest import Outcome
from lakehouse.generator.messages import cdc_messages
from lakehouse.generator.orders import customer_pools_from_cdc, generate_orders
from lakehouse.generator.products import generate_products

from .delta_io import DOCKER_TIMEOUT_S
from .order_flow import Row, land, produce, run
from .spark_jobs import REPO_ROOT


if TYPE_CHECKING:
    from pathlib import Path

GATE_TABLES = ("raw_customers", "raw_clickstream", "raw_orders", "raw_products")
#: ``aws s3 ls`` rejects ``--only-show-errors`` (a cp/sync/rm option): the listing then failed and
#: returned nothing, which made every checkpoint look empty. Do not add it back.
_AWS = 'aws --endpoint-url "http://$S3_HOST:$S3_PORT" --no-cli-pager s3'


@dataclass
class World:
    """Generated inputs plus the manifest-derived expectations (per brand)."""

    cdc: Any
    click: Any  # None when no clickstream was requested
    landed: dict[str, bytes]
    order_batches: list[Any]
    brands: list[str]

    # ---- customers (CDC manifest) ----
    def cdc_emitted(self) -> Counter[str]:
        return Counter(e.brand_id for e in self.cdc.manifest.entries)

    def cdc_dlq(self) -> Counter[str]:
        return Counter(
            e.brand_id for e in self.cdc.manifest.entries if e.expected_outcome == Outcome.DLQ
        )

    def customers_final(self) -> Counter[str]:
        return Counter(r["brand_id"] for r in self.cdc.manifest.expected_final)

    # ---- clickstream ----
    def click_emitted(self, *, parseable_only: bool) -> Counter[str]:
        if self.click is None:
            return Counter()
        bad = {e["index"] for e in self.click.manifest.entries if e["defect"] == "malformed_json"}
        return Counter(
            m.brand_id
            for i, m in enumerate(self.click.messages)
            if not (parseable_only and i in bad)
        )

    def click_malformed(self) -> int:
        if self.click is None:
            return 0
        return sum(1 for e in self.click.manifest.entries if e["defect"] == "malformed_json")

    def click_dlq(self) -> Counter[str]:
        if self.click is None:
            return Counter()
        return Counter(
            e["brand_id"] for e in self.click.manifest.entries if e["defect"] == "null_session"
        )

    # ---- orders (landed bytes + order manifests) ----
    def orders_truth(self) -> dict[str, Counter[str]]:
        lines: Counter[str] = Counter()
        accepted: Counter[str] = Counter()
        items: Counter[str] = Counter()
        dlq: Counter[str] = Counter()
        dups: Counter[str] = Counter()
        seen: dict[str, set[str]] = defaultdict(set)
        for b in self.order_batches:
            lines[b.brand_id] += b.manifest["line_count"]
            bad = {e["order_id"] for e in b.manifest["entries"] if e["defect"] != "duplicate"}
            for e in b.manifest["entries"]:
                (dups if e["defect"] == "duplicate" else dlq)[b.brand_id] += 1
            for raw in b.data.decode("utf-8").splitlines():
                order = json.loads(raw)
                oid = order["order_id"]
                if oid in bad or oid in seen[b.brand_id]:
                    continue
                seen[b.brand_id].add(oid)
                accepted[b.brand_id] += 1
                items[b.brand_id] += len(order["items"])
        return {
            "bronze": lines,
            "silver_accepted": accepted,
            "items": items,
            "dlq": dlq,
            "duplicates_removed": dups,
        }


def build_world(
    *,
    brands: int,
    cdc_events: int,
    click_events: int,
    order_batches: int,
    orders_per_batch: int,
    products_per_brand: int = 10,
    cdc_cross_tenant_collision_rate: float | None = None,
) -> World:
    # The default collision rate (1 %) rounds to zero collisions on a few dozen customers; a test
    # that needs collisions sets a higher rate explicitly.
    overrides: dict[str, float] = {}
    if cdc_cross_tenant_collision_rate is not None:
        overrides["cdc_cross_tenant_collision_rate"] = cdc_cross_tenant_collision_rate
    config = GeneratorConfig(_env_file=None, products_per_brand=products_per_brand, **overrides)  # type: ignore[call-arg]
    cdc = generate_cdc(config, brands=brands, events=cdc_events)
    pools = customer_pools_from_cdc(cdc)
    click = (
        generate_clickstream(config, brands=brands, events=click_events) if click_events else None
    )
    products = generate_products(config)
    orders = generate_orders(
        config,
        customer_pools=pools,
        product_ids={b: ids for b, ids in products.product_ids().items() if b in pools},
        batches=order_batches,
        orders_per_batch=orders_per_batch,
    )
    landed: dict[str, bytes] = {}
    day = products.batch_date.isoformat()
    for brand in pools:
        landed[products_data_key(brand, day)] = products.files[brand].data
    for b in orders.batches:
        landed[b.manifest["data_key"]] = b.data
    return World(cdc, click, landed, list(orders.batches), sorted(pools))


def produce_world(servers: str, world: World, cdc_topic: str, click_topic: str) -> None:
    """Produce the CDC (and clickstream) messages to the given topics (once per module)."""
    produce(servers, cdc_messages(world.cdc, cdc_topic))
    if world.click is not None:
        # The generator was built without a topic: re-point the messages at the temp topic.
        from dataclasses import replace

        produce(servers, [replace(m, topic=click_topic) for m in world.click.messages])


def load_bronze(
    world: World,
    namespace: str,
    cdc_topic: str,
    click_topic: str,
    tmp: Path,
    *,
    max_offsets: int = 1000,
) -> None:
    """Bronze (stream + batch) and the contract gate of every Bronze table of ``namespace``."""
    land(tmp / f"stage_{namespace}", namespace, world.landed)
    run(
        [
            "bronze_stream",
            "--namespace",
            namespace,
            "--cdc-topic",
            cdc_topic,
            "--clickstream-topic",
            click_topic,
            "--max-offsets-per-trigger",
            str(max_offsets),
        ]
    )
    run(["bronze_batch", "--namespace", namespace])
    args = ["dlq_gate", "--namespace", namespace]
    for table in GATE_TABLES:
        args += ["--table", table]
    run(args)


def silver_all(namespace: str, *, with_clickstream: bool = True) -> None:
    run(["silver_customers", "--namespace", namespace])
    if with_clickstream:
        run(["silver_clickstream", "--namespace", namespace])
    run(["silver_orders", "--namespace", namespace])


def s3_names(prefix: str) -> list[str]:
    """Object/dir names directly under ``s3://<bucket>/<prefix>/`` (one-off container)."""
    script = f'{_AWS} ls "s3://$LAKEHOUSE_BUCKET/{prefix}/"'
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
    # A missing prefix exits 1 with no output (legitimately empty); an aws error is never silent.
    assert (
        "aws: [ERROR]" not in proc.stderr and "An error occurred" not in proc.stderr
    ), proc.stderr[-500:]
    names = []
    for line in proc.stdout.splitlines():
        parts = line.split()
        if parts:
            names.append(parts[-1].rstrip("/"))
    return names


def checkpoint_progress(job: str, table: str, namespace: str) -> tuple[int, int]:
    """(planned batches, committed batches) of a Silver stream checkpoint."""
    base = f"checkpoints/{job}/{namespace}/{table}"
    planned = [n for n in s3_names(f"{base}/offsets") if n.isdigit()]
    committed = [n for n in s3_names(f"{base}/commits") if n.isdigit()]
    return len(planned), len(committed)


def by_brand(rows: list[Row]) -> Counter[str]:
    return Counter(r["brand_id"] for r in rows)
