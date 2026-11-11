"""Landing files -> Bronze end to end (W3.D13.3) against the core stack.

Requires ``docker compose --profile core up -d``; skips cleanly when the broker is unreachable.
Expectations come from the generator's landed bytes and manifests, never from job output.

Host-side S3 access has no credentials, so everything that touches the bucket runs in a one-off
``seaweedfs-init`` container (same pattern as ``conftest.purge_it_namespaces``): landing files are
generated with the ``*_local`` writers into a temp dir and copied to ``landing/<ns>/``; the Bronze
Delta table is copied out the same way and read with pyarrow (Bronze is append-only, so the
``add`` actions of the Delta log name exactly the live parquet files). Every object lives under
``it_<hex8>``, so the session purge removes it and the default data is never touched.
"""

from __future__ import annotations

import json
import subprocess
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

import pytest

from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.landing import (
    orders_data_key,
    products_data_key,
    write_batch_local,
    write_products_local,
)
from lakehouse.generator.orders import OrderBatch, customer_pools_from_cdc, generate_orders
from lakehouse.generator.products import generate_products

from .spark_jobs import REPO_ROOT, metrics, run_ok


if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

pa_parquet = pytest.importorskip("pyarrow.parquet")

integration = pytest.mark.integration

BRANDS = 3
CDC_EVENTS = 200
PRODUCTS_PER_BRAND = 10
ORDERS_PER_BATCH = 20
FIRST_WAVE = 11  # batches 1..11 (channel from batch 10 on)
TOTAL_BATCHES = 12
CHANNEL_FROM_BATCH = 10
DOCKER_TIMEOUT_S = 600


# --------------------------------------------------------------------------- docker / S3 helpers
def _container_aws(script: str, mount: Path) -> None:
    """Run ``script`` (aws CLI, env from the container) with ``mount`` available as /data."""
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
            f"{mount.resolve().as_posix()}:/data",
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
    assert (
        proc.returncode == 0
    ), f"container aws failed (exit {proc.returncode}): {proc.stderr[-400:]}"


_AWS = 'aws --endpoint-url "http://$S3_HOST:$S3_PORT" --no-cli-pager --only-show-errors s3 cp'


def _upload_landing(stage: Path, namespace: str) -> None:
    """Copy ``stage`` (contains ``landing/<brand>/...``) to ``landing/<namespace>/<brand>/...``."""
    script = f'{_AWS} /data/landing "s3://$LAKEHOUSE_BUCKET/landing/{namespace}/" --recursive'
    _container_aws(script, stage)


def _download_table(table: str, namespace: str, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    script = f'{_AWS} "s3://$LAKEHOUSE_BUCKET/bronze/{namespace}/{table}" /data --recursive'
    _container_aws(script, dest)


def _live_parquet_files(root: Path) -> list[Path]:
    """Parquet files that the Delta log of ``root`` adds (and does not remove)."""
    live: dict[str, Path] = {}
    for log in sorted((root / "_delta_log").glob("*.json")):
        for line in log.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            action = json.loads(line)
            if "add" in action:
                rel = unquote(action["add"]["path"])
                live[rel] = root / rel
            elif "remove" in action:
                live.pop(unquote(action["remove"]["path"]), None)
    return list(live.values())


def _read_rows(root: Path) -> list[dict[str, Any]]:
    """All live rows of a downloaded Bronze table; ``brand_id`` comes from the hive dir if needed."""
    rows: list[dict[str, Any]] = []
    for file in _live_parquet_files(root):
        part = {
            seg.split("=", 1)[0]: unquote(seg.split("=", 1)[1])
            for seg in file.relative_to(root).parts[:-1]
            if "=" in seg
        }
        for row in pa_parquet.read_table(file).to_pylist():
            rows.append({**part, **row})
    return rows


# --------------------------------------------------------------------------- expectations
@dataclass(frozen=True)
class Expected:
    """Landed key -> (brand, line count, batch_no or None for products), built from the bytes."""

    files: dict[str, tuple[str, int, int | None]]

    def lines_of(self, entity: str) -> int:
        return sum(n for k, (_b, n, _no) in self.files.items() if f"/{entity}/" in k)


def _prefixed(namespace: str, key: str) -> str:
    return key.replace("landing/", f"landing/{namespace}/", 1)


def _order_expectations(namespace: str, batches: Sequence[OrderBatch]) -> dict[str, Any]:
    out: dict[str, tuple[str, int, int | None]] = {}
    for b in batches:
        key = _prefixed(
            namespace, orders_data_key(b.brand_id, b.batch_date.isoformat(), b.batch_no)
        )
        assert b.data.count(b"\n") == b.manifest["line_count"], key  # manifest agrees with bytes
        out[key] = (b.brand_id, b.manifest["line_count"], b.batch_no)
    return out


def _stage(root: Path, batches: Sequence[OrderBatch]) -> None:
    for b in batches:
        write_batch_local(
            root, b.brand_id, b.batch_date.isoformat(), b.batch_no, b.data, b.manifest
        )


def _split_ref(ref: str) -> tuple[str, int]:
    key, _, line = ref.rpartition("#L")
    return key, int(line)


def ref_violations(
    rows: Sequence[dict[str, Any]], expected: dict[str, tuple[str, int, int | None]], namespace: str
) -> list[str]:
    """Everything wrong with the ``_source_ref`` / ``brand_id`` of ``rows`` (empty = all good)."""
    problems: list[str] = []
    refs = [str(r["_source_ref"]) for r in rows]
    for ref, n in Counter(refs).items():
        if n > 1:
            problems.append(f"duplicate ref {ref}")
    for row in rows:
        key, line = _split_ref(str(row["_source_ref"]))
        if key not in expected:
            problems.append(f"unknown key {key}")
            continue
        brand, count, _ = expected[key]
        if not 1 <= line <= count:
            problems.append(f"line {line} outside 1..{count} of {key}")
        if row["brand_id"] != brand:
            problems.append(f"brand {row['brand_id']} != path brand {brand} in {key}")
        if key.split("/")[1] != namespace:
            problems.append(f"key outside namespace: {key}")
    return problems


# --------------------------------------------------------------------------- scenario
@dataclass(frozen=True)
class Scenario:
    namespace: str
    expected_first: dict[str, tuple[str, int, int | None]]
    expected_all: dict[str, tuple[str, int, int | None]]
    products_expected: dict[str, tuple[str, int, int | None]]
    run1: dict[str, Any]
    run2: dict[str, Any]
    run3: dict[str, Any]
    snap1: dict[str, list[dict[str, Any]]]
    snap2: dict[str, list[dict[str, Any]]]
    snap3: dict[str, list[dict[str, Any]]]
    late_files: int
    late_lines: int


def _snapshot(namespace: str, tmp: Path, tag: str) -> dict[str, list[dict[str, Any]]]:
    snap: dict[str, list[dict[str, Any]]] = {}
    for table in ("raw_orders", "raw_products"):
        dest = tmp / f"{tag}_{table}"
        _download_table(table, namespace, dest)
        snap[table] = _read_rows(dest)
    return snap


@pytest.fixture(scope="module")
def scenario(bootstrap_servers: str, tmp_path_factory: pytest.TempPathFactory) -> Scenario:
    del bootstrap_servers  # only the stack-reachability gate (skips when the stack is down)
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    tmp = tmp_path_factory.mktemp("bronze_batch")
    config = GeneratorConfig(  # type: ignore[call-arg]
        _env_file=None, products_per_brand=PRODUCTS_PER_BRAND
    )
    pools = customer_pools_from_cdc(generate_cdc(config, brands=BRANDS, events=CDC_EVENTS))
    products = generate_products(config)
    run = generate_orders(
        config,
        customer_pools=pools,
        product_ids={b: ids for b, ids in products.product_ids().items() if b in pools},
        batches=TOTAL_BATCHES,
        orders_per_batch=ORDERS_PER_BATCH,
    )
    assert len(pools) == BRANDS
    first = [b for b in run.batches if b.batch_no <= FIRST_WAVE]
    late = [b for b in run.batches if b.batch_no > FIRST_WAVE]
    assert len(first) == BRANDS * FIRST_WAVE and len(late) == BRANDS

    stage1 = tmp / "stage1"
    date_str = products.batch_date.isoformat()
    products_expected: dict[str, tuple[str, int, int | None]] = {}
    for brand in pools:
        file = products.files[brand]
        write_products_local(stage1, brand, date_str, file.data, file.manifest)
        key = _prefixed(namespace, products_data_key(brand, date_str))
        assert file.data.count(b"\n") == file.manifest["line_count"] == PRODUCTS_PER_BRAND
        products_expected[key] = (brand, file.manifest["line_count"], None)
    _stage(stage1, first)
    _upload_landing(stage1, namespace)

    args = ["bronze_batch", "--namespace", namespace]
    run1 = metrics(run_ok(args))["queries"]
    snap1 = _snapshot(namespace, tmp, "s1")

    run2 = metrics(run_ok(["--skip-build", *args]))["queries"]
    snap2 = _snapshot(namespace, tmp, "s2")

    stage2 = tmp / "stage2"
    _stage(stage2, late)
    _upload_landing(stage2, namespace)
    run3 = metrics(run_ok(["--skip-build", *args]))["queries"]
    snap3 = _snapshot(namespace, tmp, "s3")

    expected_first = _order_expectations(namespace, first)
    return Scenario(
        namespace=namespace,
        expected_first=expected_first,
        expected_all={**expected_first, **_order_expectations(namespace, late)},
        products_expected=products_expected,
        run1=run1,
        run2=run2,
        run3=run3,
        snap1=snap1,
        snap2=snap2,
        snap3=snap3,
        late_files=len(late),
        late_lines=sum(b.manifest["line_count"] for b in late),
    )


def _per_file(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts[_split_ref(str(row["_source_ref"]))[0]] += 1
    return dict(counts)


# --------------------------------------------------------------------------- tests
@integration
def test_bronze_rows_per_file_equal_manifest_line_counts(scenario: Scenario) -> None:
    s = scenario
    assert len(s.expected_first) == BRANDS * FIRST_WAVE >= 11 * BRANDS
    orders = s.snap1["raw_orders"]
    assert _per_file(orders) == {k: n for k, (_b, n, _no) in s.expected_first.items()}
    # Defects and duplicates stay in Bronze: total is the sum of every landed line.
    assert len(orders) == sum(n for _b, n, _no in s.expected_first.values())
    assert s.run1["raw_orders"]["files"] == len(s.expected_first)
    assert s.run1["raw_orders"]["lines"] == len(orders)

    products = s.snap1["raw_products"]
    assert _per_file(products) == {k: n for k, (_b, n, _no) in s.products_expected.items()}
    assert len(products) == BRANDS * PRODUCTS_PER_BRAND
    assert s.run1["raw_products"]["files"] == BRANDS
    assert s.run1["raw_products"]["lines"] == len(products)

    per_brand: dict[str, int] = defaultdict(int)
    for brand, n, _no in s.expected_first.values():
        per_brand[brand] += n
    assert {b: c["lines"] for b, c in s.run1["raw_orders"]["per_brand"].items()} == dict(per_brand)


@integration
def test_source_refs_are_unique_existing_keys_with_path_brand(scenario: Scenario) -> None:
    s = scenario
    assert ref_violations(s.snap1["raw_orders"], s.expected_first, s.namespace) == []
    assert ref_violations(s.snap1["raw_products"], s.products_expected, s.namespace) == []
    # Every line number 1..n of every file is present exactly once.
    lines: dict[str, set[int]] = defaultdict(set)
    for row in s.snap1["raw_orders"]:
        key, line = _split_ref(str(row["_source_ref"]))
        lines[key].add(line)
    for key, (_brand, count, _no) in s.expected_first.items():
        assert lines[key] == set(range(1, count + 1)), key
    for row in s.snap1["raw_orders"] + s.snap1["raw_products"]:
        assert row["payload_brand_id"] == row["brand_id"]  # generator output is not cross-tenant


@integration
def test_sales_channel_is_null_before_batch_10_and_set_from_batch_10(scenario: Scenario) -> None:
    s = scenario
    seen_old = seen_new = 0
    for row in s.snap1["raw_orders"]:
        batch_no = s.expected_first[_split_ref(str(row["_source_ref"]))[0]][2]
        assert batch_no is not None
        if batch_no < CHANNEL_FROM_BATCH:
            assert row.get("sales_channel") is None, row["_source_ref"]
            seen_old += 1
        else:
            assert row.get("sales_channel") is not None, row["_source_ref"]
            seen_new += 1
    assert seen_old > 0 and seen_new > 0  # both sides of the evolution are exercised


@integration
def test_generator_output_has_no_parse_errors(scenario: Scenario) -> None:
    s = scenario
    for table in ("raw_orders", "raw_products"):
        assert s.run1[table]["parse_errors"] == 0, table
        assert [r for r in s.snap1[table] if r.get("_parse_error") is not None] == [], table


@integration
def test_rerun_without_new_files_adds_nothing(scenario: Scenario) -> None:
    s = scenario
    for table in ("raw_orders", "raw_products"):
        assert s.run2[table]["files"] == 0, table
        assert s.run2[table]["lines"] == 0, table
        assert len(s.snap2[table]) == len(s.snap1[table]), table
        assert {r["_source_ref"] for r in s.snap2[table]} == {
            r["_source_ref"] for r in s.snap1[table]
        }, table


@integration
def test_new_batch_adds_only_its_own_lines(scenario: Scenario) -> None:
    s = scenario
    before = len(s.snap2["raw_orders"])
    assert s.run3["raw_orders"]["files"] == s.late_files == BRANDS
    assert s.run3["raw_orders"]["lines"] == s.late_lines
    assert s.run3["raw_products"]["files"] == 0 and s.run3["raw_products"]["lines"] == 0
    assert len(s.snap3["raw_orders"]) == before + s.late_lines
    assert len(s.snap3["raw_products"]) == len(s.snap2["raw_products"])
    assert _per_file(s.snap3["raw_orders"]) == {k: n for k, (_b, n, _no) in s.expected_all.items()}
    assert ref_violations(s.snap3["raw_orders"], s.expected_all, s.namespace) == []
    old_refs = {r["_source_ref"] for r in s.snap2["raw_orders"]}
    new_rows = [r for r in s.snap3["raw_orders"] if r["_source_ref"] not in old_refs]
    assert len(new_rows) == s.late_lines
    assert all(r["sales_channel"] is not None for r in new_rows)  # batch 12 >= 10


# Negative tests: the reference checks detect a violation (no stack needed).
def test_ref_check_detects_duplicate_wrong_brand_and_unknown_key() -> None:
    ns = "it_00000000"
    key = f"landing/{ns}/brand_alpha/orders/2026-01-01/batch-00001.ndjson"
    expected = {key: ("brand_alpha", 2, 1)}
    good = [
        {"_source_ref": f"{key}#L1", "brand_id": "brand_alpha"},
        {"_source_ref": f"{key}#L2", "brand_id": "brand_alpha"},
    ]
    assert ref_violations(good, expected, ns) == []
    dup = [*good, good[0]]
    assert any("duplicate" in p for p in ref_violations(dup, expected, ns))
    wrong_brand = [{**good[0], "brand_id": "brand_beta"}]
    assert any("path brand" in p for p in ref_violations(wrong_brand, expected, ns))
    beyond = [{"_source_ref": f"{key}#L3", "brand_id": "brand_alpha"}]
    assert any("outside" in p for p in ref_violations(beyond, expected, ns))
    ghost = [{"_source_ref": f"{key}x#L1", "brand_id": "brand_alpha"}]
    assert any("unknown key" in p for p in ref_violations(ghost, expected, ns))
