"""Orders + products generator: contract, determinism, tenancy, evolution, defects (W3.D13.1).

Derived from DATA_CONTRACTS.md 2.2-2.4. All implementation-specific names live in the ADAPTER
block; adjust only that block if the public API differs.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from lakehouse.contracts.models import OrderEvent, Product
from lakehouse.generator.config import GeneratorConfig, defect_count

# ---------------------------------------------------------------- ADAPTER (assumed API)
from lakehouse.generator.orders import generate_orders
from lakehouse.generator.products import generate_products


BRANDS = ["brand_alpha", "brand_beta"]
BATCHES = 12
PER_BATCH = 100  # orders per brand per batch (before duplicates are appended)
EVOLUTION_BATCH = 10
NON_DUP_RATES = {
    "orphan_fk": "order_orphan_fk_rate",
    "future_ts": "order_future_ts_rate",
    "negative_amount": "order_negative_amount_rate",
}


def make(**kw: Any) -> GeneratorConfig:
    return GeneratorConfig(_env_file=None, tenants=BRANDS, tenant_weights=[0.5, 0.5], **kw)  # type: ignore[call-arg]


def pools_for(brands: list[str]) -> dict[str, list[str]]:
    """Deterministic fake customer pools: brand -> uuid4 strings."""
    return {
        b: [str(uuid.UUID(int=(i + 1) * 10**6 + n, version=4)) for n in range(50)]
        for i, b in enumerate(brands)
    }


def gen_products(cfg: GeneratorConfig) -> dict[str, tuple[bytes, dict[str, Any]]]:
    """-> {brand_id: (ndjson_bytes, manifest_dict)}"""
    run = generate_products(cfg)
    return {b: (f.data, f.manifest) for b, f in run.files.items()}


def gen_orders(
    cfg: GeneratorConfig, pools: dict[str, list[str]], products: dict[str, list[str]]
) -> list[tuple[str, int, bytes, dict[str, Any]]]:
    """-> [(brand_id, batch_no (1-based), ndjson_bytes, manifest_dict)]"""
    run = generate_orders(
        cfg,
        customer_pools=pools,
        product_ids=products,
        batches=BATCHES,
        orders_per_batch=PER_BATCH,
    )
    return [(b.brand_id, b.batch_no, b.data, b.manifest) for b in run.batches]


# ---------------------------------------------------------------- helpers
def lines(data: bytes) -> list[str]:
    assert data.endswith(b"\n")
    parts = data.decode("utf-8").split("\n")[:-1]
    assert all(parts), "blank line in NDJSON"
    return parts


def product_ids_by_brand(cfg: GeneratorConfig) -> dict[str, list[str]]:
    return {
        b: [json.loads(ln)["product_id"] for ln in lines(data)]
        for b, (data, _m) in gen_products(cfg).items()
    }


def defects_of(manifest: dict[str, Any]) -> dict[int, str]:
    return {e["line_no"]: e["defect"] for e in manifest["entries"] if e.get("defect")}


@pytest.fixture(scope="module")
def world() -> dict[str, Any]:
    cfg = make(seed=7)
    prods = product_ids_by_brand(cfg)
    pools = pools_for(BRANDS)
    return {"cfg": cfg, "prods": prods, "pools": pools, "batches": gen_orders(cfg, pools, prods)}


def clean_lines(world: dict[str, Any]) -> list[tuple[str, int, dict[str, Any]]]:
    out = []
    for brand, batch_no, data, manifest in world["batches"]:
        defects = defects_of(manifest)
        for i, ln in enumerate(lines(data), start=1):
            if i not in defects:
                out.append((brand, batch_no, json.loads(ln)))
    return out


# ---------------------------------------------------------------- determinism
def test_orders_same_seed_is_byte_identical(world: dict[str, Any]) -> None:
    assert gen_orders(make(seed=7), world["pools"], world["prods"]) == world["batches"]


def test_orders_different_seed_differs(world: dict[str, Any]) -> None:
    other_cfg = make(seed=8)
    other = gen_orders(other_cfg, world["pools"], product_ids_by_brand(other_cfg))
    assert [b[2] for b in other] != [b[2] for b in world["batches"]]


def test_products_deterministic_and_seed_sensitive() -> None:
    assert gen_products(make(seed=1)) == gen_products(make(seed=1))
    assert gen_products(make(seed=1)) != gen_products(make(seed=2))


# ---------------------------------------------------------------- products contract
def test_products_validate_and_carry_path_brand() -> None:
    files = gen_products(make(seed=3))
    assert set(files) == set(BRANDS)
    for brand, (data, manifest) in files.items():
        rows = lines(data)
        assert manifest["line_count"] == len(rows) > 0
        ids = []
        for ln in rows:
            raw = json.loads(ln)
            p = Product.model_validate(raw)
            assert p.schema_version == 1
            assert p.brand_id == brand
            assert len(raw["retail_price"].split(".")[1]) == 2
            ids.append(p.product_id)
        assert len(ids) == len(set(ids)), "product_id unique within brand"


# ---------------------------------------------------------------- order contract
def test_clean_orders_validate_against_contract(world: dict[str, Any]) -> None:
    clean = clean_lines(world)
    assert clean
    for brand, _b, raw in clean:
        o = OrderEvent.model_validate(raw)
        assert o.schema_version == 1
        assert o.brand_id == brand
        assert 1 <= len(o.items) <= 5
        for key in ("gross_amount", "discount_amount", "net_amount"):
            assert len(raw[key].split(".")[1]) == 2, key
        assert sum((i.line_total for i in o.items), Decimal(0)) == o.gross_amount
        assert o.net_amount == o.gross_amount - o.discount_amount
        for item in raw["items"]:
            assert Decimal(item["line_total"]) == item["quantity"] * Decimal(item["unit_price"])
            assert item["brand_id"] == brand


def test_clean_order_items_reference_same_brand_products(world: dict[str, Any]) -> None:
    for brand, _b, raw in clean_lines(world):
        for item in raw["items"]:
            assert item["product_id"] in world["prods"][brand]


def test_every_line_brand_matches_batch_brand(world: dict[str, Any]) -> None:
    for brand, _b, data, _m in world["batches"]:
        assert all(json.loads(ln)["brand_id"] == brand for ln in lines(data))


def test_batches_cover_each_brand_with_one_based_numbering(world: dict[str, Any]) -> None:
    for brand in BRANDS:
        nos = sorted(b[1] for b in world["batches"] if b[0] == brand)
        assert nos == list(range(1, BATCHES + 1))


# ---------------------------------------------------------------- tenancy / FK
def test_clean_orders_customer_in_own_brand_pool(world: dict[str, Any]) -> None:
    for brand, _b, raw in clean_lines(world):
        assert raw["customer_id"] in world["pools"][brand]


def test_orphan_fk_ids_are_in_no_pool(world: dict[str, Any]) -> None:
    all_ids = {c for p in world["pools"].values() for c in p}
    seen = 0
    for _brand, _b, data, manifest in world["batches"]:
        rows = lines(data)
        for ln_no, defect in defects_of(manifest).items():
            if defect == "orphan_fk":
                seen += 1
                assert json.loads(rows[ln_no - 1])["customer_id"] not in all_ids
    assert seen > 0


def foreign_customers(
    brand: str, orders: list[dict[str, Any]], pools: dict[str, list[str]]
) -> list[str]:
    """Order ids of ``brand`` whose customer belongs to another brand's pool (a tenancy leak)."""
    others = {c for b, pool in pools.items() if b != brand for c in pool}
    return [o["order_id"] for o in orders if o["customer_id"] in others]


def test_pool_check_flags_cross_tenant_customer(world: dict[str, Any]) -> None:
    """Negative: the leak check flags a planted foreign customer and passes generated orders."""
    pools = world["pools"]
    assert not set(pools["brand_alpha"]) & set(pools["brand_beta"])
    for brand in BRANDS:
        own = [raw for b, _n, raw in clean_lines(world) if b == brand]
        assert own
        assert foreign_customers(brand, own, pools) == []
        leaked = dict(
            own[0], customer_id=pools["brand_beta" if brand == "brand_alpha" else "brand_alpha"][0]
        )
        assert foreign_customers(brand, [*own, leaked], pools) == [leaked["order_id"]]


# ---------------------------------------------------------------- schema evolution
def test_sales_channel_absent_before_batch_10_present_from_10(world: dict[str, Any]) -> None:
    for _brand, batch_no, data, manifest in world["batches"]:
        defects = defects_of(manifest)
        for i, ln in enumerate(lines(data), start=1):
            raw = json.loads(ln)
            if batch_no < EVOLUTION_BATCH:
                assert "sales_channel" not in raw
            elif i not in defects:
                assert raw["sales_channel"] in {"web", "app", "store"}
    assert any(b[1] >= EVOLUTION_BATCH for b in world["batches"])


# ---------------------------------------------------------------- defects
def test_defect_counts_match_config_and_manifest(world: dict[str, Any]) -> None:
    cfg = world["cfg"]
    for brand, batch_no, data, manifest in world["batches"]:
        got = list(defects_of(manifest).values())
        assert manifest["line_count"] == len(lines(data))
        for defect in ("duplicate", *NON_DUP_RATES):
            assert got.count(defect) == manifest["counts"][defect], (brand, batch_no, defect)
        base = PER_BATCH  # defect counts are drawn on the PER_BATCH logical orders
        for defect, field in NON_DUP_RATES.items():
            assert got.count(defect) == defect_count(getattr(cfg, field), base), defect
        assert got.count("duplicate") == defect_count(cfg.order_duplicate_rate, base)


def test_run_level_defect_budget_is_exact_when_batches_are_not(world: dict[str, Any]) -> None:
    """n=50 with the default rates (1 % of 50 is 0.5): batches are inexact, the run total is not."""
    n, batches = 50, 8
    cfg = world["cfg"]
    run = generate_orders(
        cfg,
        customer_pools=world["pools"],
        product_ids=world["prods"],
        batches=batches,
        orders_per_batch=n,
    )
    rates = {**NON_DUP_RATES, "duplicate": "order_duplicate_rate"}
    for brand in BRANDS:
        mine = [b for b in run.batches if b.brand_id == brand]
        assert [b.batch_no for b in mine] == list(range(1, batches + 1))
        for defect, field in rates.items():
            rate = getattr(cfg, field)
            per_batch = [list(defects_of(b.manifest).values()).count(defect) for b in mine]
            assert per_batch == [b.manifest["counts"][defect] for b in mine]
            assert sum(per_batch) == defect_count(rate, batches * n), (brand, defect)
            assert all(abs(c - rate / 100 * n) <= 1 for c in per_batch), (brand, defect, per_batch)
    # the default 1 % negative_amount would be 0 in every batch if each batch were rounded alone
    assert defect_count(cfg.order_negative_amount_rate, n) == 0
    assert defect_count(cfg.order_negative_amount_rate, batches * n) == 4


def test_duplicates_are_byte_identical_to_earlier_line(world: dict[str, Any]) -> None:
    # A duplicate replays a clean line of the same batch or of the brand's previous batch
    # (cross-file dedup demo); the manifest names the source batch in ``duplicate_of``.
    by_batch = {(brand, b): lines(data) for brand, b, data, _m in world["batches"]}
    seen = {"same": 0, "previous": 0}
    for brand, b, data, manifest in world["batches"]:
        rows = lines(data)
        for entry in manifest["entries"]:
            if entry["defect"] != "duplicate":
                continue
            ln_no = entry["line_no"]
            source_batch = entry["duplicate_of"]["batch_no"]
            if source_batch == b:
                seen["same"] += 1
                assert rows[ln_no - 1] in rows[: ln_no - 1]
            else:
                assert source_batch == b - 1
                seen["previous"] += 1
                assert rows[ln_no - 1] in by_batch[(brand, source_batch)]
    assert seen["same"] > 0
    assert seen["previous"] > 0


def test_future_and_negative_lines_are_valid_json_with_expected_shape(
    world: dict[str, Any],
) -> None:
    sim_start = world["cfg"].sim_start
    seen = {"future_ts": 0, "negative_amount": 0}
    for _brand, _b, data, manifest in world["batches"]:
        rows = lines(data)
        for ln_no, defect in defects_of(manifest).items():
            if defect in seen:
                raw = json.loads(rows[ln_no - 1])
                seen[defect] += 1
                if defect == "future_ts":
                    ts = datetime.fromisoformat(raw["order_timestamp"].replace("Z", "+00:00"))
                    assert ts.astimezone(UTC) > sim_start
                else:
                    assert Decimal(raw["gross_amount"]) < 0
    assert all(v > 0 for v in seen.values())


def test_manifest_defect_line_numbers_are_in_range_and_unique(world: dict[str, Any]) -> None:
    for _brand, _b, data, manifest in world["batches"]:
        nos = [e["line_no"] for e in manifest["entries"] if e.get("defect")]
        assert len(nos) == len(set(nos))
        assert all(1 <= x <= len(lines(data)) for x in nos)


def test_negative_amount_lines_fail_contract(world: dict[str, Any]) -> None:
    """Negative: the injected defect is real, i.e. the contract rejects it."""
    checked = 0
    for _brand, _b, data, manifest in world["batches"]:
        rows = lines(data)
        for ln_no, defect in defects_of(manifest).items():
            if defect == "negative_amount":
                with pytest.raises(ValidationError):
                    OrderEvent.model_validate(json.loads(rows[ln_no - 1]))
                checked += 1
    assert checked > 0
