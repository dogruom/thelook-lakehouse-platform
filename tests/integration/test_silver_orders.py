"""Orders -> Silver end to end (W3.D17.2) against the core stack, compared with the generator
manifests (DATA_CONTRACTS 2.2, 3.2, 4.3).

Flow: seeded CDC -> ``bronze_stream`` -> ``silver_customers``; landed products and orders ->
``bronze_batch`` -> ``dlq_gate --table raw_orders --table raw_products`` -> ``silver_orders`` (twice).
Expectations come from the landed bytes and manifests, never from job output. Tables are copied out
through the ``seaweedfs-init`` container and read with pyarrow.

Budget: about 6 jobs and 5 table copies, roughly 6-8 minutes. Requires
``docker compose --profile core up -d``; skips cleanly when the broker is unreachable.
"""

from __future__ import annotations

import json
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.landing import products_data_key
from lakehouse.generator.messages import cdc_messages
from lakehouse.generator.orders import customer_pools_from_cdc, generate_orders
from lakehouse.generator.products import generate_products

from .delta_io import temp_topics
from .order_flow import (
    Row,
    customers_round,
    land,
    prefixed,
    run,
    silver_snapshot,
    violations,
)
from .spark_jobs import metrics


if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

BRANDS = 2
CDC_EVENTS = 200
PRODUCTS_PER_BRAND = 10
ORDERS_PER_BATCH = 25
BATCHES = 4
COUNTER_FIELDS = (
    "rows_read",
    "reread_rows",
    "parse_excluded",
    "dlq_excluded",
    "duplicates_removed",
    "fk_orphans",
    "late_beyond_window",
    "silver_accepted",
    "items_inserted",
)
DEFECT_CATEGORY = {
    "orphan_fk": "REFERENTIAL_INTEGRITY_VIOLATION",
    "future_ts": "TIMESTAMP_OUT_OF_BOUNDS",
    "negative_amount": "BUSINESS_RULE_VIOLATION",
}


@dataclass(frozen=True)
class Truth:
    """Ground truth derived from the landed bytes and the order manifests."""

    namespace: str
    lines: int
    totals: dict[str, int]
    accepted: dict[str, set[str]]  # brand -> accepted order ids
    items: dict[str, int]  # brand -> items of accepted orders
    amounts: dict[tuple[str, str], tuple[Decimal, Decimal]]  # (brand, order) -> (gross, net)
    dlq_refs: dict[str, str]  # Bronze ref -> defect
    dlq_per_brand: dict[str, int]
    duplicates_per_brand: dict[str, int]


@dataclass(frozen=True)
class Scenario:
    truth: Truth
    run1: dict[str, Any]
    gate: dict[str, Any]
    snap1: dict[str, list[Row]]
    run2: dict[str, Any]
    snap2: dict[str, list[Row]]


def _truth(namespace: str, batches: list[Any]) -> Truth:
    totals: Counter[str] = Counter()
    accepted: dict[str, set[str]] = defaultdict(set)
    items: dict[str, int] = defaultdict(int)
    amounts: dict[tuple[str, str], tuple[Decimal, Decimal]] = {}
    dlq_refs: dict[str, str] = {}
    dlq_per_brand: dict[str, int] = defaultdict(int)
    duplicates: dict[str, int] = defaultdict(int)
    lines = 0
    for b in batches:
        m = b.manifest
        lines += m["line_count"]
        for name, value in m["counts"].items():
            totals[name] += value
        key = prefixed(namespace, m["data_key"])
        bad_ids = {e["order_id"] for e in m["entries"] if e["defect"] != "duplicate"}
        for e in m["entries"]:
            if e["defect"] == "duplicate":
                duplicates[b.brand_id] += 1
            else:
                dlq_refs[f"{key}#L{e['line_no']}"] = e["defect"]
                dlq_per_brand[b.brand_id] += 1
        seen: set[str] = set()
        for raw in b.data.decode("utf-8").splitlines():
            order = json.loads(raw)
            oid = order["order_id"]
            if oid in bad_ids or oid in seen or oid in accepted[b.brand_id]:
                continue
            seen.add(oid)
            accepted[b.brand_id].add(oid)
            items[b.brand_id] += len(order["items"])
            amounts[(b.brand_id, oid)] = (
                Decimal(order["gross_amount"]),
                Decimal(order["net_amount"]),
            )
    return Truth(
        namespace=namespace,
        lines=lines,
        totals=dict(totals),
        accepted=dict(accepted),
        items=dict(items),
        amounts=amounts,
        dlq_refs=dlq_refs,
        dlq_per_brand=dict(dlq_per_brand),
        duplicates_per_brand=dict(duplicates),
    )


@pytest.fixture(scope="module")
def scenario(
    bootstrap_servers: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Scenario]:
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    tmp = tmp_path_factory.mktemp("silver_orders")
    config = GeneratorConfig(  # type: ignore[call-arg]
        _env_file=None, products_per_brand=PRODUCTS_PER_BRAND
    )
    cdc = generate_cdc(config, brands=BRANDS, events=CDC_EVENTS)
    pools = customer_pools_from_cdc(cdc)
    assert len(pools) == BRANDS
    products = generate_products(config)
    orders = generate_orders(
        config,
        customer_pools=pools,
        product_ids={b: ids for b, ids in products.product_ids().items() if b in pools},
        batches=BATCHES,
        orders_per_batch=ORDERS_PER_BATCH,
    )
    truth = _truth(namespace, orders.batches)

    landed: dict[str, bytes] = {}
    date_str = products.batch_date.isoformat()
    for brand in pools:
        landed[products_data_key(brand, date_str)] = products.files[brand].data
    for b in orders.batches:
        landed[b.manifest["data_key"]] = b.data

    with temp_topics(bootstrap_servers, "so_cdc", "so_click") as (cdc_topic, click_topic):
        customers_round(
            bootstrap_servers, cdc_messages(cdc, cdc_topic), namespace, cdc_topic, click_topic
        )
    land(tmp / "stage", namespace, landed)
    run(["bronze_batch", "--namespace", namespace])
    gate = metrics(
        run(
            [
                "dlq_gate",
                "--namespace",
                namespace,
                "--table",
                "raw_orders",
                "--table",
                "raw_products",
            ]
        )
    )
    run1 = metrics(run(["silver_orders", "--namespace", namespace]))
    snap1 = silver_snapshot(namespace, tmp, "s1")
    run2 = metrics(run(["silver_orders", "--namespace", namespace]))
    snap2 = silver_snapshot(namespace, tmp, "s2")
    yield Scenario(truth, run1, gate, snap1, run2, snap2)


def _items_of(brand_rows: list[Row], brand: str) -> list[Row]:
    return [r for r in brand_rows if r["brand_id"] == brand]


# ---- the generator exercises every defect (a vacuous run would prove nothing) ----------------


def test_generated_run_contains_every_defect_kind(scenario: Scenario) -> None:
    totals = scenario.truth.totals
    for name in ("orphan_fk", "future_ts", "negative_amount", "duplicate", "clean"):
        assert totals[name] > 0, f"seeded run has no {name}"


# ---- counters versus manifests (exact) -------------------------------------------------------


def test_counters_equal_the_manifest_totals_exactly(scenario: Scenario) -> None:
    t = scenario.truth.totals
    counts = scenario.run1["counts"]
    assert counts["rows_read"] == scenario.truth.lines
    assert counts["silver_accepted"] == t["clean"]
    assert counts["fk_orphans"] == t["orphan_fk"]
    assert counts["dlq_excluded"] == t["future_ts"] + t["negative_amount"]
    assert counts["duplicates_removed"] == t["duplicate"]
    assert counts["parse_excluded"] == 0 and counts["reread_rows"] == 0
    assert counts["late_beyond_window"] == 0
    assert counts["rows_read"] == (
        counts["silver_accepted"]
        + counts["fk_orphans"]
        + counts["dlq_excluded"]
        + counts["duplicates_removed"]
        + counts["parse_excluded"]
    )


def test_items_inserted_equals_the_items_of_the_accepted_orders(scenario: Scenario) -> None:
    assert scenario.run1["counts"]["items_inserted"] == sum(scenario.truth.items.values())


def test_gate_rejects_exactly_the_non_retryable_defects(scenario: Scenario) -> None:
    t = scenario.truth.totals
    assert scenario.gate["tables"]["raw_orders"]["rejected_rows"] == (
        t["future_ts"] + t["negative_amount"]
    )
    assert scenario.gate["tables"]["raw_products"]["rejected_rows"] == 0


# ---- Silver content --------------------------------------------------------------------------


def test_silver_orders_and_items_equal_the_manifest_accepted_set_per_brand(
    scenario: Scenario,
) -> None:
    snap = scenario.snap1
    assert (
        violations(scenario.truth.accepted, scenario.truth.items, snap["orders"], snap["items"])
        == []
    )
    assert len(snap["orders"]) == scenario.truth.totals["clean"]


def test_silver_amounts_equal_the_landed_amounts(scenario: Scenario) -> None:
    for row in scenario.snap1["orders"]:
        gross, net = scenario.truth.amounts[(row["brand_id"], row["order_id"])]
        assert (Decimal(row["gross_amount"]), Decimal(row["net_amount"])) == (gross, net)
        assert gross > 0 and net <= gross


def test_every_silver_row_names_a_tenant_and_every_item_matches_its_order_brand(
    scenario: Scenario,
) -> None:
    for row in [*scenario.snap1["orders"], *scenario.snap1["items"]]:
        assert row["brand_id"] in scenario.truth.accepted
    orders = {(r["brand_id"], r["order_id"]) for r in scenario.snap1["orders"]}
    assert all((r["brand_id"], r["order_id"]) in orders for r in scenario.snap1["items"])


def test_duplicates_leave_no_second_row_per_brand(scenario: Scenario) -> None:
    assert (
        sum(scenario.truth.duplicates_per_brand.values())
        == (scenario.run1["counts"]["duplicates_removed"])
    )
    for brand, accepted in scenario.truth.accepted.items():
        ids = [r["order_id"] for r in _items_of(scenario.snap1["orders"], brand)]
        assert len(ids) == len(set(ids)) == len(accepted)


# ---- DLQ -------------------------------------------------------------------------------------


def test_dlq_holds_exactly_the_manifest_defects_with_contract_categories(
    scenario: Scenario,
) -> None:
    dlq = scenario.snap1["dlq"]
    got = {r["_bronze_ref"]: r for r in dlq}
    assert len(got) == len(dlq), "two DLQ rows for one Bronze ref"
    assert set(got) == set(scenario.truth.dlq_refs)
    for ref, defect in scenario.truth.dlq_refs.items():
        row = got[ref]
        assert row["_error_category"] == DEFECT_CATEGORY[defect], ref
        assert row["_retryable"] is (defect == "orphan_fk"), ref
        assert (row["_replay_status"], row["_replay_count"]) == ("PENDING", 0), ref
    assert Counter(r["brand_id"] for r in dlq) == Counter(scenario.truth.dlq_per_brand)
    assert len({r["_dlq_id"] for r in dlq}) == len(dlq)


def test_dlq_codes_name_the_failing_rule(scenario: Scenario) -> None:
    by_ref = {r["_bronze_ref"]: r for r in scenario.snap1["dlq"]}
    for ref, defect in scenario.truth.dlq_refs.items():
        codes = list(by_ref[ref]["_rejection_codes"])
        if defect == "future_ts":
            assert codes == ["order_timestamp_in_future"], ref
        elif defect == "negative_amount":
            assert codes == ["gross_amount_not_positive"], ref
        else:
            assert len(codes) == 1 and "customer" in codes[0], ref


# ---- re-run ----------------------------------------------------------------------------------


def test_rerun_adds_nothing_and_changes_nothing(scenario: Scenario) -> None:
    counts = scenario.run2.get("counts", {})
    assert counts.get("silver_accepted", 0) == 0
    assert counts.get("items_inserted", 0) == 0
    assert counts.get("fk_orphans", 0) == 0
    for table, rows in scenario.snap1.items():
        assert sorted(map(repr, scenario.snap2[table])) == sorted(map(repr, rows)), table


# ---- negative tests: the checks above detect a violation (no stack needed) --------------------


def _good() -> tuple[dict[str, set[str]], dict[str, int], list[Row], list[Row]]:
    orders = [
        {"brand_id": "a", "order_id": "o1"},
        {"brand_id": "b", "order_id": "o1"},
    ]
    items = [
        {"brand_id": "a", "order_id": "o1", "item_id": "i1"},
        {"brand_id": "b", "order_id": "o1", "item_id": "i1"},
    ]
    return {"a": {"o1"}, "b": {"o1"}}, {"a": 1, "b": 1}, orders, items


def test_violation_check_accepts_the_expected_state() -> None:
    assert violations(*_good()) == []


def test_violation_check_detects_a_duplicate_key_a_missing_order_and_a_leak() -> None:
    exp_o, exp_i, orders, items = _good()
    assert any("duplicate" in p for p in violations(exp_o, exp_i, [*orders, orders[0]], items))
    assert any("missing=1" in p for p in violations(exp_o, exp_i, orders[:1], items))
    leaked = [*orders, {"brand_id": "a", "order_id": "o2"}]
    assert any("unexpected=1" in p for p in violations(exp_o, exp_i, leaked, items))
    cross = [{"brand_id": "b", "order_id": "o1", "item_id": "i1"}, items[0]]
    assert violations(exp_o, exp_i, orders, cross) == []  # same item id in two brands is fine
    foreign = [*items, {"brand_id": "a", "order_id": "o9", "item_id": "i9"}]
    assert any("no parent" in p for p in violations(exp_o, {**exp_i, "a": 2}, orders, foreign))
    assert any(
        "items 1 != expected 2" in p for p in violations(exp_o, {**exp_i, "a": 2}, orders, items)
    )
