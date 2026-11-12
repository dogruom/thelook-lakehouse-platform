"""Tenant isolation over Silver (W3.D18, D-085) against the core stack.

Silver is built from seeded CDC and landed orders for three brands (the CDC generator plants
cross-tenant customer-id collisions). ``isolation_check`` creates temp views and verifies in Spark
that each view holds one brand, that the view counts add up, that the per-brand counts equal the
expected JSON and that a leaky join is detected. The host side repeats the count/brand checks from
the Silver snapshots and proves the collision data is not vacuous. Budget: about 7 jobs, at most
6 minutes.
"""

from __future__ import annotations

import json
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from .delta_io import temp_topics
from .order_flow import Row, run, snapshot
from .pipeline_flow import World, build_world, by_brand, load_bronze, produce_world
from .spark_jobs import submit


if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

pytestmark = pytest.mark.integration

BRANDS = 3
#: 10 % of the ~32 customers of brands 2..3 reuse an earlier brand's id (the 1 % default
#: rounds to zero collisions at this size, which made the leaky-join check vacuous).
COLLISION_RATE_PCT = 10.0


def cross_brand_pairs(left: list[Row], right: list[Row], key: str, *, composite: bool) -> int:
    """Joined pairs whose two sides belong to different brands (0 for a tenant-safe join)."""
    index: dict[tuple[str, ...], list[Row]] = defaultdict(list)
    for r in right:
        index[(r["brand_id"], r[key]) if composite else (r[key],)].append(r)
    pairs = 0
    for row in left:
        lookup = (row["brand_id"], row[key]) if composite else (row[key],)
        pairs += sum(1 for m in index.get(lookup, []) if m["brand_id"] != row["brand_id"])
    return pairs


@dataclass
class Scenario:
    world: World
    namespace: str
    expected: dict[str, dict[str, int]]
    snaps: dict[str, list[Row]]
    check_code: int
    check_out: str
    bad_code: int
    bad_out: str


@pytest.fixture(scope="module")
def scenario(
    bootstrap_servers: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Scenario]:
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    tmp: Path = tmp_path_factory.mktemp("isolation")
    world = build_world(
        brands=BRANDS,
        cdc_events=240,
        click_events=0,
        order_batches=3,
        orders_per_batch=20,
        cdc_cross_tenant_collision_rate=COLLISION_RATE_PCT,
    )
    with temp_topics(bootstrap_servers, "ti_cdc", "ti_click") as (cdc_topic, click_topic):
        produce_world(bootstrap_servers, world, cdc_topic, click_topic)
        load_bronze(world, namespace, cdc_topic, click_topic, tmp)
    run(["silver_customers", "--namespace", namespace])
    run(["silver_orders", "--namespace", namespace])
    truth = world.orders_truth()
    expected = {
        "customers": dict(world.customers_final()),
        "orders": dict(truth["silver_accepted"]),
        "order_items": dict(truth["items"]),
    }
    snaps = {t: snapshot("silver", t, namespace, tmp, f"i_{t}") for t in expected}
    code, out = submit(
        [
            "--skip-build",
            "isolation_check",
            "--namespace",
            namespace,
            "--expected-json",
            json.dumps(expected),
        ]
    )
    wrong = {t: {b: n + 1 for b, n in c.items()} for t, c in expected.items()}
    bad_code, bad_out = submit(
        [
            "--skip-build",
            "isolation_check",
            "--namespace",
            namespace,
            "--expected-json",
            json.dumps(wrong),
        ]
    )
    yield Scenario(world, namespace, expected, snaps, code, out, bad_code, bad_out)


def test_isolation_check_job_passes_on_the_clean_silver(scenario: Scenario) -> None:
    assert scenario.check_code == 0, scenario.check_out[-2000:]


def test_per_brand_counts_equal_the_manifest(scenario: Scenario) -> None:
    for table, want in scenario.expected.items():
        assert dict(by_brand(scenario.snaps[table])) == want, table
        assert sum(want.values()) == len(scenario.snaps[table]), table


def test_every_brand_view_holds_exactly_its_own_rows(scenario: Scenario) -> None:
    for table, rows in scenario.snaps.items():
        for brand in scenario.expected[table]:
            view = [r for r in rows if r["brand_id"] == brand]  # what v_<table>__<brand> selects
            assert {r["brand_id"] for r in view} == {brand}
        assert sum(
            len([r for r in rows if r["brand_id"] == b]) for b in set(by_brand(rows))
        ) == len(rows)


def test_collisions_exist_so_that_a_leaky_join_is_detectable(scenario: Scenario) -> None:
    assert scenario.world.cdc.manifest.counts["cross_tenant_collision"] > 0
    customers = scenario.snaps["customers"]
    ids = Counter(r["customer_id"] for r in customers)
    assert any(n > 1 for n in ids.values()), "no customer id shared by two brands"
    assert cross_brand_pairs(customers, customers, "customer_id", composite=False) > 0


def test_composite_brand_join_has_no_cross_tenant_pairs(scenario: Scenario) -> None:
    customers = scenario.snaps["customers"]
    assert cross_brand_pairs(customers, customers, "customer_id", composite=True) == 0
    orders = scenario.snaps["orders"]
    assert cross_brand_pairs(orders, customers, "customer_id", composite=True) == 0


def test_isolation_check_fails_when_an_expected_count_is_wrong(scenario: Scenario) -> None:
    assert scenario.bad_code != 0, scenario.bad_out[-2000:]


def test_cross_brand_pair_counter_detects_a_leak_and_passes_a_clean_join() -> None:
    a = [{"brand_id": "x", "customer_id": "c1"}]
    b = [{"brand_id": "y", "customer_id": "c1"}]
    assert cross_brand_pairs(a, b, "customer_id", composite=False) == 1
    assert cross_brand_pairs(a, b, "customer_id", composite=True) == 0
    assert cross_brand_pairs(a, a, "customer_id", composite=False) == 0
