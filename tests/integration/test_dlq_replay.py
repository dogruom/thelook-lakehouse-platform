"""Controlled DLQ replay of a late-arriving parent (W3.D17.3) against the core stack.

Scenario (brand_alpha, one landed orders file, expectations crafted here, never from job output):

* L1 ``ORDER_LATE``: its customer's ``I`` is produced only after the first Silver run;
* L2 ``ORDER_GHOST``: its customer never arrives (a true orphan);
* L3 ``ORDER_OK``: its customer exists from the start.

Round 1 (customer OK -> bronze_stream -> silver_customers; land -> bronze_batch -> dlq_gate ->
silver_orders) leaves L1 and L2 as retryable ``PENDING`` DLQ rows. Round 2 publishes the late
customer and runs ``replay`` twice: the late order and its items reach Silver exactly once, its DLQ
row becomes ``REPLAYED``; the ghost stays ``FAILED_AGAIN``; a second replay changes no Silver row
and creates no DLQ row (DATA_CONTRACTS 3.2: "replaying the same subset twice creates no Silver
duplicates").

Budget: about 9 jobs and 6 table copies, roughly 7-9 minutes. Requires
``docker compose --profile core up -d``; skips cleanly when the broker is unreachable.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from ..spark.order_data import (
    ALPHA,
    C1,
    C2,
    C3,
    DAY,
    O1,
    O2,
    O3,
    P1,
    P2,
    P3,
    dump,
    order,
    order_path,
    product,
    product_path,
)
from .delta_io import temp_topics
from .order_flow import (
    Row,
    cdc_message,
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

CUSTOMER_OK, CUSTOMER_LATE, CUSTOMER_GHOST = C1, C2, C3
ORDER_LATE, ORDER_GHOST, ORDER_OK = O1, O2, O3
MAX_REPLAYS = 3


@dataclass(frozen=True)
class Replay:
    namespace: str
    refs: dict[str, str]  # label -> Bronze ref
    before: dict[str, list[Row]]  # after the first silver_orders run
    after1: dict[str, list[Row]]  # after replay 1
    after2: dict[str, list[Row]]  # after replay 2
    replay1: dict[str, Any]
    replay2: dict[str, Any]


def _by_ref(rows: list[Row]) -> dict[str, Row]:
    out = {r["_bronze_ref"]: r for r in rows}
    assert len(out) == len(rows), "two DLQ rows for one Bronze ref"
    return out


def _keys(rows: list[Row], *cols: str) -> set[tuple[Any, ...]]:
    return {tuple(r[c] for c in cols) for r in rows}


@pytest.fixture(scope="module")
def replay(bootstrap_servers: str, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Replay]:
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    tmp = tmp_path_factory.mktemp("dlq_replay")
    orders_file = "".join(
        dump(order(oid, customer=cust)) + "\n"
        for oid, cust in (
            (ORDER_LATE, CUSTOMER_LATE),
            (ORDER_GHOST, CUSTOMER_GHOST),
            (ORDER_OK, CUSTOMER_OK),
        )
    ).encode("utf-8")
    products_file = "".join(dump(product(p, brand=ALPHA)) + "\n" for p in (P1, P2, P3)).encode()
    key = prefixed(namespace, order_path(ALPHA, 1, DAY))
    refs = {"late": f"{key}#L1", "ghost": f"{key}#L2", "ok": f"{key}#L3"}

    with temp_topics(bootstrap_servers, "rp_cdc", "rp_click") as (cdc_topic, click_topic):
        customers_round(
            bootstrap_servers,
            [cdc_message(cdc_topic, ALPHA, CUSTOMER_OK, 1, 1)],
            namespace,
            cdc_topic,
            click_topic,
        )
        land(
            tmp / "stage",
            namespace,
            {order_path(ALPHA, 1, DAY): orders_file, product_path(ALPHA, DAY): products_file},
        )
        run(["bronze_batch", "--namespace", namespace])
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
        run(["silver_orders", "--namespace", namespace])
        before = silver_snapshot(namespace, tmp, "before")

        customers_round(
            bootstrap_servers,
            [cdc_message(cdc_topic, ALPHA, CUSTOMER_LATE, 1, 2)],  # the late parent's I
            namespace,
            cdc_topic,
            click_topic,
        )
    args = ["replay", "--table", "orders", "--namespace", namespace]
    replay1 = metrics(run(args))
    after1 = silver_snapshot(namespace, tmp, "after1")
    replay2 = metrics(run(args))
    after2 = silver_snapshot(namespace, tmp, "after2")
    yield Replay(namespace, refs, before, after1, after2, replay1, replay2)


# ---- before the replay: the DLQ holds both, Silver only the healthy order --------------------


def test_before_replay_late_and_ghost_orders_are_pending_retryable_dlq_rows(replay: Replay) -> None:
    dlq = _by_ref(replay.before["dlq"])
    assert set(dlq) == {replay.refs["late"], replay.refs["ghost"]}
    for row in dlq.values():
        assert row["brand_id"] == ALPHA
        assert row["_error_category"] == "REFERENTIAL_INTEGRITY_VIOLATION"
        assert row["_retryable"] is True
        assert (row["_replay_status"], row["_replay_count"]) == ("PENDING", 0)
        assert row["event_id"] is None


def test_before_replay_only_the_healthy_order_is_in_silver(replay: Replay) -> None:
    assert (
        violations({ALPHA: {ORDER_OK}}, {ALPHA: 2}, replay.before["orders"], replay.before["items"])
        == []
    )


# ---- after replay 1 --------------------------------------------------------------------------


def test_replay_recovers_the_late_order_and_its_items_exactly_once(replay: Replay) -> None:
    assert (
        violations(
            {ALPHA: {ORDER_OK, ORDER_LATE}},
            {ALPHA: 4},
            replay.after1["orders"],
            replay.after1["items"],
        )
        == []
    )
    late_items = [r for r in replay.after1["items"] if r["order_id"] == ORDER_LATE]
    assert len(late_items) == 2


def test_replay_marks_the_recovered_row_replayed_and_the_ghost_failed_again(
    replay: Replay,
) -> None:
    dlq = _by_ref(replay.after1["dlq"])
    assert set(dlq) == set(_by_ref(replay.before["dlq"]))  # no row added or removed
    late, ghost = dlq[replay.refs["late"]], dlq[replay.refs["ghost"]]
    assert (late["_replay_status"], late["_replay_count"]) == ("REPLAYED", 1)
    assert (ghost["_replay_status"], ghost["_replay_count"]) == ("FAILED_AGAIN", 1)
    assert ghost["_retryable"] is True


def test_replay_preserves_dlq_identity_and_bronze_reference(replay: Replay) -> None:
    before = _by_ref(replay.before["dlq"])
    after = _by_ref(replay.after1["dlq"])
    for ref, row in before.items():
        assert after[ref]["_dlq_id"] == row["_dlq_id"]
        assert after[ref]["brand_id"] == row["brand_id"]
        assert after[ref]["_rejection_codes"] == row["_rejection_codes"]


def test_the_ghost_order_never_reaches_silver(replay: Replay) -> None:
    for snap in (replay.after1, replay.after2):
        assert ORDER_GHOST not in {r["order_id"] for r in snap["orders"]}
        assert ORDER_GHOST not in {r["order_id"] for r in snap["items"]}


# ---- replay 2 is idempotent ------------------------------------------------------------------


def test_second_replay_changes_no_silver_row_and_adds_no_dlq_row(replay: Replay) -> None:
    for table in ("orders", "items"):
        assert sorted(map(repr, replay.after2[table])) == sorted(map(repr, replay.after1[table]))
    assert len(replay.after2["dlq"]) == len(replay.after1["dlq"])
    assert _keys(replay.after2["dlq"], "_dlq_id") == _keys(replay.after1["dlq"], "_dlq_id")


def test_second_replay_leaves_the_replayed_row_alone_and_retries_only_the_ghost(
    replay: Replay,
) -> None:
    dlq = _by_ref(replay.after2["dlq"])
    first = _by_ref(replay.after1["dlq"])
    late = dlq[replay.refs["late"]]
    assert (late["_replay_status"], late["_replay_count"]) == ("REPLAYED", 1)
    assert late == first[replay.refs["late"]]
    ghost = dlq[replay.refs["ghost"]]
    assert ghost["_replay_status"] == "FAILED_AGAIN"
    assert ghost["_replay_count"] == 2
    assert ghost["_replay_count"] <= MAX_REPLAYS


# ---- negative tests: the checks detect a violation (no stack needed) --------------------------


def test_violation_check_detects_a_replay_that_duplicated_or_skipped_an_order() -> None:
    orders = [
        {"brand_id": ALPHA, "order_id": ORDER_OK},
        {"brand_id": ALPHA, "order_id": ORDER_LATE},
    ]
    items = [
        {"brand_id": ALPHA, "order_id": o, "item_id": f"{o}-{n}"}
        for o in (ORDER_OK, ORDER_LATE)
        for n in (1, 2)
    ]
    want = {ALPHA: {ORDER_OK, ORDER_LATE}}
    assert violations(want, {ALPHA: 4}, orders, items) == []
    assert any("duplicate" in p for p in violations(want, {ALPHA: 4}, [*orders, orders[1]], items))
    assert any("missing=1" in p for p in violations(want, {ALPHA: 4}, orders[:1], items))
    assert any(
        "items 3 != expected 4" in p for p in violations(want, {ALPHA: 4}, orders, items[:3])
    )
