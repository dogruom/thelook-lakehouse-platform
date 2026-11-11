"""DLQ replay of retryable order orphans (W3.D17), derived from the replay design in
``lakehouse.quality.jobs.replay``'s contract: Silver is written before the DLQ status, a DLQ row is
selected while ``PENDING``/``FAILED_AGAIN`` and ``_replay_count < max``, and a rerun is an
idempotent outcome.

Uses the Silver orders test lake (tmp Bronze, customers/products frames, Silver and DLQ Delta
tables). Runs inside the Spark image (``docker compose --profile tools run --rm spark-test``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest


pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse.quality.jobs import replay as replay_mod  # noqa: E402
from lakehouse.quality.jobs.replay import replay_orders  # noqa: E402

from .order_data import ALPHA, BETA, C1, C2, C3, O1, O2, P1, P2, order, order_path  # noqa: E402
from .test_silver_orders import Lake, uid_missing  # noqa: E402


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame, SparkSession

LATE = uid_missing()


@pytest.fixture
def lake(spark: SparkSession, tmp_path: Path) -> Lake:
    lk = Lake(spark, tmp_path)
    lk.customers_rows = [(ALPHA, C1), (ALPHA, C2), (BETA, C3)]
    lk.products_rows = [(ALPHA, P1), (ALPHA, P2), (BETA, P1), (BETA, P2)]
    return lk


def replay(
    lake: Lake,
    max_replays: int = 3,
    brand: str | None = None,
    bronze: DataFrame | None = None,
) -> dict[str, int]:
    return replay_orders(
        lake.spark,
        bronze=lake.bronze() if bronze is None else bronze,
        products=lake.products(),
        customers=lake.customers(),
        dlq=lake.spark.read.format("delta").load(lake.dlq_path),
        dlq_path=lake.dlq_path,
        orders_path=lake.orders_path,
        items_path=lake.items_path,
        max_replays=max_replays,
        brand=brand,
    )


def dlq_state(lake: Lake) -> list[tuple[str, int]]:
    return sorted((r["_replay_status"], r["_replay_count"]) for r in lake.dlq())


def orphan_lake(lake: Lake) -> None:
    """One valid order and one whose customer ``LATE`` is not in Silver yet (DLQ PENDING)."""
    lake.ingest({order_path(): [order(O1), order(O2, customer=LATE)]})
    assert set(lake.orders()) == {(ALPHA, O1)}
    assert dlq_state(lake) == [("PENDING", 0)]


def test_crash_after_silver_before_the_dlq_update_is_repaired_by_the_rerun(
    lake: Lake, monkeypatch: pytest.MonkeyPatch
) -> None:
    orphan_lake(lake)
    lake.customers_rows.append((ALPHA, LATE))

    def crash(*_args: Any, **_kwargs: Any) -> int:
        raise RuntimeError("simulated crash before the DLQ update")

    with monkeypatch.context() as patch:
        patch.setattr(replay_mod, "set_replay_outcome", crash)
        with pytest.raises(RuntimeError, match="simulated crash"):
            replay(lake)
    # Silver was written, the DLQ row was not touched.
    assert set(lake.orders()) == {(ALPHA, O1), (ALPHA, O2)} and len(lake.items()) == 4
    assert dlq_state(lake) == [("PENDING", 0)]
    orders, items = lake.orders(), lake.items()

    counts = replay(lake)
    assert (counts["candidates"], counts["recovered"], counts["replayed"]) == (1, 1, 1)
    assert (counts["orders_inserted"], counts["items_inserted"]) == (0, 0)
    assert lake.orders() == orders and lake.items() == items  # Silver unchanged
    assert dlq_state(lake) == [("REPLAYED", 1)]
    assert replay(lake)["candidates"] == 0  # REPLAYED is never selected again


def test_row_at_the_max_replay_count_is_not_selected(lake: Lake) -> None:
    orphan_lake(lake)
    first = replay(lake, max_replays=1)  # parent still missing
    assert (first["candidates"], first["failed_again"]) == (1, 1)
    assert dlq_state(lake) == [("FAILED_AGAIN", 1)]

    lake.customers_rows.append((ALPHA, LATE))  # the parent arrives, but the row is exhausted
    at_max = replay(lake, max_replays=1)
    assert at_max["candidates"] == 0
    assert dlq_state(lake) == [("FAILED_AGAIN", 1)]
    assert set(lake.orders()) == {(ALPHA, O1)}

    raised = replay(lake, max_replays=2)  # one more attempt allowed
    assert (raised["candidates"], raised["replayed"]) == (1, 1)
    assert dlq_state(lake) == [("REPLAYED", 2)]
    assert (ALPHA, O2) in lake.orders()


def test_failed_again_recovers_when_the_parent_arrives(lake: Lake) -> None:
    orphan_lake(lake)
    first = replay(lake)
    assert (first["candidates"], first["recovered"], first["still_orphan"]) == (1, 0, 1)
    assert (first["failed_again"], first["orders_inserted"]) == (1, 0)
    assert dlq_state(lake) == [("FAILED_AGAIN", 1)]

    lake.customers_rows.append((ALPHA, LATE))
    second = replay(lake)
    assert (second["candidates"], second["recovered"], second["replayed"]) == (1, 1, 1)
    assert (second["orders_inserted"], second["items_inserted"]) == (1, 2)
    assert set(lake.orders()) == {(ALPHA, O1), (ALPHA, O2)}
    assert dlq_state(lake) == [("REPLAYED", 2)]
    third = replay(lake)
    assert third["candidates"] == 0
    assert len(lake.orders()) == 2 and len(lake.items()) == 4


def test_dlq_row_without_a_bronze_row_becomes_failed_again(lake: Lake) -> None:
    """Nothing to re-check (e.g. wrong namespace or an erased Bronze row): the row is not
    recovered, so it is ``FAILED_AGAIN`` and its count grows until ``max_replays`` leaves it alone.
    That is the right outcome: it never reaches ``REPLAYED`` without Silver data, and the row stays
    visible to ``dlq_report`` instead of being dropped or retried forever."""
    orphan_lake(lake)
    lake.customers_rows.append((ALPHA, LATE))
    counts = replay(lake, bronze=lake.bronze().where(F.lit(False)))
    assert (counts["candidates"], counts["bronze_found"], counts["recovered"]) == (1, 0, 0)
    assert (counts["failed_again"], counts["replayed"]) == (1, 0)
    assert dlq_state(lake) == [("FAILED_AGAIN", 1)]
    assert set(lake.orders()) == {(ALPHA, O1)}


def test_brand_filter_replays_only_that_brand(lake: Lake) -> None:
    orphan_lake(lake)
    lake.customers_rows.append((ALPHA, LATE))
    other = replay(lake, brand=BETA)
    assert other["candidates"] == 0 and dlq_state(lake) == [("PENDING", 0)]
    assert replay(lake, brand=ALPHA)["replayed"] == 1
