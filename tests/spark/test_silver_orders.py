"""Silver orders and order items (W3.D17, D-078), derived from DATA_CONTRACTS sections 2.2, 2.3,
3.2 and 4.3 and the approved design, not from the job code.

Bronze is a tmp Delta table filled through the production ``to_bronze_rows`` of the batch ingest,
so ``_source_ref`` is a realistic ``landing/...#L<n>``. Customers and products are small frames
shaped like ``silver.customers`` / valid ``bronze.raw_products`` (columns ``brand_id`` and the
business key; a tombstone row still counts as a present customer).

Outcome precedence: parse -> reread -> dlq_excluded -> duplicate -> fk_orphan -> accepted.

Runs inside the Spark image (``docker compose --profile tools run --rm spark-test``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest


pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse.ingestion.batch import ORDERS, PRODUCTS  # noqa: E402
from lakehouse.quality import rules as rules_mod  # noqa: E402
from lakehouse.silver import orders as orders_mod  # noqa: E402
from lakehouse.silver.orders import (  # noqa: E402
    merge_orders,
    process_orders_batch,
    recheck_fk,
    typed_items,
    typed_orders,
)

from .order_data import (  # noqa: E402
    ALPHA,
    BETA,
    C1,
    C2,
    C3,
    FUTURE_TS,
    O1,
    O2,
    O3,
    O4,
    O5,
    P1,
    P2,
    P3,
    bronze_files,
    item,
    order,
    order_path,
    product,
    product_path,
    ref,
)


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame, SparkSession

NOW = datetime(2026, 11, 11, 12, 0, 0, tzinfo=UTC)
RUN = "test_run"
COUNTER_FIELDS = (
    "rows_read",
    "reread_rows",
    "parse_excluded",
    "dlq_excluded",
    "duplicates_removed",
    "fk_orphans",
    "silver_accepted",
    "items_inserted",
)


def _code(constant: Any) -> str:
    return str(getattr(constant, "code", constant))


CUSTOMER_FK = _code(rules_mod.CUSTOMER_FK_MISSING)
PRODUCT_FK = _code(rules_mod.PRODUCT_FK_MISSING)


class Lake:
    """Tmp Bronze (orders), customers/products frames, and Silver/DLQ Delta tables."""

    def __init__(self, spark: SparkSession, root: Path) -> None:
        self.spark = spark
        self.bronze_path = (root / "bronze").as_posix()
        self.orders_path = (root / "orders").as_posix()
        self.items_path = (root / "items").as_posix()
        self.dlq_path = (root / "dlq").as_posix()
        self.batches = 0
        self.customers_rows: list[tuple[str, str]] = []
        self.products_rows: list[tuple[str, str]] = []
        ensure = getattr(orders_mod, "ensure_tables", None)
        if ensure is not None:  # production creates the tables; call it when it is public
            ensure(spark, self.orders_path, self.items_path)

    # -- fixtures ---------------------------------------------------------------------------
    def customers(self) -> DataFrame:
        return self.spark.createDataFrame(
            self.customers_rows, "brand_id string, customer_id string"
        )

    def products(self) -> DataFrame:
        """Valid Bronze products (the same frame the job reads from ``raw_products``)."""
        files: dict[str, list[dict[str, Any] | str]] = {}
        for brand, pid in self.products_rows:
            files.setdefault(product_path(brand), []).append(product(pid, brand=brand))
        if not files:
            return bronze_files(self.spark, PRODUCTS, {product_path(): [product(P3)]}).limit(0)
        return bronze_files(self.spark, PRODUCTS, files, "products")

    def bronze(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.bronze_path)

    def append(self, files: dict[str, list[dict[str, Any] | str]]) -> DataFrame:
        """Append landed files to Bronze; return just these rows as the micro-batch."""
        self.batches += 1
        batch_id = f"batch-{self.batches}"
        bronze_files(self.spark, ORDERS, files, batch_id).write.format("delta").mode("append").save(
            self.bronze_path
        )
        return self.bronze().where(F.col("_batch_id") == batch_id)

    def process(self, batch: DataFrame) -> Any:
        return process_orders_batch(
            self.spark,
            batch,
            self.bronze(),
            self.customers(),
            self.products(),
            self.orders_path,
            self.items_path,
            self.dlq_path,
            RUN,
            NOW,
        )

    def ingest(self, files: dict[str, list[dict[str, Any] | str]]) -> Any:
        return self.process(self.append(files))

    # -- state ------------------------------------------------------------------------------
    def _read(self, path: str) -> list[dict[str, Any]]:
        try:
            return [r.asDict() for r in self.spark.read.format("delta").load(path).collect()]
        except Exception:  # table never created = empty
            return []

    def orders(self) -> dict[tuple[str, str], dict[str, Any]]:
        rows = self._read(self.orders_path)
        out = {(r["brand_id"], r["order_id"]): r for r in rows}
        assert len(out) == len(rows), "Silver orders holds a duplicate (brand_id, order_id)"
        return out

    def items(self) -> dict[tuple[str, str, str], dict[str, Any]]:
        rows = self._read(self.items_path)
        out = {(r["brand_id"], r["order_id"], r["item_id"]): r for r in rows}
        assert len(out) == len(rows), "Silver items holds a duplicate (brand_id, order_id, item_id)"
        return out

    def dlq(self) -> list[dict[str, Any]]:
        return self._read(self.dlq_path)


@pytest.fixture
def lake(spark: SparkSession, tmp_path: Path) -> Lake:
    lk = Lake(spark, tmp_path)
    lk.customers_rows = [(ALPHA, C1), (ALPHA, C2), (BETA, C3)]
    lk.products_rows = [(ALPHA, P1), (ALPHA, P2), (BETA, P1), (BETA, P2)]
    return lk


def sum_holds(c: Any) -> bool:
    """distinct refs == the five disjoint outcome terms (D18 reconciliation)."""
    return c.rows_read - c.reread_rows == (
        c.parse_excluded + c.dlq_excluded + c.duplicates_removed + c.fk_orphans + c.silver_accepted
    )


def _dlq_by_ref(lake: Lake) -> dict[str, dict[str, Any]]:
    rows = lake.dlq()
    out = {r["_bronze_ref"]: r for r in rows}
    assert len(out) == len(rows), "DLQ holds two rows for one Bronze ref"
    return out


# ---- outcomes -------------------------------------------------------------------------------


def test_counter_names_are_the_contract_ones(lake: Lake) -> None:
    counts = lake.ingest({order_path(): [order(O1)]})
    for name in COUNTER_FIELDS:
        assert hasattr(counts, name), name


def test_clean_order_lands_in_silver_with_typed_columns_and_its_items(lake: Lake) -> None:
    counts = lake.ingest({order_path(): [order(O1)]})
    assert (counts.rows_read, counts.silver_accepted, counts.items_inserted) == (1, 1, 2)
    assert sum_holds(counts)
    row = lake.orders()[(ALPHA, O1)]
    assert str(row["gross_amount"]) == "25.50" and str(row["net_amount"]) == "23.00"
    assert str(row["discount_amount"]) == "2.50"
    assert row["order_timestamp"].replace(tzinfo=UTC) == datetime(2026, 3, 1, 10, 0, tzinfo=UTC)
    assert (row["customer_id"], row["order_status"], row["currency"]) == (C1, "completed", "EUR")
    items = lake.items()
    assert len(items) == 2
    for (brand, order_id, _item_id), it in items.items():
        assert (brand, order_id) == (ALPHA, O1)
        assert it["product_id"] in {P1, P2}
        assert str(it["unit_price"]) in {"10.00", "5.50"}
    assert lake.dlq() == []


def test_contract_violation_is_excluded_from_silver_and_not_written_to_the_dlq_by_the_job(
    lake: Lake,
) -> None:
    """The gate owns non-retryable DLQ rows; Silver only counts them as ``dlq_excluded``."""
    counts = lake.ingest(
        {
            order_path(): [
                order(O1),
                order(O2, gross_amount="-25.50", net_amount="-28.00"),
                order(O3, order_timestamp=FUTURE_TS),
            ]
        }
    )
    assert (counts.rows_read, counts.dlq_excluded, counts.silver_accepted) == (3, 2, 1)
    assert sum_holds(counts)
    assert set(lake.orders()) == {(ALPHA, O1)}
    assert lake.dlq() == []


def test_parse_error_row_is_counted_and_excluded(lake: Lake) -> None:
    counts = lake.ingest({order_path(): [order(O1), "{not json"]})
    assert (counts.rows_read, counts.parse_excluded, counts.silver_accepted) == (2, 1, 1)
    assert sum_holds(counts)
    assert len(lake.orders()) == 1


def test_duplicate_in_the_same_batch_keeps_the_first_copy_and_counts_one(lake: Lake) -> None:
    first = order(O1, order_status="pending")
    second = order(O1, order_status="completed")
    counts = lake.ingest({order_path(): [first, second]})
    assert (counts.duplicates_removed, counts.silver_accepted, counts.items_inserted) == (1, 1, 2)
    assert sum_holds(counts)
    assert lake.orders()[(ALPHA, O1)]["order_status"] == "pending"  # FIRST valid copy
    assert len(lake.items()) == 2


def test_duplicate_arriving_in_a_later_batch_is_counted_and_leaves_silver_unchanged(
    lake: Lake,
) -> None:
    lake.ingest({order_path(batch_no=1): [order(O1, order_status="pending")]})
    before = lake.orders()
    counts = lake.ingest({order_path(batch_no=2): [order(O1, order_status="completed")]})
    assert (counts.duplicates_removed, counts.silver_accepted, counts.items_inserted) == (1, 0, 0)
    assert sum_holds(counts)
    assert lake.orders() == before
    assert len(lake.items()) == 2


def test_invalid_earlier_copy_does_not_mask_a_later_valid_copy(lake: Lake) -> None:
    """Only VALID Bronze copies rank for dedup: a rejected first copy is not "the first"."""
    lake.ingest({order_path(batch_no=1): [order(O1, gross_amount="-25.50", net_amount="-28.00")]})
    counts = lake.ingest({order_path(batch_no=2): [order(O1)]})
    assert (counts.duplicates_removed, counts.silver_accepted) == (0, 1)
    assert str(lake.orders()[(ALPHA, O1)]["gross_amount"]) == "25.50"


def test_same_order_id_in_two_brands_is_two_orders_not_a_duplicate(lake: Lake) -> None:
    counts = lake.ingest(
        {
            order_path(ALPHA): [order(O1, brand=ALPHA, customer=C1)],
            order_path(BETA): [order(O1, brand=BETA, customer=C3)],
        }
    )
    assert (counts.duplicates_removed, counts.silver_accepted) == (0, 2)
    assert set(lake.orders()) == {(ALPHA, O1), (BETA, O1)}
    assert len(lake.items()) == 4  # same item ids too: the key includes brand_id


def test_missing_customer_is_a_retryable_orphan_in_the_dlq(lake: Lake) -> None:
    path = order_path()
    counts = lake.ingest({path: [order(O1), order(O2, customer=uid_missing())]})
    assert (counts.fk_orphans, counts.silver_accepted, counts.items_inserted) == (1, 1, 2)
    assert sum_holds(counts)
    assert set(lake.orders()) == {(ALPHA, O1)}
    row = _dlq_by_ref(lake)[ref(path, 2)]
    assert row["brand_id"] == ALPHA
    assert row["_error_category"] == "REFERENTIAL_INTEGRITY_VIOLATION"
    assert row["_retryable"] is True
    assert list(row["_rejection_codes"]) == [CUSTOMER_FK]
    assert (row["_replay_status"], row["_replay_count"]) == ("PENDING", 0)
    assert row["event_id"] is None  # orders carry no event_id
    assert len(row["_dlq_id"]) == 64
    assert ref(path, 1) not in _dlq_by_ref(lake)


def uid_missing() -> str:
    return "00000000-0000-4000-8000-0000000000ff"


def test_customer_of_another_brand_does_not_satisfy_the_foreign_key(lake: Lake) -> None:
    """Tenant isolation negative: C3 exists only in brand_beta, so an alpha order is an orphan."""
    path = order_path(ALPHA)
    counts = lake.ingest({path: [order(O1, brand=ALPHA, customer=C3)]})
    assert (counts.fk_orphans, counts.silver_accepted) == (1, 0)
    assert lake.orders() == {}
    assert list(_dlq_by_ref(lake)[ref(path, 1)]["_rejection_codes"]) == [CUSTOMER_FK]


def test_tombstoned_customer_still_satisfies_the_foreign_key(lake: Lake) -> None:
    """Presence in silver.customers is what counts; a deleted customer keeps its orders."""
    lake.customers_rows.append((ALPHA, uid_missing()))
    counts = lake.ingest({order_path(): [order(O1, customer=uid_missing())]})
    assert (counts.fk_orphans, counts.silver_accepted) == (0, 1)


def test_unknown_product_makes_the_order_an_orphan_with_the_product_code(lake: Lake) -> None:
    path = order_path()
    bad = order(O1)
    bad["items"] = [item(1, O1, product=P3, quantity=2, unit_price="10.00", line_total="20.00")]
    bad.update(gross_amount="20.00", discount_amount="0.00", net_amount="20.00")
    counts = lake.ingest({path: [bad]})
    assert (counts.fk_orphans, counts.silver_accepted, counts.items_inserted) == (1, 0, 0)
    assert lake.orders() == {} and lake.items() == {}
    row = _dlq_by_ref(lake)[ref(path, 1)]
    assert PRODUCT_FK in list(row["_rejection_codes"])
    assert row["_retryable"] is True


def test_product_of_another_brand_does_not_satisfy_the_item_foreign_key(lake: Lake) -> None:
    lake.products_rows = [(BETA, P1), (BETA, P2), (ALPHA, P1)]  # alpha has no PRD-00002
    counts = lake.ingest({order_path(): [order(O1)]})  # items use P1 and P2
    assert (counts.fk_orphans, counts.silver_accepted) == (1, 0)


def test_one_orphan_item_orphans_the_whole_order_atomically(lake: Lake) -> None:
    lake.products_rows = [(ALPHA, P1)]  # P2 missing
    lake.ingest({order_path(): [order(O1)]})
    assert lake.orders() == {} and lake.items() == {}  # no partial order, no partial items


def test_order_and_customer_orphan_counts_are_per_row(lake: Lake) -> None:
    path = order_path()
    counts = lake.ingest(
        {
            path: [
                order(O1),
                order(O2, customer=uid_missing()),
                order(O3, customer=C2),
                order(O4, customer=uid_missing()),
            ]
        }
    )
    assert (counts.rows_read, counts.fk_orphans, counts.silver_accepted) == (4, 2, 2)
    assert sum_holds(counts)
    assert set(_dlq_by_ref(lake)) == {ref(path, 2), ref(path, 4)}


# ---- idempotency and re-runs ---------------------------------------------------------------


def test_reprocessing_the_same_batch_changes_nothing(lake: Lake) -> None:
    path = order_path()
    batch = lake.append({path: [order(O1), order(O2, customer=uid_missing())]})
    lake.process(batch)
    orders, items, dlq = lake.orders(), lake.items(), lake.dlq()
    again = lake.process(batch)  # a restarted micro-batch replays the same rows
    assert lake.orders() == orders
    assert lake.items() == items
    assert sorted(r["_dlq_id"] for r in lake.dlq()) == sorted(r["_dlq_id"] for r in dlq)
    assert len(lake.dlq()) == len(dlq) == 1
    assert sum_holds(again)


def test_rerun_over_a_whole_bronze_table_never_duplicates_keys(lake: Lake) -> None:
    lake.append({order_path(batch_no=1): [order(O1)]})
    batch2 = lake.append({order_path(batch_no=2): [order(O2), order(O1)]})
    lake.process(batch2)
    full = lake.bronze()
    lake.process(full)
    assert len(lake.orders()) == 2 and len(lake.items()) == 4  # .orders() asserts key uniqueness


def test_reread_of_a_known_source_ref_is_counted_apart_from_the_equation(lake: Lake) -> None:
    path = order_path()
    first = lake.append({path: [order(O1)]})
    lake.process(first)
    # D-057: the same landing line appears again in Bronze (checkpoint-lineage rewind).
    again = lake.append({path: [order(O1)]})
    counts = lake.process(again)
    assert counts.reread_rows >= 1
    assert sum_holds(counts)
    assert len(lake.orders()) == 1 and len(lake.items()) == 2


# ---- typed projections ---------------------------------------------------------------------


def test_typed_orders_and_items_project_contract_types(lake: Lake) -> None:
    batch = lake.append({order_path(): [order(O1), order(O2, customer=C2)]})
    orders = typed_orders(batch)
    items = typed_items(batch)
    types = {f.name: f.dataType.simpleString() for f in orders.schema.fields}
    assert types["gross_amount"] == types["net_amount"] == types["discount_amount"]
    assert types["gross_amount"] == "decimal(10,2)"
    assert types["order_timestamp"] == "timestamp"
    item_types = {f.name: f.dataType.simpleString() for f in items.schema.fields}
    assert item_types["unit_price"] == item_types["line_total"] == "decimal(10,2)"
    assert {"brand_id", "order_id", "item_id"} <= set(items.columns)
    assert orders.count() == 2 and items.count() == 4
    assert "items" not in orders.columns  # exploded into order_items


# ---- recheck_fk / merge_orders (the replay path) -------------------------------------------


def test_recheck_fk_splits_recovered_from_still_orphan_per_tenant(lake: Lake) -> None:
    late = order(O1, customer=uid_missing())  # customer arrives later -> recovered
    ghost = order(O2, customer="00000000-0000-4000-8000-0000000000ee")  # never arrives
    cross = order(O3, brand=ALPHA, customer=C3)  # exists only in brand_beta
    bad_product = order(O4)
    bad_product["items"] = [item(1, O4, product=P3)]
    rows = lake.append({order_path(): [late, ghost, cross, bad_product]})
    lake.customers_rows.append((ALPHA, uid_missing()))
    recovered, still = recheck_fk(rows, lake.customers(), lake.products())
    got_ok = {r["order_id"] for r in recovered.collect()}
    got_bad = {r["order_id"] for r in still.collect()}
    assert got_ok == {O1}
    assert got_bad == {O2, O3, O4}


def test_merge_orders_is_insert_only_and_reports_inserted_counts(lake: Lake) -> None:
    rows = lake.append({order_path(): [order(O1), order(O2, customer=C2)]})
    recovered, _ = recheck_fk(rows, lake.customers(), lake.products())
    assert merge_orders(lake.spark, lake.orders_path, lake.items_path, recovered) == (2, 4)
    snapshot = (lake.orders(), lake.items())
    assert merge_orders(lake.spark, lake.orders_path, lake.items_path, recovered) == (0, 0)
    assert (lake.orders(), lake.items()) == snapshot


def test_replay_of_a_late_parent_order_recovers_it_exactly_once(lake: Lake) -> None:
    path = order_path()
    lake.customers_rows = [(ALPHA, C1)]
    lake.ingest({path: [order(O5, customer=C2)]})  # C2 not there yet -> DLQ PENDING
    assert lake.orders() == {} and len(lake.dlq()) == 1
    lake.customers_rows.append((ALPHA, C2))  # the customer's I is now in Silver
    rows = lake.bronze().where(F.col("_source_ref") == ref(path, 1))
    recovered, still = recheck_fk(rows, lake.customers(), lake.products())
    assert still.count() == 0
    assert merge_orders(lake.spark, lake.orders_path, lake.items_path, recovered) == (1, 2)
    assert merge_orders(lake.spark, lake.orders_path, lake.items_path, recovered) == (0, 0)
    assert set(lake.orders()) == {(ALPHA, O5)} and len(lake.items()) == 2


def test_crash_between_the_orders_and_the_items_merge_is_repaired_by_the_rerun(
    lake: Lake, monkeypatch: pytest.MonkeyPatch
) -> None:
    batch = lake.append({order_path(): [order(O1)]})
    real = orders_mod._insert_only

    def crash_on_items(spark: Any, path: str, source: Any, key: list[str]) -> int:
        if key == orders_mod.ITEM_KEY:
            raise RuntimeError("simulated crash before the items MERGE")
        return real(spark, path, source, key)

    with monkeypatch.context() as patch:
        patch.setattr(orders_mod, "_insert_only", crash_on_items)
        with pytest.raises(RuntimeError, match="simulated crash"):
            lake.process(batch)
    assert set(lake.orders()) == {(ALPHA, O1)} and lake.items() == {}  # orders only

    counts = lake.process(batch)
    assert (counts.silver_accepted, counts.duplicates_removed, counts.already_present) == (0, 1, 1)
    assert counts.items_inserted == 2
    assert sum_holds(counts)
    assert len(lake.orders()) == 1 and len(lake.items()) == 2
