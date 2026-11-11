"""Silver ``orders`` and ``order_items``: insert-only MERGE of Bronze ``raw_orders`` (DATA_CONTRACTS §4.3).

One micro-batch of Bronze order lines is classified, the surviving orders are merged insert-only on
``(brand_id, order_id)`` and their exploded items on ``(brand_id, order_id, item_id)``. A key that is
already in the target is never rewritten: the first valid copy wins (D-078), so replaying a batch
leaves both tables unchanged (an idempotent outcome, not exactly-once).

Gate order per Bronze row (D-022, D-029, D-078), reported in ``_outcome``:

1. ``reread``         the row repeats a ``_source_ref`` (a checkpoint-loss re-read of one landing line):
                      another row of the same batch has it, or a *valid* earlier Bronze copy exists with
                      a smaller ``_ingest_ts``. Re-reads are counted (``reread_rows``), not duplicates.
2. ``parse_excluded`` ``_parse_error`` is set (stays in Bronze only).
3. ``dlq_excluded``   the row fails an ``order_event`` rule (``apply_rules``, inline: no dependency on
                      ``dlq_gate`` having run). The ``dlq_gate`` job owns those DLQ rows.
4. ``duplicate``      an earlier *valid* Bronze copy of ``(brand_id, order_id)`` exists, ranked by
                      ``(_ingest_ts, landing file key, line number)``. A DLQ row never makes a later
                      valid copy a duplicate.
5. ``fk_orphan``      ``(brand_id, customer_id)`` is not in ``silver.customers`` (a tombstone counts as
                      present) or one item's ``(brand_id, product_id)`` is not a valid Bronze product.
                      A late parent is indistinguishable from an orphan at run time: both go to
                      ``dead_letter.dlq_orders`` with ``_retryable = true`` and are recovered by the
                      ``replay`` job (:func:`recheck_fk`).
6. ``accepted``       merge candidate.

A candidate whose key is already in the target (a Silver-checkpoint replay, or an order the replay job
inserted) is counted in ``duplicates_removed`` (and in ``already_present``), so ``silver_accepted``
equals the rows the MERGE really inserted. ``items_inserted`` is the items MERGE's own metric,
predicted by an anti-join before the MERGE and cross-checked afterwards.

Item-level failures live on the order line's DLQ row (there is no ``dlq_order_items`` table).
PySpark and Delta are imported lazily so this module imports on hosts without them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakehouse.quality.dlq import dlq_rows, merge_dlq
from lakehouse.quality.rules import (
    CUSTOMER_FK_MISSING,
    FK_ERROR_CATEGORY,
    PRODUCT_FK_MISSING,
    _canonical_ts,
    apply_rules,
    order_rules,
    product_rules,
)
from lakehouse.silver.customers import MultipleMatchError, ReconciliationError, is_valid


if TYPE_CHECKING:
    from datetime import datetime

    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.types import DataType, StructField, StructType


OUTCOMES = ("parse_excluded", "dlq_excluded", "reread", "duplicate", "fk_orphan", "accepted")
KEY = ["brand_id", "order_id"]
ITEM_KEY = [*KEY, "item_id"]
SOURCE_TABLE = "bronze.raw_orders"
_CONSTRAINT = "brand_id_not_null"


@dataclass(frozen=True)
class OrderBatchCounts:
    """Outcome counts of one micro-batch, in Bronze rows (``reread_rows`` are extra copies).

    ``rows_read - reread_rows == parse_excluded + dlq_excluded + duplicates_removed + fk_orphans +
    silver_accepted``. ``already_present`` is informational: it is part of ``duplicates_removed``.
    ``late_beyond_window`` is constant 0 (orders have no event-time window).
    """

    rows_read: int
    reread_rows: int
    parse_excluded: int
    dlq_excluded: int
    duplicates_removed: int
    fk_orphans: int
    silver_accepted: int
    items_inserted: int
    late_beyond_window: int = 0
    already_present: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "rows_read": self.rows_read,
            "reread_rows": self.reread_rows,
            "parse_excluded": self.parse_excluded,
            "dlq_excluded": self.dlq_excluded,
            "duplicates_removed": self.duplicates_removed,
            "fk_orphans": self.fk_orphans,
            "late_beyond_window": self.late_beyond_window,
            "silver_accepted": self.silver_accepted,
            "items_inserted": self.items_inserted,
        }


def _field(name: str, data_type: DataType, nullable: bool = True) -> StructField:
    from pyspark.sql.types import StructField

    return StructField(name, data_type, nullable)


def silver_orders_schema() -> StructType:
    """Schema of ``silver.orders``; ``brand_id`` and ``order_id`` are NOT NULL."""
    from pyspark.sql.types import DecimalType, StringType, StructType, TimestampType

    money = DecimalType(10, 2)
    return StructType(
        [
            _field("brand_id", StringType(), False),
            _field("order_id", StringType(), False),
            _field("customer_id", StringType()),
            _field("order_timestamp", TimestampType()),
            _field("order_status", StringType()),
            _field("payment_method", StringType()),
            _field("currency", StringType()),
            _field("gross_amount", money),
            _field("discount_amount", money),
            _field("net_amount", money),
            _field("sales_channel", StringType()),
            _field("_bronze_ref", StringType()),
            _field("_bronze_batch_id", StringType()),
            _field("_updated_at", TimestampType()),
        ]
    )


def silver_order_items_schema() -> StructType:
    """Schema of ``silver.order_items``; the three key columns are NOT NULL."""
    from pyspark.sql.types import DecimalType, IntegerType, StringType, StructType, TimestampType

    money = DecimalType(10, 2)
    return StructType(
        [
            _field("brand_id", StringType(), False),
            _field("order_id", StringType(), False),
            _field("item_id", StringType(), False),
            _field("product_id", StringType()),
            _field("quantity", IntegerType()),
            _field("unit_price", money),
            _field("line_total", money),
            _field("_bronze_ref", StringType()),
            _field("_bronze_batch_id", StringType()),
            _field("_updated_at", TimestampType()),
        ]
    )


def _ensure(spark: SparkSession, path: str, schema: StructType) -> None:
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        return
    if "`" in path:
        raise ValueError("path must not contain a backtick")
    spark.createDataFrame([], schema).write.format("delta").save(path)
    spark.sql(
        f"ALTER TABLE delta.`{path}` ADD CONSTRAINT {_CONSTRAINT} CHECK (brand_id IS NOT NULL)"
    )


def ensure_tables(spark: SparkSession, orders_path: str, items_path: str) -> None:
    """Create the empty Silver tables (NOT NULL keys, CHECK ``brand_id IS NOT NULL``) if missing."""
    _ensure(spark, orders_path, silver_orders_schema())
    _ensure(spark, items_path, silver_order_items_schema())


def _valid_product_keys(products: DataFrame) -> DataFrame:
    """Distinct ``(brand_id, product_id)`` of the valid Bronze product rows (path brand)."""
    return (
        apply_rules(products, product_rules())
        .where(is_valid())
        .select("brand_id", "product_id")
        .distinct()
    )


def _with_fk_codes(rows: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    """``rows`` plus ``_fk_codes`` (ARRAY<STRING>): the foreign keys that do not resolve."""
    from pyspark.sql import functions as F

    present = (
        customers.select("brand_id", "customer_id")
        .distinct()
        .withColumn("_customer_present", F.lit(True))
    )
    missing_products = (
        rows.select(
            "brand_id", "_source_ref", F.explode("items").alias("_item")
        )  # NULL or empty arrays yield no row
        .select("brand_id", "_source_ref", F.col("_item.product_id").alias("product_id"))
        .where(F.col("product_id").isNotNull())
        .join(_valid_product_keys(products), ["brand_id", "product_id"], "left_anti")
        .select("_source_ref")
        .distinct()
        .withColumn("_product_missing", F.lit(True))
    )
    joined = rows.join(present, ["brand_id", "customer_id"], "left").join(
        missing_products, "_source_ref", "left"
    )
    codes = F.filter(
        F.array(
            F.when(
                ~F.coalesce(F.col("_customer_present"), F.lit(False)), F.lit(CUSTOMER_FK_MISSING)
            ),
            F.when(F.coalesce(F.col("_product_missing"), F.lit(False)), F.lit(PRODUCT_FK_MISSING)),
        ),
        lambda code: code.isNotNull(),
    )
    return joined.withColumn("_fk_codes", codes).drop("_customer_present", "_product_missing")


def recheck_fk(
    bronze_rows: DataFrame, customers: DataFrame, products: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Re-resolve the foreign keys of already validated Bronze order rows (DLQ replay).

    Returns ``(recovered, still_orphan)``: ``recovered`` has an empty ``_fk_codes`` and can go to
    :func:`merge_orders`; ``still_orphan`` keeps the codes that still fail. ``customers`` is
    ``silver.customers``; ``products`` is the Bronze ``raw_products`` table (validated inline).
    """
    from pyspark.sql import functions as F

    checked = _with_fk_codes(bronze_rows, customers, products)
    return checked.where(F.size("_fk_codes") == 0), checked.where(F.size("_fk_codes") > 0)


def _landing_position() -> list[Column]:
    """``(file key, line number)`` parsed from ``<file key>#L<n>``; the tail of the dedup order."""
    from pyspark.sql import functions as F

    ref = F.col("_source_ref")
    return [
        F.substring_index(ref, "#L", 1),
        F.regexp_extract(ref, r"#L([0-9]+)$", 1).cast("long"),
        ref,
    ]


def classify_orders(
    batch: DataFrame, bronze: DataFrame, customers: DataFrame, products: DataFrame
) -> DataFrame:
    """``batch`` (Bronze order rows of one micro-batch) plus the rule columns, ``_fk_codes`` and
    ``_outcome``.

    ``bronze`` is ``raw_orders``, read for earlier copies of the batch's keys; rows of ``batch``
    itself need not be in it. ``customers`` is ``silver.customers``, ``products`` the Bronze
    ``raw_products`` table. ``accepted`` means "merge candidate": whether the key is already in the
    target is decided in :func:`process_orders_batch`.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    rules = order_rules()
    one_per_ref = Window.partitionBy("_source_ref").orderBy(
        F.col("_ingest_ts").asc_nulls_last(), F.col("_batch_id").asc_nulls_last()
    )
    checked = apply_rules(batch.withColumn("_ref_rank", F.row_number().over(one_per_ref)), rules)
    valid = is_valid()
    first_copy = valid & (F.col("_ref_rank") == 1)

    current = checked.where(first_copy)
    keys = current.select(*KEY).distinct()
    history = apply_rules(bronze.join(F.broadcast(keys), KEY, "left_semi"), rules)
    # One row per valid Bronze/current ``_source_ref`` with its earliest ingest time.
    refs = (
        history.where(is_valid())
        .select(*KEY, "_source_ref", "_ingest_ts")
        .unionByName(current.select(*KEY, "_source_ref", "_ingest_ts"))
        .groupBy("_source_ref", *KEY)
        .agg(F.min("_ingest_ts").alias("_first_ingest_ts"))
    )
    occurrence = Window.partitionBy(*KEY).orderBy(F.col("_first_ingest_ts"), *_landing_position())
    ranked = refs.select(
        "_source_ref", "_first_ingest_ts", F.row_number().over(occurrence).alias("_dup_rank")
    )

    joined = (
        checked.join(ranked, "_source_ref", "left")
        .withColumn("_dup_rank", F.coalesce(F.col("_dup_rank"), F.lit(1)))
        .withColumn(
            "_later_reread",
            F.coalesce(valid & (F.col("_ingest_ts") > F.col("_first_ingest_ts")), F.lit(False)),
        )
    )
    with_fk = _with_fk_codes(joined, customers, products)
    no_codes = F.array().cast("array<string>")
    with_fk = with_fk.withColumn("_fk_codes", F.when(valid, F.col("_fk_codes")).otherwise(no_codes))
    outcome = (
        F.when((F.col("_ref_rank") > 1) | F.col("_later_reread"), "reread")
        .when(F.col("_parse_error").isNotNull(), "parse_excluded")
        .when(F.size("_rejection_codes") > 0, "dlq_excluded")
        .when(F.col("_dup_rank") > 1, "duplicate")
        .when(F.size("_fk_codes") > 0, "fk_orphan")
        .otherwise("accepted")
    )
    return with_fk.withColumn("_outcome", outcome).drop(
        "_ref_rank", "_dup_rank", "_first_ingest_ts", "_later_reread"
    )


def typed_orders(rows: DataFrame) -> DataFrame:
    """Typed ``silver.orders`` columns (without ``_updated_at``) of validated Bronze ``rows``.

    Casts happen here, after the rules (D-045); ``brand_id`` is the landing-path brand.
    """
    from pyspark.sql import functions as F

    return rows.select(
        F.col("brand_id"),
        F.col("order_id"),
        F.col("customer_id"),
        F.to_timestamp(_canonical_ts(F.col("order_timestamp"))).alias("order_timestamp"),
        F.col("order_status"),
        F.col("payment_method"),
        F.col("currency"),
        F.col("gross_amount").cast("decimal(10,2)").alias("gross_amount"),
        F.col("discount_amount").cast("decimal(10,2)").alias("discount_amount"),
        F.col("net_amount").cast("decimal(10,2)").alias("net_amount"),
        F.col("sales_channel"),
        F.col("_source_ref").alias("_bronze_ref"),
        F.col("_batch_id").alias("_bronze_batch_id"),
    )


def typed_items(rows: DataFrame) -> DataFrame:
    """Typed ``silver.order_items`` columns (without ``_updated_at``): one row per embedded item."""
    from pyspark.sql import functions as F

    return rows.select(
        "brand_id", "order_id", "_source_ref", "_batch_id", F.explode("items").alias("_item")
    ).select(
        F.col("brand_id"),
        F.col("order_id"),
        F.col("_item.item_id").alias("item_id"),
        F.col("_item.product_id").alias("product_id"),
        F.col("_item.quantity").cast("int").alias("quantity"),
        F.col("_item.unit_price").cast("decimal(10,2)").alias("unit_price"),
        F.col("_item.line_total").cast("decimal(10,2)").alias("line_total"),
        F.col("_source_ref").alias("_bronze_ref"),
        F.col("_batch_id").alias("_bronze_batch_id"),
    )


def _insert_only(spark: SparkSession, path: str, source: DataFrame, key: list[str]) -> int:
    """Insert-only MERGE on ``key``; rows inserted according to the MERGE's own metrics.

    A source with two rows for one key would insert both (no match in the target), so it is refused.
    The count is read from the history row of version ``before + 1`` (0 when nothing committed).
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    stats = source.agg(
        F.count(F.lit(1)).alias("rows"), F.countDistinct(F.struct(*key)).alias("distinct")
    ).first()
    rows = int(stats["rows"]) if stats is not None else 0
    if rows == 0:
        return 0
    distinct = int(stats["distinct"]) if stats is not None else 0
    if rows != distinct:
        raise MultipleMatchError(
            f"MERGE source has {rows} rows for {distinct} distinct ({', '.join(key)}) keys"
        )
    target = DeltaTable.forPath(spark, path)
    version_before = int(target.history(1).collect()[0]["version"])
    condition = " AND ".join(f"t.{name} = s.{name}" for name in key)
    target.alias("t").merge(source.alias("s"), condition).whenNotMatchedInsertAll().execute()
    history = (
        target.history(2)
        .where(F.col("version") == version_before + 1)
        .select("operation", "operationMetrics")
        .collect()
    )
    if not history or history[0]["operation"] != "MERGE":
        return 0
    return int((history[0]["operationMetrics"] or {}).get("numTargetRowsInserted", 0))


def merge_orders(
    spark: SparkSession,
    orders_path: str,
    items_path: str,
    rows: DataFrame,
    now: datetime | None = None,
) -> tuple[int, int]:
    """Insert validated Bronze order ``rows`` into both Silver tables; ``(orders, items)`` inserted.

    Orders first, then items. Both MERGEs are keyed insert-only, so the items MERGE also repairs a
    run that crashed between the two. ``rows`` keeps the Bronze columns; at most one row per
    ``(brand_id, order_id)`` survives (the earliest by ``_ingest_ts``, ``_source_ref``). Existing
    keys are never touched. ``now`` stamps ``_updated_at`` (default: ``current_timestamp``).
    """
    from pyspark import StorageLevel
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ensure_tables(spark, orders_path, items_path)
    stamp = F.lit(now).cast("timestamp") if now is not None else F.current_timestamp()
    first = Window.partitionBy(*KEY).orderBy(
        F.col("_ingest_ts").asc_nulls_last(), F.col("_source_ref").asc()
    )
    unique = (
        rows.withColumn("_key_rank", F.row_number().over(first))
        .where(F.col("_key_rank") == 1)
        .drop("_key_rank")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    try:
        orders = typed_orders(unique).withColumn("_updated_at", stamp)
        items = typed_items(unique).withColumn("_updated_at", stamp)
        orders_inserted = _insert_only(spark, orders_path, orders, KEY)
        items_inserted = _insert_only(spark, items_path, items, ITEM_KEY)
    finally:
        unique.unpersist()
    return orders_inserted, items_inserted


def _write_orphans(
    spark: SparkSession, classified: DataFrame, dlq_path: str, run_id: str
) -> tuple[int, int]:
    """Insert-only MERGE of the ``fk_orphan`` rows into the DLQ; ``(orphans, dlq_inserted)``."""
    from pyspark.sql import functions as F

    orphans = (
        classified.where(F.col("_outcome") == "fk_orphan")
        .withColumn("_rejection_codes", F.col("_fk_codes"))
        .withColumn("_error_category", F.lit(FK_ERROR_CATEGORY.value))
        .withColumn("_rejection_reason", F.array_join(F.col("_fk_codes"), ", "))
        .withColumn("_retryable", F.lit(True))
    )
    rows = dlq_rows(orphans, SOURCE_TABLE, run_id).dropDuplicates(["_dlq_id"])
    return rows.count(), merge_dlq(spark, dlq_path, rows)


def process_orders_batch(
    spark: SparkSession,
    batch: DataFrame,
    bronze: DataFrame,
    customers: DataFrame,
    products: DataFrame,
    orders_path: str,
    items_path: str,
    dlq_path: str,
    run_id: str,
    now: datetime,
) -> OrderBatchCounts:
    """Classify, merge, write orphans and reconcile one micro-batch (:class:`ReconciliationError`)."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    ensure_tables(spark, orders_path, items_path)
    classified = classify_orders(batch, bronze, customers, products).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        # One pass: rows per outcome and the distinct _source_refs of the non-reread rows.
        stats = classified.agg(
            F.countDistinct(F.when(F.col("_outcome") != "reread", F.col("_source_ref"))).alias(
                "_distinct_refs"
            ),
            *[
                F.count(F.when(F.col("_outcome") == name, F.lit(1))).alias(name)
                for name in OUTCOMES
            ],
        ).first()
        by_outcome = {name: int(stats[name]) for name in OUTCOMES} if stats else {}
        distinct_refs = int(stats["_distinct_refs"]) if stats else 0

        accepted = classified.where(F.col("_outcome") == "accepted")
        orders_target = spark.read.format("delta").load(orders_path)
        items_target = spark.read.format("delta").load(items_path)
        already_present = accepted.join(orders_target.select(*KEY), KEY, "left_semi").count()
        expected_items = (
            typed_items(accepted)
            .join(items_target.select(*ITEM_KEY), ITEM_KEY, "left_anti")
            .count()
        )
        inserted, items_inserted = merge_orders(spark, orders_path, items_path, accepted, now)
        orphans, dlq_inserted = _write_orphans(spark, classified, dlq_path, run_id)
    finally:
        classified.unpersist()

    candidates = by_outcome.get("accepted", 0)
    counts = OrderBatchCounts(
        rows_read=sum(by_outcome.values()),
        reread_rows=by_outcome.get("reread", 0),
        parse_excluded=by_outcome.get("parse_excluded", 0),
        dlq_excluded=by_outcome.get("dlq_excluded", 0),
        duplicates_removed=by_outcome.get("duplicate", 0) + already_present,
        fk_orphans=by_outcome.get("fk_orphan", 0),
        silver_accepted=inserted,
        items_inserted=items_inserted,
        already_present=already_present,
    )
    problems: list[str] = []
    if inserted != candidates - already_present:
        problems.append(
            f"MERGE inserted {inserted} orders but {candidates} candidates minus "
            f"{already_present} already present is {candidates - already_present}"
        )
    if items_inserted != expected_items:
        problems.append(f"items MERGE inserted {items_inserted}, expected {expected_items}")
    if orphans != counts.fk_orphans or dlq_inserted > orphans:
        problems.append(
            f"fk_orphans={counts.fk_orphans} but {orphans} DLQ rows built, {dlq_inserted} inserted"
        )
    accounted = (
        counts.parse_excluded
        + counts.dlq_excluded
        + counts.duplicates_removed
        + counts.fk_orphans
        + counts.silver_accepted
    )
    if accounted != distinct_refs or counts.rows_read - counts.reread_rows != distinct_refs:
        problems.append(
            f"distinct _source_ref={distinct_refs} but outcomes add up to {accounted} "
            f"(rows_read={counts.rows_read}, reread_rows={counts.reread_rows})"
        )
    if problems:
        raise ReconciliationError("silver.orders batch: " + "; ".join(problems))
    return counts
