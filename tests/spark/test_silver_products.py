"""Silver products (W4.D20.0, D-091), derived from the task spec and DATA_CONTRACTS, not the job code.

Outcome precedence: parse_excluded -> dlq_excluded -> superseded -> accepted.
Runs inside the Spark image (``docker compose --profile tools run --rm spark-test``).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest


pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse.common.tables import ResolvedTable  # noqa: E402
from lakehouse.silver.customers import ReconciliationError  # noqa: E402
from lakehouse.silver.products import (  # noqa: E402
    OUTCOMES,
    ProductCounts,
    check_equation,
    classify_products,
    latest_products,
    run_products,
)


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame, SparkSession

ALPHA = "brand_alpha"
BETA = "brand_beta"
T0 = datetime(2026, 11, 1, 10, 0, 0)
T1 = datetime(2026, 11, 2, 10, 0, 0)
T2 = datetime(2026, 11, 3, 10, 0, 0)

BRONZE_SCHEMA = StructType(
    [
        StructField("schema_version", LongType()),
        StructField("product_id", StringType()),
        StructField("payload_brand_id", StringType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("retail_price", StringType()),
        StructField("cost_price", StringType()),
        StructField("is_active", BooleanType()),
        StructField("brand_id", StringType()),
        StructField("_raw_value", StringType()),
        StructField("_source_ref", StringType()),
        StructField("_ingest_ts", TimestampType()),
        StructField("_batch_id", StringType()),
        StructField("_parse_error", StringType()),
    ]
)
SILVER_COLUMNS = [
    "brand_id",
    "product_id",
    "product_name",
    "category",
    "retail_price",
    "cost_price",
    "is_active",
    "_bronze_ref",
    "_bronze_batch_id",
    "_updated_at",
]


def row(**over: Any) -> tuple[Any, ...]:
    """A valid Bronze product row; keyword overrides replace single fields."""
    base: dict[str, Any] = {
        "schema_version": 1,
        "product_id": "PRD-00001",
        "payload_brand_id": ALPHA,
        "product_name": "Widget",
        "category": "Apparel",
        "retail_price": "12.50",
        "cost_price": "7.25",
        "is_active": True,
        "brand_id": ALPHA,
        "_raw_value": "{}",
        "_source_ref": "landing/products/f1.jsonl#L1",
        "_ingest_ts": T0,
        "_batch_id": "batch-1",
        "_parse_error": None,
    }
    base.update(over)
    return tuple(base[f.name] for f in BRONZE_SCHEMA.fields)


def bronze(spark: SparkSession, *rows: tuple[Any, ...]) -> DataFrame:
    return spark.createDataFrame(list(rows), BRONZE_SCHEMA)


def outcomes(df: DataFrame) -> dict[tuple[str, str, str], str]:
    """Map (brand_id, product_id, _source_ref) -> _outcome."""
    return {
        (r["brand_id"], r["product_id"], r["_source_ref"]): r["_outcome"]
        for r in classify_products(df).collect()
    }


def silver(spark: SparkSession, target: ResolvedTable) -> dict[tuple[str, str], Any]:
    rows = spark.read.format("delta").load(target.location).collect()
    return {(r["brand_id"], r["product_id"]): r for r in rows}


@pytest.fixture
def target(tmp_path: Path) -> ResolvedTable:
    return ResolvedTable(name=None, location=str(tmp_path / "silver_products"))


def test_outcomes_constant() -> None:
    assert OUTCOMES == ("parse_excluded", "dlq_excluded", "superseded", "accepted")


def test_valid_row_is_accepted(spark: SparkSession) -> None:
    assert set(outcomes(bronze(spark, row())).values()) == {"accepted"}


def test_classify_keeps_bronze_columns_plus_outcome(spark: SparkSession) -> None:
    cols = classify_products(bronze(spark, row())).columns
    assert cols == [f.name for f in BRONZE_SCHEMA.fields] + ["_outcome"]


def test_parse_error_row_is_parse_excluded_even_if_otherwise_invalid(spark: SparkSession) -> None:
    df = bronze(spark, row(_parse_error="bad json", product_id="nope", retail_price="-1"))
    assert set(outcomes(df).values()) == {"parse_excluded"}


@pytest.mark.parametrize(
    "override",
    [
        {"product_id": "PRD-1"},
        {"product_id": "prd-00001"},
        {"product_id": "PRD-000012"},
        {"product_id": None},
        {"product_name": ""},
        {"product_name": "x" * 256},
        {"product_name": None},
        {"category": "Toys"},
        {"category": None},
        {"retail_price": "0.00"},
        {"retail_price": "-5.00"},
        {"retail_price": "12.5"},
        {"retail_price": "abc"},
        {"retail_price": None},
        {"cost_price": "0.00"},
        {"cost_price": "-1.00"},
        {"cost_price": "7.255"},
        {"payload_brand_id": BETA},
        {"schema_version": 2},
        {"schema_version": None},
    ],
    ids=lambda o: "-".join(f"{k}={str(v)[:12]}" for k, v in o.items()),
)
def test_contract_violation_is_dlq_excluded(spark: SparkSession, override: dict[str, Any]) -> None:
    assert set(outcomes(bronze(spark, row(**override))).values()) == {"dlq_excluded"}


@pytest.mark.parametrize(
    "override",
    [
        {"cost_price": None},
        {"is_active": None},
        {"is_active": False},
        {"product_name": "x" * 255},
        {"product_name": "A"},
        {"category": "Electronics"},
        {"category": "Home"},
        {"category": "Beauty"},
        {"retail_price": "0.01"},
    ],
    ids=lambda o: "-".join(f"{k}={str(v)[:12]}" for k, v in o.items()),
)
def test_boundary_valid_row_is_accepted(spark: SparkSession, override: dict[str, Any]) -> None:
    assert set(outcomes(bronze(spark, row(**override))).values()) == {"accepted"}


def test_older_valid_copy_is_superseded_by_newer_ingest_ts(spark: SparkSession) -> None:
    df = bronze(
        spark,
        row(_source_ref="r-old", _ingest_ts=T0),
        row(_source_ref="r-new", _ingest_ts=T1),
    )
    out = outcomes(df)
    assert out[(ALPHA, "PRD-00001", "r-old")] == "superseded"
    assert out[(ALPHA, "PRD-00001", "r-new")] == "accepted"


def test_ingest_ts_tie_is_broken_by_source_ref_desc(spark: SparkSession) -> None:
    df = bronze(
        spark,
        row(_source_ref="landing/a#L1", _ingest_ts=T0),
        row(_source_ref="landing/b#L1", _ingest_ts=T0),
    )
    out = outcomes(df)
    assert out[(ALPHA, "PRD-00001", "landing/a#L1")] == "superseded"
    assert out[(ALPHA, "PRD-00001", "landing/b#L1")] == "accepted"


def test_invalid_newer_copy_does_not_supersede_older_valid_copy(spark: SparkSession) -> None:
    df = bronze(
        spark,
        row(_source_ref="r-old", _ingest_ts=T0),
        row(_source_ref="r-new", _ingest_ts=T1, retail_price="-1.00"),
    )
    out = outcomes(df)
    assert out[(ALPHA, "PRD-00001", "r-old")] == "accepted"
    assert out[(ALPHA, "PRD-00001", "r-new")] == "dlq_excluded"


def test_same_product_id_in_two_brands_is_not_superseding(spark: SparkSession) -> None:
    df = bronze(
        spark,
        row(brand_id=ALPHA, payload_brand_id=ALPHA, _source_ref="a", _ingest_ts=T1),
        row(brand_id=BETA, payload_brand_id=BETA, _source_ref="b", _ingest_ts=T0),
    )
    assert set(outcomes(df).values()) == {"accepted"}


def test_outcome_counts_sum_to_bronze_rows(spark: SparkSession) -> None:
    df = bronze(
        spark,
        row(product_id="PRD-00001", _source_ref="a1", _ingest_ts=T0),
        row(product_id="PRD-00001", _source_ref="a2", _ingest_ts=T1),
        row(product_id="PRD-00002", _source_ref="b1", _parse_error="boom"),
        row(product_id="PRD-00003", _source_ref="c1", category="Toys"),
        row(product_id="PRD-00004", _source_ref="d1"),
    )
    counts = {o: 0 for o in OUTCOMES}
    for r in classify_products(df).collect():
        counts[r["_outcome"]] += 1
    assert counts == {"parse_excluded": 1, "dlq_excluded": 1, "superseded": 1, "accepted": 2}
    assert sum(counts.values()) == df.count()


def test_latest_products_schema_and_values(spark: SparkSession) -> None:
    df = bronze(
        spark,
        row(_source_ref="r-old", _ingest_ts=T0, product_name="Old"),
        row(_source_ref="r-new", _ingest_ts=T1, product_name="New", is_active=None),
        row(product_id="PRD-00002", cost_price=None, _source_ref="x", _batch_id="b9"),
        row(product_id="PRD-00003", category="Toys"),
    )
    out = latest_products(df)
    assert out.columns == SILVER_COLUMNS[:-1]
    types = dict(out.dtypes)
    assert types["retail_price"] == "decimal(10,2)"
    assert types["cost_price"] == "decimal(10,2)"
    assert types["is_active"] == "boolean"
    rows = {r["product_id"]: r for r in out.collect()}
    assert set(rows) == {"PRD-00001", "PRD-00002"}
    p1 = rows["PRD-00001"]
    assert p1["product_name"] == "New"
    assert p1["is_active"] is True  # NULL in Bronze -> true
    assert p1["_bronze_ref"] == "r-new"
    assert p1["_bronze_batch_id"] == "batch-1"
    assert p1["retail_price"] == Decimal("12.50")
    assert rows["PRD-00002"]["cost_price"] is None
    assert rows["PRD-00002"]["_bronze_batch_id"] == "b9"


def test_latest_products_excludes_parse_and_dlq_rows(spark: SparkSession) -> None:
    df = bronze(
        spark,
        row(product_id="PRD-00001", _parse_error="bad"),
        row(product_id="PRD-00002", payload_brand_id=BETA),
    )
    assert latest_products(df).count() == 0


def test_first_run_inserts_and_creates_table(spark: SparkSession, target: ResolvedTable) -> None:
    df = bronze(
        spark,
        row(product_id="PRD-00001", _source_ref="a1", _ingest_ts=T0),
        row(product_id="PRD-00001", _source_ref="a2", _ingest_ts=T1, product_name="Newer"),
        row(product_id="PRD-00002", _source_ref="b1", cost_price=None),
        row(product_id="PRD-00003", _source_ref="c1", category="Toys"),
        row(product_id="PRD-00004", _source_ref="d1", _parse_error="x"),
    )
    counts = run_products(spark, df, target)
    assert counts == ProductCounts(
        bronze_rows=5,
        parse_excluded=1,
        dlq_excluded=1,
        superseded=1,
        accepted=2,
        inserted=2,
        updated=0,
    )
    assert (
        counts.bronze_rows
        == counts.parse_excluded + counts.dlq_excluded + counts.superseded + counts.accepted
    )
    table = spark.read.format("delta").load(target.location)
    assert table.columns == SILVER_COLUMNS
    assert dict(table.dtypes)["retail_price"] == "decimal(10,2)"
    assert dict(table.dtypes)["cost_price"] == "decimal(10,2)"
    rows = silver(spark, target)
    assert set(rows) == {(ALPHA, "PRD-00001"), (ALPHA, "PRD-00002")}
    assert rows[(ALPHA, "PRD-00001")]["product_name"] == "Newer"
    assert rows[(ALPHA, "PRD-00001")]["_bronze_ref"] == "a2"
    assert rows[(ALPHA, "PRD-00002")]["_updated_at"] is not None


def test_rerun_on_unchanged_bronze_is_a_noop(spark: SparkSession, target: ResolvedTable) -> None:
    df = bronze(spark, row(), row(product_id="PRD-00002", _source_ref="b1"))
    run_products(spark, df, target)
    before = sorted(
        spark.read.format("delta").load(target.location).collect(), key=lambda r: r["product_id"]
    )
    again = run_products(spark, df, target)
    after = sorted(
        spark.read.format("delta").load(target.location).collect(), key=lambda r: r["product_id"]
    )
    assert (again.inserted, again.updated) == (0, 0)
    assert again.accepted == 2
    assert before == after  # includes _updated_at: untouched rows keep their timestamp


def test_newer_relanded_copy_updates_existing_row(
    spark: SparkSession, target: ResolvedTable
) -> None:
    run_products(spark, bronze(spark, row(_source_ref="r1", _ingest_ts=T0)), target)
    newer = row(
        _source_ref="r2",
        _ingest_ts=T1,
        product_name="Renamed",
        retail_price="19.99",
        cost_price=None,
        is_active=False,
        _batch_id="batch-2",
    )
    counts = run_products(spark, bronze(spark, row(_source_ref="r1", _ingest_ts=T0), newer), target)
    assert (counts.inserted, counts.updated, counts.superseded) == (0, 1, 1)
    r = silver(spark, target)[(ALPHA, "PRD-00001")]
    assert r["product_name"] == "Renamed"
    assert r["retail_price"] == Decimal("19.99")
    assert r["cost_price"] is None
    assert r["is_active"] is False
    assert r["_bronze_ref"] == "r2"
    assert r["_bronze_batch_id"] == "batch-2"
    assert spark.read.format("delta").load(target.location).count() == 1


def test_merge_never_deletes_products_missing_from_bronze(
    spark: SparkSession, target: ResolvedTable
) -> None:
    run_products(spark, bronze(spark, row(), row(product_id="PRD-00002", _source_ref="b1")), target)
    counts = run_products(
        spark, bronze(spark, row(product_id="PRD-00003", _source_ref="c1")), target
    )
    assert (counts.inserted, counts.updated) == (1, 0)
    assert set(silver(spark, target)) == {
        (ALPHA, "PRD-00001"),
        (ALPHA, "PRD-00002"),
        (ALPHA, "PRD-00003"),
    }


def test_same_product_id_in_two_brands_yields_two_silver_rows(
    spark: SparkSession, target: ResolvedTable
) -> None:
    df = bronze(
        spark,
        row(brand_id=ALPHA, payload_brand_id=ALPHA, product_name="Alpha", _source_ref="a"),
        row(brand_id=BETA, payload_brand_id=BETA, product_name="Beta", _source_ref="b"),
    )
    counts = run_products(spark, df, target)
    assert (counts.accepted, counts.inserted) == (2, 2)
    rows = silver(spark, target)
    assert rows[(ALPHA, "PRD-00001")]["product_name"] == "Alpha"
    assert rows[(BETA, "PRD-00001")]["product_name"] == "Beta"


def test_brand_a_update_never_overwrites_brand_b(
    spark: SparkSession, target: ResolvedTable
) -> None:
    beta = row(brand_id=BETA, payload_brand_id=BETA, product_name="Beta", _source_ref="b1")
    alpha = row(product_name="Alpha", _source_ref="a1")
    run_products(spark, bronze(spark, alpha, beta), target)
    alpha2 = row(product_name="Alpha v2", _source_ref="a2", _ingest_ts=T2)
    counts = run_products(spark, bronze(spark, alpha, beta, alpha2), target)
    assert (counts.inserted, counts.updated) == (0, 1)
    rows = silver(spark, target)
    assert rows[(ALPHA, "PRD-00001")]["product_name"] == "Alpha v2"
    assert rows[(BETA, "PRD-00001")]["product_name"] == "Beta"
    assert rows[(BETA, "PRD-00001")]["_bronze_ref"] == "b1"


def test_tenant_isolation_check_detects_a_brand_blind_dedupe(spark: SparkSession) -> None:
    """Negative: a key without brand_id would collapse the two brands; our assertion sees it."""
    df = bronze(
        spark,
        row(brand_id=ALPHA, payload_brand_id=ALPHA, _source_ref="a"),
        row(brand_id=BETA, payload_brand_id=BETA, _source_ref="b"),
    )
    brand_blind = latest_products(df).dropDuplicates(["product_id"]).count()
    assert brand_blind == 1
    assert latest_products(df).count() == 2  # the real output keeps both


def test_counts_equation_check_detects_a_silent_loss() -> None:
    """Negative: the real reconciliation check fails if a category is dropped."""
    check_equation(5, 1, 1, 1, 2)  # balanced: no error
    with pytest.raises(ReconciliationError):
        check_equation(5, 1, 1, 0, 2)  # one row lost


def test_same_file_line_10_beats_line_9_numerically(spark: SparkSession) -> None:
    """Batch refs are ``<file>#L<n>`` and ``_ingest_ts`` is constant per batch: not lexical."""
    df = bronze(
        spark,
        row(_source_ref="landing/products/f1.jsonl#L9", _ingest_ts=T0),
        row(_source_ref="landing/products/f1.jsonl#L10", _ingest_ts=T0),
    )
    out = outcomes(df)
    assert out[(ALPHA, "PRD-00001", "landing/products/f1.jsonl#L9")] == "superseded"
    assert out[(ALPHA, "PRD-00001", "landing/products/f1.jsonl#L10")] == "accepted"


def test_later_file_beats_earlier_file_regardless_of_line(spark: SparkSession) -> None:
    df = bronze(
        spark,
        row(_source_ref="landing/products/f1.jsonl#L99", _ingest_ts=T0),
        row(_source_ref="landing/products/f2.jsonl#L1", _ingest_ts=T0),
    )
    out = outcomes(df)
    assert out[(ALPHA, "PRD-00001", "landing/products/f1.jsonl#L99")] == "superseded"
    assert out[(ALPHA, "PRD-00001", "landing/products/f2.jsonl#L1")] == "accepted"


def test_newer_parse_error_row_does_not_supersede_valid_copy(spark: SparkSession) -> None:
    df = bronze(
        spark,
        row(_source_ref="r-valid", _ingest_ts=T0),
        row(_source_ref="r-broken", _ingest_ts=T1, _parse_error="bad json"),
    )
    out = outcomes(df)
    assert out[(ALPHA, "PRD-00001", "r-valid")] == "accepted"
    assert out[(ALPHA, "PRD-00001", "r-broken")] == "parse_excluded"


def test_run_with_no_accepted_rows_creates_empty_table(
    spark: SparkSession, target: ResolvedTable
) -> None:
    df = bronze(spark, row(_parse_error="bad"), row(product_id="PRD-00002", category="Toys"))
    counts = run_products(spark, df, target)
    assert (counts.accepted, counts.inserted, counts.updated) == (0, 0, 0)
    assert (counts.bronze_rows, counts.parse_excluded, counts.dlq_excluded) == (2, 1, 1)
    table = spark.read.format("delta").load(target.location)
    assert table.columns == SILVER_COLUMNS
    assert table.count() == 0


def test_table_has_brand_id_not_null_constraint_and_rejects_null_brand(
    spark: SparkSession, target: ResolvedTable
) -> None:
    run_products(spark, bronze(spark, row()), target)
    props = spark.sql(f"SHOW TBLPROPERTIES delta.`{target.location}`").collect()
    constraints = {r["key"]: r["value"] for r in props if r["key"].startswith("delta.constraints.")}
    assert constraints == {"delta.constraints.brand_id_not_null": "brand_id IS NOT NULL"}
    with pytest.raises(Exception, match=r"(?i)brand_id"):
        spark.sql(
            f"INSERT INTO delta.`{target.location}` "
            "(brand_id, product_id) VALUES (CAST(NULL AS STRING), 'PRD-00009')"
        )
    assert spark.read.format("delta").load(target.location).count() == 1
