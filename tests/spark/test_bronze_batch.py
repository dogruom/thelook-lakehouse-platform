"""Landing files -> Bronze (W3.D13): one row per line, exact refs, schema evolution, idempotence.

Runs inside the Spark image on local paths (no S3): ``docker compose -p crm_lakehouse --profile
tools run --rm spark-test pytest tests/spark/test_bronze_batch.py -q -p no:cacheprovider``.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest


pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.utils import AnalysisException  # noqa: E402

from lakehouse.ingestion.batch import (  # noqa: E402
    FILE_PATH_COLUMN,
    ORDERS,
    PRODUCTS,
    BatchTarget,
    to_bronze_rows,
    write_bronze_files,
)
from lakehouse.ingestion.bronze import CheckpointRewoundError, append_idempotent  # noqa: E402
from lakehouse.ingestion.jobs.bronze_batch import _Counters, source_glob, start_query  # noqa: E402


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame, SparkSession

ALPHA = "brand_alpha"
BETA = "brand_beta"
DATE = "2026-03-05"


def _order(brand: str, n: int, channel: str | None = None) -> str:
    oid = f"00000000-0000-4000-8000-{n:012d}"
    row: dict[str, Any] = {
        "schema_version": 1,
        "order_id": oid,
        "brand_id": brand,
        "customer_id": "11111111-1111-4111-8111-111111111111",
        "order_timestamp": "2026-03-05T10:00:00+00:00",
        "order_status": "completed",
        "payment_method": "paypal",
        "currency": "EUR",
        "gross_amount": "20.00",
        "discount_amount": "0.00",
        "net_amount": "20.00",
        "items": [
            {
                "item_id": f"22222222-2222-4222-8222-{n:012d}",
                "order_id": oid,
                "brand_id": brand,
                "product_id": "PRD-00001",
                "quantity": 2,
                "unit_price": "10.00",
                "line_total": "20.00",
            }
        ],
    }
    if channel is not None:
        row["sales_channel"] = channel
    return json.dumps(row)


def _land(root: Path, brand: str, entity: str, name: str, lines: list[str] | bytes) -> None:
    folder = root / "landing" / brand / entity / DATE
    folder.mkdir(parents=True, exist_ok=True)
    if isinstance(lines, bytes):
        (folder / name).write_bytes(lines)
    else:
        (folder / name).write_text("".join(f"{line}\n" for line in lines), encoding="utf-8")


def _ingest(spark: SparkSession, root: Path, target: BatchTarget, tag: str = "t") -> _Counters:
    """Drain the landing files of ``target`` with one availableNow query (same wiring as the job)."""
    counters = _Counters()
    query = start_query(
        spark,
        source_glob(f"{root.as_posix()}/landing", target),
        target,
        f"{root.as_posix()}/bronze/{target.table}",
        f"{root.as_posix()}/checkpoints/{target.table}",
        100,
        counters,
    )
    query.awaitTermination()
    assert query.exception() is None
    return counters


def _bronze(spark: SparkSession, root: Path, target: BatchTarget) -> DataFrame:
    return spark.read.format("delta").load(f"{root.as_posix()}/bronze/{target.table}")


def test_one_row_per_line_with_exact_refs_and_path_brand(
    spark: SparkSession, tmp_path: Path
) -> None:
    lines = [_order(ALPHA, 1), "{not json", _order(ALPHA, 2)]
    _land(tmp_path, ALPHA, "orders", "batch-00001.ndjson", lines)
    _land(tmp_path, ALPHA, "orders", "batch-00001.manifest.json", ['{"rows": 3}'])
    # the payload claims another brand than the landing prefix: both must stay visible
    _land(tmp_path, BETA, "orders", "batch-00001.ndjson", [_order(ALPHA, 3)])

    counters = _ingest(spark, tmp_path, ORDERS)

    rows = {r["_source_ref"]: r for r in _bronze(spark, tmp_path, ORDERS).collect()}
    assert sorted(rows) == [
        f"landing/{ALPHA}/orders/{DATE}/batch-00001.ndjson#L1",
        f"landing/{ALPHA}/orders/{DATE}/batch-00001.ndjson#L2",
        f"landing/{ALPHA}/orders/{DATE}/batch-00001.ndjson#L3",
        f"landing/{BETA}/orders/{DATE}/batch-00001.ndjson#L1",
    ]  # the manifest is not ingested
    bad = rows[f"landing/{ALPHA}/orders/{DATE}/batch-00001.ndjson#L2"]
    assert bad["_parse_error"] == "malformed_json"
    assert bad["_raw_value"] == "{not json"
    assert bad["order_id"] is None
    assert bad["brand_id"] == ALPHA  # from the path even though the line did not parse
    good = rows[f"landing/{ALPHA}/orders/{DATE}/batch-00001.ndjson#L3"]
    assert good["_parse_error"] is None
    assert good["order_id"].endswith("0002")
    assert good["gross_amount"] == "20.00"  # money stays a string
    assert good["order_timestamp"] == "2026-03-05T10:00:00+00:00"
    assert good["items"][0]["quantity"] == 2
    leak = rows[f"landing/{BETA}/orders/{DATE}/batch-00001.ndjson#L1"]
    assert (leak["brand_id"], leak["payload_brand_id"]) == (BETA, ALPHA)
    assert good["_batch_id"].endswith(":0")
    assert counters.files == 2
    assert (counters.lines, counters.parse_errors) == (4, 1)
    assert counters.brands() == {
        ALPHA: {"files": 1, "lines": 3, "parse_errors": 1},
        BETA: {"files": 1, "lines": 1, "parse_errors": 0},
    }


def test_products_land_in_their_own_table(spark: SparkSession, tmp_path: Path) -> None:
    product = {
        "schema_version": 1,
        "product_id": "PRD-00001",
        "brand_id": ALPHA,
        "product_name": "Shirt",
        "category": "Apparel",
        "retail_price": "19.90",
        "is_active": True,
    }
    _land(tmp_path, ALPHA, "products", "products.ndjson", [json.dumps(product)])
    _land(tmp_path, ALPHA, "products", "products.manifest.json", ["{}"])

    _ingest(spark, tmp_path, PRODUCTS)

    (row,) = _bronze(spark, tmp_path, PRODUCTS).collect()
    assert row["_source_ref"] == f"landing/{ALPHA}/products/{DATE}/products.ndjson#L1"
    assert (row["brand_id"], row["product_id"], row["retail_price"]) == (
        ALPHA,
        "PRD-00001",
        "19.90",
    )
    assert row["cost_price"] is None


def test_blank_line_and_missing_trailing_newline(spark: SparkSession, tmp_path: Path) -> None:
    text = f"{_order(ALPHA, 1)}\n\n{_order(ALPHA, 2)}".encode()  # blank middle, no final newline
    _land(tmp_path, ALPHA, "orders", "batch-00001.ndjson", text)

    _ingest(spark, tmp_path, ORDERS)

    by_ref = {
        r["_source_ref"].rsplit("#L", 1)[1]: r for r in _bronze(spark, tmp_path, ORDERS).collect()
    }
    assert sorted(by_ref) == ["1", "2", "3"]
    assert by_ref["2"]["_parse_error"] == "malformed_json"
    assert by_ref["3"]["order_id"].endswith("0002")


def test_invalid_utf8_line_is_kept_and_flagged(spark: SparkSession, tmp_path: Path) -> None:
    payload = _order(ALPHA, 1).encode() + b"\n" + b"\xff\xfe{broken\n" + _order(ALPHA, 2).encode()
    _land(tmp_path, ALPHA, "orders", "batch-00001.ndjson", payload + b"\n")

    _ingest(spark, tmp_path, ORDERS)

    rows = {
        r["_source_ref"].rsplit("#L", 1)[1]: r for r in _bronze(spark, tmp_path, ORDERS).collect()
    }
    assert sorted(rows) == ["1", "2", "3"]
    assert rows["2"]["_parse_error"] == "malformed_json"
    assert rows["1"]["_parse_error"] is None
    assert rows["3"]["_parse_error"] is None


def test_sales_channel_null_before_batch_10_and_set_from_batch_10(
    spark: SparkSession, tmp_path: Path
) -> None:
    _land(tmp_path, ALPHA, "orders", "batch-00001.ndjson", [_order(ALPHA, 1)])
    _land(tmp_path, ALPHA, "orders", "batch-00009.ndjson", [_order(ALPHA, 2)])
    _land(tmp_path, ALPHA, "orders", "batch-00010.ndjson", [_order(ALPHA, 3, "web")])
    _land(tmp_path, ALPHA, "orders", "batch-00011.ndjson", [_order(ALPHA, 4, "store")])

    _ingest(spark, tmp_path, ORDERS)

    by_file = {
        r["_source_ref"].split("/")[-1].split("#")[0]: r["sales_channel"]
        for r in _bronze(spark, tmp_path, ORDERS).collect()
    }
    assert by_file == {
        "batch-00001.ndjson": None,
        "batch-00009.ndjson": None,
        "batch-00010.ndjson": "web",
        "batch-00011.ndjson": "store",
    }


def test_additive_field_evolves_an_existing_table(spark: SparkSession, tmp_path: Path) -> None:
    """A table written before ``sales_channel`` existed gains the column on the next ingest."""
    _land(tmp_path, ALPHA, "orders", "batch-00001.ndjson", [_order(ALPHA, 1)])
    _ingest(spark, tmp_path, ORDERS)
    path = f"{tmp_path.as_posix()}/bronze/{ORDERS.table}"
    # simulate the table of an older job version: same rows, no sales_channel column
    old = spark.read.format("delta").load(path).drop("sales_channel")
    old.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(
        "brand_id"
    ).save(path)
    assert "sales_channel" not in spark.read.format("delta").load(path).columns

    _land(tmp_path, ALPHA, "orders", "batch-00010.ndjson", [_order(ALPHA, 2, "app")])
    _ingest(spark, tmp_path, ORDERS)

    table = spark.read.format("delta").load(path)
    assert "sales_channel" in table.columns
    got = {r["order_id"][-4:]: r["sales_channel"] for r in table.collect()}
    assert got == {"0001": None, "0002": "app"}


def test_extra_column_needs_merge_schema(spark: SparkSession, tmp_path: Path) -> None:
    """A column outside the current table schema is evolved with mergeSchema, refused without."""
    path = f"{tmp_path.as_posix()}/bronze/t"
    base = spark.createDataFrame(
        [("a#L1", "app:0")], "_source_ref string, _batch_id string"
    ).withColumn("order_id", F.lit("o1"))
    append_idempotent(base, path, app_id="app", batch_id=0, merge_schema=True)
    wider = (
        spark.createDataFrame([("a#L2", "app:1")], "_source_ref string, _batch_id string")
        .withColumn("order_id", F.lit("o2"))
        .withColumn("loyalty_tier", F.lit("gold"))
    )

    with pytest.raises(AnalysisException):
        append_idempotent(wider, path, app_id="app", batch_id=1)
    assert "loyalty_tier" not in spark.read.format("delta").load(path).columns

    append_idempotent(wider, path, app_id="app", batch_id=1, merge_schema=True)
    table = spark.read.format("delta").load(path)
    assert "loyalty_tier" in table.columns
    assert {r["order_id"]: r["loyalty_tier"] for r in table.collect()} == {"o1": None, "o2": "gold"}


def test_rerun_without_new_files_writes_nothing(spark: SparkSession, tmp_path: Path) -> None:
    _land(tmp_path, ALPHA, "orders", "batch-00001.ndjson", [_order(ALPHA, 1), _order(ALPHA, 2)])
    first = _ingest(spark, tmp_path, ORDERS)
    path = f"{tmp_path.as_posix()}/bronze/{ORDERS.table}"
    version = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").agg(F.max("version")).first()[0]  # type: ignore[index]

    second = _ingest(spark, tmp_path, ORDERS)

    assert (first.lines, first.batches) == (2, 1)
    assert (second.files, second.lines, second.batches) == (0, 0, 0)
    assert spark.read.format("delta").load(path).count() == 2
    assert spark.sql(f"DESCRIBE HISTORY delta.`{path}`").agg(F.max("version")).first()[0] == version  # type: ignore[index]

    # a new file is picked up on the next run, the old one is not read again
    _land(tmp_path, ALPHA, "orders", "batch-00002.ndjson", [_order(ALPHA, 3)])
    third = _ingest(spark, tmp_path, ORDERS)
    assert (third.files, third.lines) == (1, 1)
    assert spark.read.format("delta").load(path).count() == 3


def _file_frame(spark: SparkSession, root: Path, name: str, lines: list[str]) -> DataFrame:
    path = f"file:{root.as_posix()}/landing/{ALPHA}/orders/{DATE}/{name}"
    return spark.createDataFrame(
        [(path, "".join(f"{x}\n" for x in lines))], f"{FILE_PATH_COLUMN} string, value string"
    )


def test_replayed_micro_batch_is_not_duplicated(spark: SparkSession, tmp_path: Path) -> None:
    path = f"{tmp_path.as_posix()}/bronze/{ORDERS.table}"
    frame = _file_frame(spark, tmp_path, "batch-00001.ndjson", [_order(ALPHA, 1), "oops"])

    first = write_bronze_files(frame, path, target=ORDERS, app_id="app-x", batch_id=0)
    replay = write_bronze_files(frame, path, target=ORDERS, app_id="app-x", batch_id=0)

    assert (first.lines, first.parse_errors) == (2, 1)
    assert (replay.lines, replay.parse_errors) == (2, 1)  # seen again, but Delta skipped the write
    assert spark.read.format("delta").load(path).count() == 2


def test_to_bronze_rows_schema(spark: SparkSession, tmp_path: Path) -> None:
    rows = to_bronze_rows(
        _file_frame(spark, tmp_path, "batch-00001.ndjson", [_order(ALPHA, 1)]), ORDERS, "q:0"
    )
    assert rows.columns == [
        "schema_version",
        "order_id",
        "payload_brand_id",
        "customer_id",
        "order_timestamp",
        "order_status",
        "payment_method",
        "currency",
        "gross_amount",
        "discount_amount",
        "net_amount",
        "items",
        "sales_channel",
        "brand_id",
        "_raw_value",
        "_source_ref",
        "_ingest_ts",
        "_batch_id",
        "_parse_error",
    ]


def test_namespaced_run_reads_only_its_landing_prefix(spark: SparkSession, tmp_path: Path) -> None:
    """``--namespace`` isolates landing too (D-069): default landing files are not read, the brand
    is still the segment before the entity and ``_source_ref`` names the real namespaced key."""
    ns = "it_0a1b2c3d"
    _land(tmp_path, ALPHA, "orders", "batch-00001.ndjson", [_order(ALPHA, 1)])  # default data
    _land(tmp_path / "landing" / ns, BETA, "orders", "batch-00001.ndjson", [_order(BETA, 2)])
    # ``_land`` adds ``landing/`` itself; move the namespaced file one level up to
    # ``landing/<ns>/<brand>/...``.
    nested = tmp_path / "landing" / ns / "landing"
    (nested / BETA).rename(tmp_path / "landing" / ns / BETA)
    nested.rmdir()

    query = start_query(
        spark,
        source_glob(f"{tmp_path.as_posix()}/landing/{ns}", ORDERS),
        ORDERS,
        f"{tmp_path.as_posix()}/bronze/{ns}/{ORDERS.table}",
        f"{tmp_path.as_posix()}/checkpoints/{ns}/{ORDERS.table}",
        100,
        _Counters(),
    )
    query.awaitTermination()
    assert query.exception() is None

    rows = (
        spark.read.format("delta")
        .load(f"{tmp_path.as_posix()}/bronze/{ns}/{ORDERS.table}")
        .select("brand_id", "_source_ref")
        .collect()
    )
    assert [(r["brand_id"], r["_source_ref"]) for r in rows] == [
        (BETA, f"landing/{ns}/{BETA}/orders/{DATE}/batch-00001.ndjson#L1")
    ]


def test_valid_json_with_wrong_field_types_is_kept_with_null_fields(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Pins Spark 3.5 ``from_json`` (PERMISSIVE, partial results): the row stays, ``_raw_value``
    is intact, ``_parse_error`` is NULL and only the mistyped field is NULL; the rest is parsed.
    The type violation is for the DLQ gate (contract failure), not a parse error."""
    wrong = json.loads(_order(ALPHA, 1))
    wrong["items"] = "not-a-list"
    wrong["schema_version"] = "x"
    nested = json.loads(_order(ALPHA, 2))
    nested["items"][0]["quantity"] = "abc"
    raw_lines = [json.dumps(wrong), json.dumps(nested)]
    _land(tmp_path, ALPHA, "orders", "batch-00001.ndjson", raw_lines)

    _ingest(spark, tmp_path, ORDERS)

    rows = sorted(_bronze(spark, tmp_path, ORDERS).collect(), key=lambda r: r["_source_ref"])
    assert [r["_raw_value"] for r in rows] == raw_lines
    assert [r["_parse_error"] for r in rows] == [None, None]
    top, deep = rows
    assert top["schema_version"] is None  # "x" is not an int
    assert top["items"] is None  # a string is not an array
    assert top["order_id"].endswith("0001")  # the other fields survive
    assert top["gross_amount"] == "20.00"
    assert deep["schema_version"] == 1
    assert deep["items"][0]["quantity"] is None  # only the mistyped item field is NULL
    assert deep["items"][0]["line_total"] == "20.00"


def test_zero_byte_file_yields_no_rows_and_no_error(spark: SparkSession, tmp_path: Path) -> None:
    """Spark's file source hands no row over for an empty file: no batch, no Bronze table yet."""
    _land(tmp_path, ALPHA, "orders", "batch-00001.ndjson", b"")

    counters = _ingest(spark, tmp_path, ORDERS)

    assert (counters.files, counters.lines, counters.parse_errors) == (0, 0, 0)
    assert not (tmp_path / "bronze" / ORDERS.table).exists()

    # the empty file does not block later files
    _land(tmp_path, ALPHA, "orders", "batch-00002.ndjson", [_order(ALPHA, 1)])
    _ingest(spark, tmp_path, ORDERS)
    assert _bronze(spark, tmp_path, ORDERS).count() == 1


def test_rewound_checkpoint_on_the_file_path_raises(spark: SparkSession, tmp_path: Path) -> None:
    """Same (app id, batch id) with other files is a lost-checkpoint rewind, not a replay."""
    path = f"{tmp_path.as_posix()}/bronze/{ORDERS.table}"
    first = _file_frame(spark, tmp_path, "batch-00001.ndjson", [_order(ALPHA, 1)])
    other = _file_frame(spark, tmp_path, "batch-00002.ndjson", [_order(ALPHA, 2)])
    write_bronze_files(first, path, target=ORDERS, app_id="app-r", batch_id=0)

    with pytest.raises(CheckpointRewoundError, match="rewound"):
        write_bronze_files(other, path, target=ORDERS, app_id="app-r", batch_id=0)

    refs = [r["_source_ref"] for r in spark.read.format("delta").load(path).collect()]
    assert refs == [f"landing/{ALPHA}/orders/{DATE}/batch-00001.ndjson#L1"]
