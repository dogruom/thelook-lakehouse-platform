"""Rewound-checkpoint guard of ``write_bronze_batch`` (W2.D9).

If a checkpoint loses ``offsets/`` and ``commits/`` but keeps its query id, batch ids restart at 0
and Delta skips the new data of an already recorded (app id, batch id). The guard must tell that
apart from a true replay (same message set) and fail loudly instead of losing data silently.

Runs inside the Spark image (``docker compose -p crm_lakehouse --profile tools run --rm
spark-test``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest


pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse.ingestion.bronze import (  # noqa: E402
    CUSTOMERS,
    CheckpointRewoundError,
    write_bronze_batch,
)


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame, SparkSession


KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField(
            "headers",
            ArrayType(
                StructType([StructField("key", StringType()), StructField("value", BinaryType())])
            ),
        ),
    ]
)
TS = datetime(2026, 3, 5, 10, 0, 0, tzinfo=UTC)
APP = "app-rewound"


def _frame(spark: SparkSession, offsets: list[int]) -> DataFrame:
    """Raw Kafka rows of partition 0 with the given offsets (unparseable payloads are fine)."""
    rows = [
        (b"k", b"{not json", "customer_cdc", 0, offset, TS, [("brand_id", b"brand_alpha")])
        for offset in offsets
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def _write(
    spark: SparkSession, path: Path, offsets: list[int], batch_id: int, app_id: str = APP
) -> None:
    write_bronze_batch(
        _frame(spark, offsets), str(path), target=CUSTOMERS, app_id=app_id, batch_id=batch_id
    )


def _stored_refs(spark: SparkSession, path: Path) -> list[str]:
    table = spark.read.format("delta").load(str(path))
    return sorted(r["_source_ref"] for r in table.select("_source_ref").collect())


def test_replay_of_the_same_message_set_adds_no_rows_and_raises_nothing(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = tmp_path / "raw_customers"
    _write(spark, path, [10, 11, 12], 0)
    _write(spark, path, [10, 11, 12], 0)
    assert _stored_refs(spark, path) == [f"customer_cdc/0/{o}" for o in (10, 11, 12)]


def test_different_messages_under_a_recorded_pair_raise_and_leave_the_table_unchanged(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = tmp_path / "raw_customers"
    _write(spark, path, [10, 11, 12], 0)
    before = _stored_refs(spark, path)
    with pytest.raises(CheckpointRewoundError, match="rewound") as excinfo:
        _write(spark, path, [20, 21], 0)
    message = str(excinfo.value)
    assert str(path) in message
    assert APP in message
    assert "batch 0" in message
    assert _stored_refs(spark, path) == before


def test_superset_of_the_stored_messages_under_a_recorded_pair_raises(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Negative: a count comparison would miss this only if sizes matched; the set must be equal."""
    path = tmp_path / "raw_customers"
    _write(spark, path, [10, 11], 0)
    with pytest.raises(CheckpointRewoundError):
        _write(spark, path, [10, 11, 12], 0)
    assert _stored_refs(spark, path) == ["customer_cdc/0/10", "customer_cdc/0/11"]


def test_same_size_but_different_messages_raise(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "raw_customers"
    _write(spark, path, [10, 11], 0)
    with pytest.raises(CheckpointRewoundError):
        _write(spark, path, [10, 99], 0)


def test_subset_of_the_stored_messages_under_a_recorded_pair_raises(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = tmp_path / "raw_customers"
    _write(spark, path, [10, 11, 12], 0)
    with pytest.raises(CheckpointRewoundError):
        _write(spark, path, [10, 11], 0)


def test_zero_row_first_batch_then_rows_under_the_same_pair_raises(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = tmp_path / "raw_customers"
    _write(spark, path, [], 0)
    with pytest.raises(CheckpointRewoundError):
        _write(spark, path, [5, 6], 0)
    assert _stored_refs(spark, path) == []


def test_replay_of_an_empty_batch_is_a_true_replay(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "raw_customers"
    _write(spark, path, [], 0)
    _write(spark, path, [], 0)
    assert _stored_refs(spark, path) == []


def test_next_batch_id_still_writes_after_a_rejected_rewound_batch(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = tmp_path / "raw_customers"
    _write(spark, path, [10], 0)
    with pytest.raises(CheckpointRewoundError):
        _write(spark, path, [20], 0)
    _write(spark, path, [20], 1)
    assert _stored_refs(spark, path) == ["customer_cdc/0/10", "customer_cdc/0/20"]


def test_new_app_id_with_the_same_batch_id_is_not_a_rewind(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Negative: a different checkpoint lineage is a different key and simply appends."""
    path = tmp_path / "raw_customers"
    _write(spark, path, [10], 0)
    _write(spark, path, [20], 0, app_id="app-other")
    assert _stored_refs(spark, path) == ["customer_cdc/0/10", "customer_cdc/0/20"]
