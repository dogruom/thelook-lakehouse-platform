"""Silver clickstream (W3.D17, D-079), derived from DATA_CONTRACTS sections 2.5, 3.2 and 4.3 and
the approved design, not from the job code.

Outcome precedence: parse -> reread -> dlq_excluded -> late_beyond_window -> duplicate -> accepted.
The window is ``event_date >= date(hwm) - lookback_days`` where ``hwm`` is the maximum
``event_timestamp`` over the target and the batch (event time, never wall clock).

Frames are Kafka-shaped rows run through the production ``to_bronze_rows`` so ``event_date`` (the
Bronze partition column) and ``_source_ref`` are realistic.

Runs inside the Spark image (``docker compose --profile tools run --rm spark-test``).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

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

from lakehouse.ingestion.bronze import CLICKSTREAM, to_bronze_rows  # noqa: E402
from lakehouse.silver import clickstream as click_mod  # noqa: E402
from lakehouse.silver.clickstream import process_clickstream_batch  # noqa: E402


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
NOW = datetime(2026, 11, 11, 12, 0, 0, tzinfo=UTC)
KAFKA_TS = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
ALPHA, BETA = "brand_alpha", "brand_beta"
SESSION = "44444444-4444-4444-8444-444444444444"
LOOKBACK = 7


def eid(n: int) -> str:
    return f"{n:08x}-0000-4000-8000-000000000000"


def ev(
    n: int,
    ts: str = "2026-03-10T09:00:00Z",
    *,
    brand: str = ALPHA,
    url: str = "/home",
    **over: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event_id": eid(n),
        "brand_id": brand,
        "schema_version": 1,
        "produced_at": "2026-03-10T09:00:01Z",
        "session_id": SESSION,
        "customer_id": None,
        "event_type": "page_view",
        "page_url": url,
        "device_category": "mobile",
        "event_timestamp": ts,
    }
    payload.update(over)
    return payload


class Lake:
    def __init__(self, spark: SparkSession, root: Path) -> None:
        self.spark = spark
        self.path = (root / "silver_clickstream").as_posix()
        self.offset = 0
        self.batches = 0
        ensure = getattr(click_mod, "ensure_table", None)
        if ensure is not None:
            ensure(spark, self.path)

    def batch(self, events: list[dict[str, Any] | bytes], partition: int = 0) -> DataFrame:
        """Kafka-shaped rows -> Bronze rows (one Kafka offset each, in the given order)."""
        self.batches += 1
        rows = []
        for e in events:
            n = self.offset
            self.offset += 1
            if isinstance(e, bytes):
                key, value, headers = (
                    f"{ALPHA}:{SESSION}".encode(),
                    e,
                    [("brand_id", ALPHA.encode())],
                )
            else:
                brand = e["brand_id"]
                key = f"{brand}:{e.get('session_id') or ''}".encode()
                value = json.dumps(e, separators=(",", ":")).encode()
                headers = [("brand_id", str(brand).encode())]
            rows.append((key, value, "clickstream_events", partition, n, KAFKA_TS, headers))
        frame = self.spark.createDataFrame(rows, KAFKA_SCHEMA)
        return to_bronze_rows(frame, CLICKSTREAM, f"batch-{self.batches}")

    def process(self, events: list[dict[str, Any] | bytes], lookback: int = LOOKBACK) -> Any:
        return process_clickstream_batch(self.spark, self.batch(events), self.path, lookback, NOW)

    def rows(self) -> dict[tuple[str, str], dict[str, Any]]:
        try:
            data = [r.asDict() for r in self.spark.read.format("delta").load(self.path).collect()]
        except Exception:
            data = []
        out = {(r["brand_id"], r["event_id"]): r for r in data}
        assert len(out) == len(data), "Silver clickstream holds a duplicate (brand_id, event_id)"
        return out


@pytest.fixture
def lake(spark: SparkSession, tmp_path: Path) -> Lake:
    return Lake(spark, tmp_path)


def sum_holds(c: Any) -> bool:
    return c.rows_read - c.reread_rows == (
        c.parse_excluded
        + c.dlq_excluded
        + c.late_beyond_window
        + c.duplicates_removed
        + c.silver_accepted
    )


# ---- basics ----------------------------------------------------------------------------------


def test_counter_names_are_the_contract_ones(lake: Lake) -> None:
    counts = lake.process([ev(1)])
    for name in (
        "rows_read",
        "reread_rows",
        "parse_excluded",
        "dlq_excluded",
        "duplicates_removed",
        "late_beyond_window",
        "silver_accepted",
    ):
        assert hasattr(counts, name), name
    assert not hasattr(counts, "fk_orphans")  # clickstream has no foreign key


def test_clean_events_are_accepted_and_the_table_is_partitioned_by_event_date(lake: Lake) -> None:
    counts = lake.process([ev(1), ev(2, "2026-03-09T23:59:59Z")])
    assert (counts.rows_read, counts.silver_accepted) == (2, 2)
    assert sum_holds(counts)
    rows = lake.rows()
    assert rows[(ALPHA, eid(1))]["event_date"] == date(2026, 3, 10)
    assert rows[(ALPHA, eid(2))]["event_date"] == date(2026, 3, 9)  # UTC date of event time
    detail = lake.spark.sql(f"DESCRIBE DETAIL delta.`{lake.path}`").collect()[0]
    assert list(detail["partitionColumns"]) == ["event_date"]


def test_null_session_is_dlq_excluded_and_malformed_json_is_parse_excluded(lake: Lake) -> None:
    counts = lake.process([ev(1), ev(2, session_id=None), b"{truncated"])
    assert (counts.rows_read, counts.dlq_excluded, counts.parse_excluded) == (3, 1, 1)
    assert counts.silver_accepted == 1
    assert sum_holds(counts)
    assert set(lake.rows()) == {(ALPHA, eid(1))}


# ---- duplicates ------------------------------------------------------------------------------


def test_in_batch_duplicate_keeps_the_lowest_offset_and_counts_one(lake: Lake) -> None:
    counts = lake.process([ev(1, url="/first"), ev(1, url="/second"), ev(1, url="/third")])
    assert (counts.duplicates_removed, counts.silver_accepted) == (2, 1)
    assert sum_holds(counts)
    assert lake.rows()[(ALPHA, eid(1))]["page_url"] == "/first"


def test_lowest_kafka_partition_offset_wins_not_arrival_order_in_the_frame(
    lake: Lake, spark: SparkSession
) -> None:
    first = lake.batch([ev(1, url="/offset0")])
    second = lake.batch([ev(1, url="/offset1")])
    union = second.unionByName(first)  # higher offset listed first
    counts = process_clickstream_batch(spark, union, lake.path, LOOKBACK, NOW)
    assert (counts.duplicates_removed, counts.silver_accepted) == (1, 1)
    assert lake.rows()[(ALPHA, eid(1))]["page_url"] == "/offset0"


def test_duplicate_in_a_later_batch_is_counted_and_leaves_silver_unchanged(lake: Lake) -> None:
    lake.process([ev(1, url="/orig")])
    before = lake.rows()
    counts = lake.process([ev(1, url="/resent")])
    assert (counts.duplicates_removed, counts.silver_accepted) == (1, 0)
    assert sum_holds(counts)
    assert lake.rows() == before


def test_same_event_id_in_two_brands_is_not_a_duplicate(lake: Lake) -> None:
    counts = lake.process([ev(1, brand=ALPHA), ev(1, brand=BETA)])
    assert (counts.duplicates_removed, counts.silver_accepted) == (0, 2)
    assert set(lake.rows()) == {(ALPHA, eid(1)), (BETA, eid(1))}


def test_reprocessing_the_same_events_changes_nothing(lake: Lake) -> None:
    events = [ev(1), ev(2), ev(2)]
    lake.process(events)
    before = lake.rows()
    counts = lake.process(events)
    assert lake.rows() == before
    assert counts.silver_accepted == 0 and counts.duplicates_removed == 3
    assert sum_holds(counts)


# ---- lookback window -------------------------------------------------------------------------


def test_event_older_than_the_window_is_counted_late_and_not_written(lake: Lake) -> None:
    # hwm = 2026-03-10 -> lower bound 2026-03-03 (inclusive)
    counts = lake.process(
        [
            ev(1, "2026-03-10T09:00:00Z"),
            ev(2, "2026-03-03T00:00:00Z"),  # exactly on the boundary: accepted
            ev(3, "2026-03-02T23:59:59Z"),  # one second before the boundary date: late
            ev(4, "2026-02-01T00:00:00Z"),  # far beyond: late
        ]
    )
    assert (counts.late_beyond_window, counts.silver_accepted) == (2, 2)
    assert sum_holds(counts)
    assert set(lake.rows()) == {(ALPHA, eid(1)), (ALPHA, eid(2))}


def test_window_is_relative_to_the_event_time_high_water_mark_of_the_target(lake: Lake) -> None:
    lake.process([ev(1, "2026-03-10T09:00:00Z")])
    # The new batch is all old; only the target's hwm (03-10) makes 03-02 late. A batch-local
    # hwm (03-02) would wrongly accept it. Wall clock (NOW = November) must play no role.
    counts = lake.process([ev(2, "2026-03-02T10:00:00Z"), ev(3, "2026-03-05T10:00:00Z")])
    assert (counts.late_beyond_window, counts.silver_accepted) == (1, 1)
    assert (ALPHA, eid(2)) not in lake.rows() and (ALPHA, eid(3)) in lake.rows()


def test_lookback_days_is_a_parameter(lake: Lake) -> None:
    events = [ev(1, "2026-03-10T09:00:00Z"), ev(2, "2026-03-05T09:00:00Z")]
    narrow = lake.process(events, lookback=2)  # lower bound 03-08
    assert (narrow.late_beyond_window, narrow.silver_accepted) == (1, 1)
    wide = lake.process(events, lookback=7)  # event 2 is now inside the window
    assert (wide.late_beyond_window, wide.silver_accepted, wide.duplicates_removed) == (0, 1, 1)
    assert set(lake.rows()) == {(ALPHA, eid(1)), (ALPHA, eid(2))}


def test_late_beyond_window_takes_precedence_over_duplicate(lake: Lake) -> None:
    lake.process([ev(1, "2026-03-10T09:00:00Z"), ev(2, "2026-03-10T09:30:00Z")])
    # A re-send of event 3 with an old timestamp: it is late (window check first), not a duplicate.
    counts = lake.process([ev(3, "2026-02-20T09:00:00Z"), ev(3, "2026-02-20T09:00:00Z")])
    assert (counts.late_beyond_window, counts.duplicates_removed) == (2, 0)
    assert sum_holds(counts)


def test_old_event_for_a_known_duplicate_key_leaves_the_stored_row_untouched(lake: Lake) -> None:
    """MERGE is bounded by the window: an out-of-window re-send never reaches the stored row."""
    lake.process([ev(1, "2026-03-10T09:00:00Z", url="/kept")])
    lake.process([ev(2, "2026-03-20T09:00:00Z")])  # hwm moves on; event 1 is now outside
    counts = lake.process([ev(1, "2026-03-10T09:00:00Z", url="/resent")])
    assert counts.silver_accepted == 0
    assert lake.rows()[(ALPHA, eid(1))]["page_url"] == "/kept"
    assert sum_holds(counts)
