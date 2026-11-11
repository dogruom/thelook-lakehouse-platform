"""Silver customers (W3.D14), derived from DATA_CONTRACTS sections 2.1 and 4, decisions D-022 to
D-029 and the ground-truth semantics of ``manifest.classify_outcomes`` - not from the job code.

Frames are built the way production builds them: Kafka-shaped rows -> ``to_bronze_rows`` ->
append to a tmp Delta Bronze table, so ``_source_ref`` is a realistic ``topic/partition/offset``.
Multi-event scenarios are cross-checked against the manifest state machine, batch by batch.

Runs inside the Spark image (``docker compose --profile tools run --rm spark-test``).
"""

from __future__ import annotations

import dataclasses
import hashlib
import hmac
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import pytest


pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402
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

from lakehouse.common.pii import email_token_column  # noqa: E402
from lakehouse.generator.manifest import ManifestEntry, Outcome, classify_outcomes  # noqa: E402
from lakehouse.ingestion.bronze import CUSTOMERS, to_bronze_rows  # noqa: E402
from lakehouse.silver.customers import (  # noqa: E402
    MultipleMatchError,
    classify_batch,
    ensure_table,
    merge_customers,
    merge_source,
    process_batch,
    silver_customers_schema,
)


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame, SparkSession

    from lakehouse.silver.customers import CustomerBatchCounts


KEY = b"test-key-" * 4  # obvious test key, 36 bytes
NOW = datetime(2026, 11, 11, 12, 0, 0, tzinfo=UTC)
KAFKA_TS = datetime(2026, 3, 5, 10, 0, 0, tzinfo=UTC)
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
ALPHA, BETA = "brand_alpha", "brand_beta"
C1, C2 = "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"
BAD_EMAIL = {"email": "not-an-email"}


@dataclass
class Ev:
    """One CDC emission. ``bad`` marks a payload the contract gate must reject."""

    brand: str
    cust: str
    seq: int
    action: str = "I"
    over: dict[str, Any] = field(default_factory=dict)
    bad: bool = False
    raw: bytes | None = None  # replaces the JSON value (parse-error cases)
    partition: int = 0


def _payload(ev: Ev, n: int) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event_id": f"{n:08x}-0000-4000-8000-000000000000",
        "customer_id": ev.cust,
        "brand_id": ev.brand,
        "schema_version": 1,
        "produced_at": "2026-01-01T10:00:00.123Z",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": "Ada@Example.com",
        "phone": "+447700900123",
        "city": f"City{ev.seq}",
        "country": "DE",
        "loyalty_tier": "Gold",
        "signup_date": "2025-12-01",
        "_cdc_action": ev.action,
        "_cdc_sequence": ev.seq,
        "_cdc_ts": "2026-01-01T10:00:00.000Z",
    }
    payload.update(ev.over)
    return payload


class Lake:
    """Tmp Bronze + Silver Delta tables and the manifest bookkeeping of everything ingested."""

    def __init__(self, spark: SparkSession, root: Path) -> None:
        self.spark = spark
        self.bronze_path = (root / "bronze").as_posix()
        self.silver_path = (root / "silver").as_posix()
        self.offset = 0
        self.batches = 0
        self.entries: list[ManifestEntry] = []
        self.valid: list[bool] = []
        self.batch_ends: list[int] = []
        self.batch_counts: list[CustomerBatchCounts] = []
        ensure_table(spark, self.silver_path)

    def bronze(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.bronze_path)

    def silver(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.silver_path)

    def append(self, events: list[Ev]) -> DataFrame:
        """Append events to Bronze (one Kafka offset each); return the new rows as the batch."""
        self.batches += 1
        batch_id = f"batch-{self.batches}"
        rows = []
        for ev in events:
            n = self.offset
            self.offset += 1
            value = ev.raw if ev.raw is not None else json.dumps(_payload(ev, n)).encode()
            key = f"{ev.brand}:{ev.cust}".encode()
            headers = [("brand_id", ev.brand.encode())]
            rows.append((key, value, "customer_cdc", ev.partition, n, KAFKA_TS, headers))
            if ev.raw is None:
                self.entries.append(
                    ManifestEntry(len(self.entries), f"e{n}", ev.brand, ev.cust, ev.seq, ev.action)
                )
                self.valid.append(not ev.bad)
        frame = self.spark.createDataFrame(rows, KAFKA_SCHEMA)
        to_bronze_rows(frame, CUSTOMERS, batch_id).write.format("delta").mode("append").save(
            self.bronze_path
        )
        self.batch_ends.append(len(self.entries))
        return self.bronze().where(F.col("_batch_id") == batch_id)

    def process(self, batch: DataFrame) -> CustomerBatchCounts:
        """Process one already-appended batch against the current Bronze (which may hold more)."""
        counts = process_batch(self.spark, batch, self.bronze(), self.silver_path, KEY, NOW)
        self.batch_counts.append(counts)
        return counts

    def ingest(self, events: list[Ev]) -> CustomerBatchCounts:
        return self.process(self.append(events))

    def state(self) -> dict[tuple[str, str], dict[str, Any]]:
        rows = self.silver().collect()
        out = {(r["brand_id"], r["customer_id"]): r.asDict() for r in rows}
        assert len(out) == len(rows), "Silver holds a duplicate (brand_id, customer_id)"
        return out

    def stable_state(self) -> dict[tuple[str, str], dict[str, Any]]:
        """State without the wall-clock column (excluded from any content comparison, D18)."""
        return {
            k: {c: v for c, v in r.items() if c != "_updated_at"} for k, r in self.state().items()
        }

    def assert_matches_manifest(self) -> None:
        """Per batch, the counters equal the manifest state machine on the same emissions."""
        expected = classify_outcomes(self.entries, self.valid, self.batch_ends)
        start = 0
        for end, counts in zip(self.batch_ends, self.batch_counts, strict=True):
            want = Counter(expected[start:end])
            got = {
                Outcome.ACCEPT: counts.silver_accepted,
                Outcome.NOOP: counts.stale_cdc_noop,
                Outcome.DUPLICATE: counts.duplicates_removed,
                Outcome.DLQ: counts.dlq_excluded,
            }
            assert got == {o: want.get(o, 0) for o in got}, f"batch ending at {end}"
            start = end


def counter_sum_holds(c: CustomerBatchCounts) -> bool:
    """distinct source refs == the five disjoint outcome terms (D18 reconciliation)."""
    distinct = c.rows_read - c.reread_rows
    return distinct == (
        c.parse_excluded
        + c.dlq_excluded
        + c.duplicates_removed
        + c.stale_cdc_noop
        + c.silver_accepted
    )


@pytest.fixture
def lake(spark: SparkSession, tmp_path: Path) -> Lake:
    return Lake(spark, tmp_path)


# ---- 1 lifecycle ------------------------------------------------------------------------------


def test_insert_into_empty_then_newer_update_then_delete_tombstone(lake: Lake) -> None:
    lake.ingest([Ev(ALPHA, C1, 1, "I")])
    row = lake.state()[(ALPHA, C1)]
    assert (row["_cdc_sequence"], row["is_deleted"], row["_cdc_action"]) == (1, False, "I")
    assert row["first_name"] == "Ada"
    assert row["signup_date"] == date(2025, 12, 1)

    lake.ingest([Ev(ALPHA, C1, 2, "U", {"city": "Berlin", "loyalty_tier": "Platinum"})])
    row = lake.state()[(ALPHA, C1)]
    assert (row["_cdc_sequence"], row["city"], row["loyalty_tier"]) == (2, "Berlin", "Platinum")

    # A generated D carries the customer's last state (before-image, as the generator emits it);
    # Silver keeps the non-PII attributes of the D event and nulls the PII.
    lake.ingest([Ev(ALPHA, C1, 3, "D", {"city": "Berlin", "loyalty_tier": "Platinum"})])
    row = lake.state()[(ALPHA, C1)]
    assert (row["_cdc_sequence"], row["is_deleted"], row["_cdc_action"]) == (3, True, "D")
    for pii_or_attr in ("first_name", "last_name", "email_token", "phone_masked", "city"):
        assert row[pii_or_attr] is None, pii_or_attr
    assert (row["country"], row["loyalty_tier"], row["signup_date"]) == (
        "DE",
        "Platinum",
        date(2025, 12, 1),
    )
    assert len(lake.state()) == 1
    lake.assert_matches_manifest()


def test_tombstone_is_inserted_when_delete_arrives_first(lake: Lake) -> None:
    counts = lake.ingest([Ev(ALPHA, C1, 5, "D")])
    row = lake.state()[(ALPHA, C1)]
    assert row["is_deleted"] is True
    assert row["email_token"] is None
    assert counts.silver_accepted == 1


# ---- 2 stale ----------------------------------------------------------------------------------


def test_stale_update_is_ignored(lake: Lake) -> None:
    lake.ingest([Ev(ALPHA, C1, 5, "I", {"city": "Hamburg"})])
    counts = lake.ingest([Ev(ALPHA, C1, 3, "U", {"city": "Stale"})])
    assert lake.state()[(ALPHA, C1)]["city"] == "Hamburg"
    assert lake.state()[(ALPHA, C1)]["_cdc_sequence"] == 5
    assert (counts.stale_cdc_noop, counts.silver_accepted, counts.duplicates_removed) == (1, 0, 0)
    lake.assert_matches_manifest()


# ---- 3 duplicates -----------------------------------------------------------------------------


def test_same_sequence_twice_in_one_batch_counts_one_duplicate(lake: Lake) -> None:
    counts = lake.ingest([Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C1, 1, "I")])
    assert (counts.duplicates_removed, counts.silver_accepted, counts.stale_cdc_noop) == (1, 1, 0)
    assert len(lake.state()) == 1
    lake.assert_matches_manifest()


def test_replay_in_a_later_batch_is_a_duplicate_not_stale(lake: Lake) -> None:
    lake.ingest([Ev(ALPHA, C1, 1, "I")])
    counts = lake.ingest([Ev(ALPHA, C1, 1, "I")])
    assert (counts.duplicates_removed, counts.stale_cdc_noop, counts.silver_accepted) == (1, 0, 0)
    lake.assert_matches_manifest()


def test_replay_of_a_noop_event_counts_duplicate(lake: Lake) -> None:
    lake.ingest([Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C1, 3, "U")])
    stale = lake.ingest([Ev(ALPHA, C1, 2, "U")])
    replay = lake.ingest([Ev(ALPHA, C1, 2, "U")])
    assert (stale.stale_cdc_noop, stale.duplicates_removed) == (1, 0)
    assert (replay.stale_cdc_noop, replay.duplicates_removed) == (0, 1)
    assert lake.state()[(ALPHA, C1)]["_cdc_sequence"] == 3
    lake.assert_matches_manifest()


# ---- 4 in-batch ordering ----------------------------------------------------------------------


def test_insert_and_two_updates_in_one_batch_apply_only_the_max(lake: Lake) -> None:
    counts = lake.ingest(
        [Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C1, 3, "U", {"city": "Three"}), Ev(ALPHA, C1, 2, "U")]
    )
    row = lake.state()[(ALPHA, C1)]
    assert (row["_cdc_sequence"], row["city"]) == (3, "Three")
    assert (counts.silver_accepted, counts.stale_cdc_noop) == (1, 2)
    lake.assert_matches_manifest()


# ---- 5 delete then late update ----------------------------------------------------------------


def test_delete_then_late_update_stays_deleted_across_batches(lake: Lake) -> None:
    lake.ingest([Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C1, 3, "D")])
    counts = lake.ingest([Ev(ALPHA, C1, 2, "U", {"city": "Resurrect"})])
    row = lake.state()[(ALPHA, C1)]
    assert (row["is_deleted"], row["_cdc_sequence"], row["city"]) == (True, 3, None)
    assert counts.stale_cdc_noop == 1
    lake.assert_matches_manifest()


def test_delete_then_late_update_stays_deleted_within_one_batch(lake: Lake) -> None:
    lake.ingest([Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C1, 3, "D"), Ev(ALPHA, C1, 2, "U")])
    row = lake.state()[(ALPHA, C1)]
    assert (row["is_deleted"], row["_cdc_sequence"]) == (True, 3)
    lake.assert_matches_manifest()


def test_update_with_higher_sequence_after_delete_resurrects_the_row(lake: Lake) -> None:
    """Pinned behaviour: sequence order wins, so a later U clears the tombstone."""
    lake.ingest([Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C1, 3, "D")])
    counts = lake.ingest([Ev(ALPHA, C1, 4, "U", {"city": "Back"})])
    row = lake.state()[(ALPHA, C1)]
    assert (row["is_deleted"], row["_cdc_sequence"], row["city"]) == (False, 4, "Back")
    assert row["first_name"] == "Ada"
    assert row["email_token"] == _hmac_hex(KEY, "ada@example.com")
    assert (counts.silver_accepted, counts.stale_cdc_noop) == (1, 0)
    lake.assert_matches_manifest()


def test_delete_with_sequence_tie_against_the_target_is_a_noop(lake: Lake, tmp_path: Path) -> None:
    """A tie never applies. Within one Bronze table a tie is a duplicate full key, so the target
    is fed from a separate Bronze here; the MERGE guard (``>``) is what is being pinned."""
    seed = Lake(lake.spark, tmp_path / "seed")
    process_batch(
        lake.spark,
        seed.append([Ev(ALPHA, C1, 5, "I", {"city": "Kept"})]),
        seed.bronze(),
        lake.silver_path,
        KEY,
        NOW,
    )
    before = lake.stable_state()
    counts = lake.ingest([Ev(ALPHA, C1, 5, "D")])
    assert (counts.stale_cdc_noop, counts.silver_accepted, counts.duplicates_removed) == (1, 0, 0)
    assert lake.stable_state() == before
    assert before[(ALPHA, C1)]["is_deleted"] is False
    assert counter_sum_holds(counts)


def test_batch_classified_while_bronze_already_holds_later_rows_of_the_key(lake: Lake) -> None:
    """Classification looks at valid Bronze history only: later Bronze rows (a higher sequence, a
    replay at a higher offset, an invalid row) change nothing about the earlier batches."""
    b1 = lake.append([Ev(ALPHA, C1, 1, "I")])
    b2 = lake.append([Ev(ALPHA, C1, 3, "U", {"city": "Three"})])
    b3 = lake.append([Ev(ALPHA, C1, 1, "I")])  # replay of b1, higher Kafka offset
    b4 = lake.append([Ev(ALPHA, C1, 9, "U", BAD_EMAIL, bad=True)])
    first = lake.process(b1)
    assert (first.silver_accepted, first.duplicates_removed, first.stale_cdc_noop) == (1, 0, 0)
    second = lake.process(b2)
    assert (second.silver_accepted, second.duplicates_removed) == (1, 0)
    third = lake.process(b3)
    assert (third.duplicates_removed, third.silver_accepted, third.stale_cdc_noop) == (1, 0, 0)
    fourth = lake.process(b4)
    assert (fourth.dlq_excluded, fourth.silver_accepted) == (1, 0)
    row = lake.state()[(ALPHA, C1)]
    assert (row["_cdc_sequence"], row["city"]) == (3, "Three")
    lake.assert_matches_manifest()


# ---- 6 tenant isolation -----------------------------------------------------------------------


def test_same_customer_id_in_two_brands_yields_two_rows(lake: Lake) -> None:
    counts = lake.ingest(
        [
            Ev(ALPHA, C1, 1, "I", {"city": "Alpha"}),
            Ev(BETA, C1, 1, "I", {"city": "Beta"}),
            Ev(BETA, C1, 2, "U", {"city": "BetaNew"}),
        ]
    )
    state = lake.state()
    assert set(state) == {(ALPHA, C1), (BETA, C1)}
    assert state[(ALPHA, C1)]["city"] == "Alpha"
    assert (state[(BETA, C1)]["city"], state[(BETA, C1)]["_cdc_sequence"]) == ("BetaNew", 2)
    # Equal sequence in two brands is not a duplicate: only the full key (with brand_id) counts.
    assert (counts.duplicates_removed, counts.silver_accepted, counts.stale_cdc_noop) == (0, 2, 1)
    lake.assert_matches_manifest()


def test_delete_in_one_brand_does_not_touch_the_other(lake: Lake) -> None:
    lake.ingest([Ev(ALPHA, C1, 1, "I"), Ev(BETA, C1, 1, "I")])
    lake.ingest([Ev(ALPHA, C1, 2, "D")])
    state = lake.state()
    assert state[(ALPHA, C1)]["is_deleted"] is True
    assert state[(BETA, C1)]["is_deleted"] is False
    assert state[(BETA, C1)]["first_name"] == "Ada"


# ---- 7 null keys and multiple-match guard -----------------------------------------------------


def test_null_brand_or_customer_key_is_dlq_excluded_not_merged(lake: Lake) -> None:
    counts = lake.ingest(
        [
            Ev(ALPHA, C1, 1, "I", {"brand_id": None}, bad=True),
            Ev(ALPHA, C2, 1, "I", {"customer_id": None}, bad=True),
        ]
    )
    assert counts.dlq_excluded == 2
    assert counts.silver_accepted == 0
    assert lake.silver().count() == 0
    lake.assert_matches_manifest()


def test_forced_duplicate_source_key_raises_multiple_match_error(lake: Lake) -> None:
    batch = lake.append([Ev(ALPHA, C1, 1, "I")])
    source = merge_source(classify_batch(batch, lake.bronze()), lake.spark, KEY)
    assert source.count() == 1
    with pytest.raises(MultipleMatchError):
        merge_customers(lake.spark, lake.silver_path, source.union(source))
    assert lake.silver().count() == 0  # the guard fires before any write


# ---- 8 parse errors ---------------------------------------------------------------------------


def test_parse_error_row_is_excluded_and_counted(lake: Lake) -> None:
    counts = lake.ingest([Ev(ALPHA, C1, 1, "I", raw=b"{not json"), Ev(ALPHA, C2, 1, "I")])
    assert (counts.parse_excluded, counts.silver_accepted, counts.dlq_excluded) == (1, 1, 0)
    assert set(lake.state()) == {(ALPHA, C2)}
    assert counter_sum_holds(counts)


def test_null_value_tombstone_is_parse_excluded(lake: Lake) -> None:
    frame = lake.spark.createDataFrame(
        [(f"{ALPHA}:{C1}".encode(), None, "customer_cdc", 0, 0, KAFKA_TS, None)], KAFKA_SCHEMA
    )
    bronze = to_bronze_rows(frame, CUSTOMERS, "b-null")
    bronze.write.format("delta").mode("append").save(lake.bronze_path)
    counts = process_batch(lake.spark, lake.bronze(), lake.bronze(), lake.silver_path, KEY, NOW)
    assert (counts.parse_excluded, counts.silver_accepted) == (1, 0)
    assert lake.silver().count() == 0


# ---- 9 and 10 DLQ interaction (D-022) ---------------------------------------------------------


def test_dlq_insert_then_valid_update_inserts_the_row(lake: Lake) -> None:
    first = lake.ingest([Ev(ALPHA, C1, 1, "I", BAD_EMAIL, bad=True)])
    assert (first.dlq_excluded, first.silver_accepted) == (1, 0)
    assert lake.silver().count() == 0
    lake.ingest([Ev(ALPHA, C1, 2, "U", {"city": "Berlin"})])
    row = lake.state()[(ALPHA, C1)]
    assert (row["_cdc_sequence"], row["city"]) == (2, "Berlin")
    lake.assert_matches_manifest()


def test_invalid_high_sequence_row_does_not_shadow_valid_lower_one(lake: Lake) -> None:
    counts = lake.ingest(
        [Ev(ALPHA, C1, 2, "I", {"city": "Valid"}), Ev(ALPHA, C1, 5, "U", BAD_EMAIL, bad=True)]
    )
    row = lake.state()[(ALPHA, C1)]
    assert (row["_cdc_sequence"], row["city"]) == (2, "Valid")
    assert (counts.dlq_excluded, counts.silver_accepted, counts.stale_cdc_noop) == (1, 1, 0)
    lake.assert_matches_manifest()


def test_invalid_row_in_a_later_batch_does_not_change_state(lake: Lake) -> None:
    lake.ingest([Ev(ALPHA, C1, 2, "I")])
    lake.ingest([Ev(ALPHA, C1, 9, "U", BAD_EMAIL, bad=True)])
    assert lake.state()[(ALPHA, C1)]["_cdc_sequence"] == 2
    lake.assert_matches_manifest()


# ---- 11 re-read -------------------------------------------------------------------------------


def test_reread_source_ref_is_not_a_duplicate(lake: Lake) -> None:
    batch = lake.append([Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C2, 1, "I")])
    doubled = batch.union(batch)
    counts = process_batch(lake.spark, doubled, lake.bronze(), lake.silver_path, KEY, NOW)
    assert counts.rows_read == 4
    assert counts.reread_rows == 2
    assert counts.duplicates_removed == 0
    assert counts.silver_accepted == 2
    assert counter_sum_holds(counts)
    labels = classify_batch(doubled, lake.bronze()).groupBy("_outcome").count().collect()
    assert {r["_outcome"]: r["count"] for r in labels} == {"accepted": 2, "reread": 2}


def test_reread_of_the_same_kafka_offset_in_a_later_batch_is_counted_as_reread(lake: Lake) -> None:
    """D-057: the same ``_source_ref`` re-ingested later (checkpoint loss) is a re-read, not a
    stale noop, and changes neither the state nor any other counter."""
    first = lake.ingest([Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C1, 2, "U", {"city": "Two"})])
    assert (first.silver_accepted, first.stale_cdc_noop) == (1, 1)
    before = lake.stable_state()

    original = lake.bronze().where(F.col("_batch_id") == "batch-1")
    again = (
        original.withColumn("_batch_id", F.lit("batch-2"))
        .withColumn("_ingest_ts", F.col("_ingest_ts") + F.expr("INTERVAL 1 SECOND"))
        .where(F.col("_source_ref").endswith("/1"))  # offset 1 = the seq-2 update only
    )
    again.write.format("delta").mode("append").save(lake.bronze_path)
    batch = lake.bronze().where(F.col("_batch_id") == "batch-2")
    second = process_batch(lake.spark, batch, lake.bronze(), lake.silver_path, KEY, NOW)

    assert second.rows_read == 1
    assert (second.reread_rows, second.stale_cdc_noop, second.duplicates_removed) == (1, 0, 0)
    assert (second.silver_accepted, second.parse_excluded, second.dlq_excluded) == (0, 0, 0)
    assert counter_sum_holds(second)
    assert lake.stable_state() == before
    labels = classify_batch(batch, lake.bronze()).select("_outcome").distinct().collect()
    assert [r["_outcome"] for r in labels] == ["reread"]


def test_process_batch_destroys_the_key_broadcast(lake: Lake) -> None:
    """The HMAC key broadcast must not outlive the batch."""
    context: Any = lake.spark.sparkContext
    real = context.broadcast
    created: list[Any] = []
    destroyed: list[Any] = []

    def spy(value: Any) -> Any:
        handle = real(value)
        real_destroy = handle.destroy

        def destroy(*args: Any, **kwargs: Any) -> None:
            destroyed.append(handle)
            real_destroy(*args, **kwargs)

        handle.destroy = destroy
        created.append(handle)
        return handle

    context.broadcast = spy
    try:
        lake.ingest([Ev(ALPHA, C1, 1, "I")])
    finally:
        del context.broadcast  # drop the instance override, back to the class method
    assert len(created) == 1
    assert destroyed == created


# ---- 12 idempotent re-run ---------------------------------------------------------------------


def test_rerun_of_same_batch_leaves_state_unchanged(lake: Lake) -> None:
    batch = lake.append(
        [Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C1, 2, "U"), Ev(BETA, C2, 1, "I"), Ev(BETA, C2, 2, "D")]
    )
    first = process_batch(lake.spark, batch, lake.bronze(), lake.silver_path, KEY, NOW)
    before = lake.stable_state()
    assert first.silver_accepted == 2

    second = process_batch(lake.spark, batch, lake.bronze(), lake.silver_path, KEY, NOW)
    assert lake.stable_state() == before
    assert second.silver_accepted == 0
    assert counter_sum_holds(second)


def test_merge_customers_on_replayed_source_inserts_and_updates_nothing(lake: Lake) -> None:
    batch = lake.append([Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C2, 4, "I")])
    source = merge_source(classify_batch(batch, lake.bronze()), lake.spark, KEY)
    assert merge_customers(lake.spark, lake.silver_path, source) == (2, 0)
    assert merge_customers(lake.spark, lake.silver_path, source) == (0, 0)
    assert lake.silver().count() == 2


# ---- 13 PII -----------------------------------------------------------------------------------


def _hmac_hex(key: bytes, text: str) -> str:
    return hmac.new(key, text.encode(), hashlib.sha256).hexdigest()


def test_silver_email_token_is_hmac_of_normalised_email(lake: Lake) -> None:
    lake.ingest([Ev(ALPHA, C1, 1, "I", {"email": "Ada@Example.com"})])
    row = lake.state()[(ALPHA, C1)]
    assert row["email_token"] == _hmac_hex(KEY, "ada@example.com")
    assert len(row["email_token"]) == 64
    assert "ada@example.com" not in json.dumps(row, default=str).lower()


def test_email_token_column_known_vector_trim_lower_null_and_key_dependence(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame(
        [(1, "  Ada@Example.COM "), (2, "ada@example.com"), (3, None)], "id int, e string"
    )
    rows = {
        r["id"]: r["t"]
        for r in df.select("id", email_token_column(spark, KEY, F.col("e")).alias("t")).collect()
    }
    expected = _hmac_hex(KEY, "ada@example.com")
    assert rows[1] == rows[2] == expected
    assert rows[3] is None
    other = df.select("id", email_token_column(spark, b"other-key-" * 4, F.col("e")).alias("t"))
    assert {r["id"]: r["t"] for r in other.collect()}[2] != expected


def test_phone_masked_is_last_four_and_null_safe(lake: Lake) -> None:
    lake.ingest(
        [Ev(ALPHA, C1, 1, "I", {"phone": "+447700900123"}), Ev(ALPHA, C2, 1, "I", {"phone": None})]
    )
    state = lake.state()
    assert state[(ALPHA, C1)]["phone_masked"] == "0123"
    assert state[(ALPHA, C2)]["phone_masked"] is None


# ---- 14 key hygiene ---------------------------------------------------------------------------


def _conf_values(spark: SparkSession) -> list[str]:
    values = [v for _, v in spark.sparkContext.getConf().getAll()]
    values += [r["value"] or "" for r in spark.sql("SET").collect()]
    return values


def test_pii_key_is_absent_from_spark_conf_after_a_run_and_the_scan_detects_a_leak(
    lake: Lake,
) -> None:
    lake.ingest([Ev(ALPHA, C1, 1, "I")])
    secret = KEY.decode()
    assert not any(secret in v for v in _conf_values(lake.spark))
    # Negative control: the same scan does notice a planted value.
    lake.spark.conf.set("test.leak.probe", secret)
    try:
        assert any(secret in v for v in _conf_values(lake.spark))
    finally:
        lake.spark.conf.unset("test.leak.probe")


# ---- 15 counter-sum invariant -----------------------------------------------------------------


def test_counter_sum_invariant_over_a_mixed_scenario(lake: Lake) -> None:
    b1 = lake.ingest(
        [
            Ev(ALPHA, C1, 1, "I"),
            Ev(ALPHA, C1, 2, "U"),
            Ev(ALPHA, C1, 2, "U"),  # in-batch duplicate
            Ev(ALPHA, C2, 1, "I", BAD_EMAIL, bad=True),
            Ev(BETA, C1, 1, "I"),
            Ev(BETA, C2, 1, "I", raw=b"garbage"),
        ]
    )
    assert (b1.parse_excluded, b1.dlq_excluded, b1.duplicates_removed) == (1, 1, 1)
    assert (b1.stale_cdc_noop, b1.silver_accepted, b1.late_beyond_window) == (1, 2, 0)
    assert b1.rows_read == 6
    b2 = lake.ingest([Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C1, 2, "U"), Ev(BETA, C1, 3, "U")])
    assert (b2.duplicates_removed, b2.stale_cdc_noop, b2.silver_accepted) == (2, 0, 1)
    for counts in lake.batch_counts:
        assert counter_sum_holds(counts)
        assert counts.late_beyond_window == 0


def test_counter_sum_detector_fails_on_a_tampered_counter(lake: Lake) -> None:
    counts = lake.ingest([Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C2, 1, "I", BAD_EMAIL, bad=True)])
    assert counter_sum_holds(counts)
    assert not counter_sum_holds(dataclasses.replace(counts, silver_accepted=2))
    assert not counter_sum_holds(dataclasses.replace(counts, dlq_excluded=0))


def test_manifest_agreement_on_a_multi_batch_scenario(lake: Lake) -> None:
    lake.ingest([Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C1, 4, "U"), Ev(BETA, C1, 1, "I")])
    lake.ingest([Ev(ALPHA, C1, 3, "U"), Ev(ALPHA, C1, 5, "U", BAD_EMAIL, bad=True)])
    lake.ingest([Ev(ALPHA, C1, 3, "U"), Ev(ALPHA, C1, 6, "D"), Ev(BETA, C1, 2, "U")])
    lake.ingest([Ev(ALPHA, C1, 2, "U"), Ev(BETA, C1, 2, "U")])
    lake.assert_matches_manifest()
    state = lake.state()
    assert (state[(ALPHA, C1)]["_cdc_sequence"], state[(ALPHA, C1)]["is_deleted"]) == (6, True)
    assert state[(BETA, C1)]["_cdc_sequence"] == 2


def test_manifest_comparison_detects_a_wrong_counter(lake: Lake) -> None:
    lake.ingest([Ev(ALPHA, C1, 1, "I"), Ev(ALPHA, C1, 1, "I")])
    lake.batch_counts[-1] = dataclasses.replace(lake.batch_counts[-1], duplicates_removed=0)
    with pytest.raises(AssertionError):
        lake.assert_matches_manifest()


# ---- schema -----------------------------------------------------------------------------------


def test_silver_schema_has_contract_columns_and_non_null_keys() -> None:
    schema = silver_customers_schema()
    names = schema.fieldNames()
    for column in (
        "brand_id",
        "customer_id",
        "first_name",
        "last_name",
        "email_token",
        "phone_masked",
        "city",
        "country",
        "loyalty_tier",
        "signup_date",
        "is_deleted",
        "_cdc_action",
        "_cdc_sequence",
        "_cdc_ts",
        "produced_at",
        "event_id",
        "_bronze_ref",
        "_bronze_batch_id",
        "_updated_at",
    ):
        assert column in names, column
    assert "email" not in names
    assert "phone" not in names
    assert not schema["brand_id"].nullable
    assert not schema["customer_id"].nullable
    assert schema["_cdc_sequence"].dataType.simpleString() == "bigint"
    assert schema["signup_date"].dataType.simpleString() == "date"
