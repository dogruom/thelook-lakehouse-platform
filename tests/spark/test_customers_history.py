"""SCD2 ``silver.customers_history`` (W3.D16), derived from DATA_CONTRACTS 4.2 and the task card.

Semantics under test: per batch the affected (brand_id, customer_id) keys are rebuilt from all
valid, first-occurrence Bronze rows ordered by ``_cdc_sequence``; versions are numbered 1..n,
``valid_from`` = the version's ``_cdc_ts``, ``valid_to`` = the next version's ``_cdc_ts`` (NULL on the
last), ``is_current`` on the last; ``D`` is a tombstone version with PII NULL; invalid, parse-error,
duplicate and re-read rows never become versions. The same events in any arrival order produce
identical history (``history_content_hash``).

Frames are built like ``test_silver_customers.py`` (Kafka-shaped rows -> ``to_bronze_rows`` -> tmp
Delta Bronze). Runs inside the Spark image only.
"""

from __future__ import annotations

import hashlib
from itertools import pairwise
from typing import TYPE_CHECKING, Any

import pytest


pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse.silver.customers import ReconciliationError  # noqa: E402
from lakehouse.silver.customers_history import (  # noqa: E402
    ensure_history_table,
    history_content_hash,
    history_schema,
    process_history_batch,
    rebuild_history,
)

from .test_silver_customers import (  # noqa: E402
    ALPHA,
    BAD_EMAIL,
    BETA,
    C1,
    C2,
    KEY,
    NOW,
    Ev,
    Lake,
)


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame, SparkSession

    from lakehouse.silver.customers_history import HistoryBatchCounts

Row = dict[str, Any]
PII = ("first_name", "last_name", "email_token", "phone_masked", "city")
VERSION_COLUMNS = ("version", "version_number", "version_no", "scd_version")


def ev(
    brand: str,
    cust: str,
    seq: int,
    action: str = "I",
    *,
    city: str | None = None,
    minute: int | None = None,
    bad: bool = False,
    **over: Any,
) -> Ev:
    """CDC event with a deterministic ``event_id`` (so two lakes ingesting the same events in a
    different order store identical content) and ``_cdc_ts`` = 10:<minute> (default: the sequence)."""
    minute = seq if minute is None else minute
    n = seq + (0x1000 if brand == BETA else 0)
    payload: dict[str, Any] = {
        "event_id": f"{n:08x}-0000-4000-8000-{cust[-12:]}",
        "_cdc_ts": f"2026-01-01T10:{minute:02d}:00.000Z",
        "city": city if city is not None else f"City{seq}",
        **over,
    }
    if bad:
        payload.update(BAD_EMAIL)
    return Ev(brand, cust, seq, action, payload, bad=bad)


class HLake(Lake):
    """Lake plus a tmp ``customers_history`` table."""

    def __init__(self, spark: SparkSession, root: Path) -> None:
        super().__init__(spark, root)
        self.history_path = (root / "history").as_posix()
        self.hcounts: list[HistoryBatchCounts] = []
        ensure_history_table(spark, self.history_path)

    def history(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.history_path)

    def process_history(self, batch: DataFrame) -> HistoryBatchCounts:
        counts = process_history_batch(
            self.spark, batch, self.bronze(), self.history_path, KEY, NOW
        )
        self.hcounts.append(counts)
        return counts

    def ingest_h(self, events: list[Ev]) -> HistoryBatchCounts:
        return self.process_history(self.append(events))

    def rows(self, brand: str | None = None, cust: str | None = None) -> list[Row]:
        frame = self.history()
        if brand is not None:
            frame = frame.where(F.col("brand_id") == brand)
        if cust is not None:
            frame = frame.where(F.col("customer_id") == cust)
        rows = [r.asDict() for r in frame.collect()]
        return sorted(rows, key=lambda r: (r["brand_id"], r["customer_id"], r["_cdc_sequence"]))

    def stable_rows(self) -> list[Row]:
        # ``_updated_at`` is wall clock; ``_bronze_ref`` is lineage and legitimately differs between
        # in-order and shuffled deliveries (other Kafka offsets). Both stay out of the content.
        drop = {"_updated_at", "_bronze_ref"}
        return [{k: v for k, v in r.items() if k not in drop} for r in self.rows()]

    def hash(self) -> str:
        return history_content_hash(self.history())


@pytest.fixture
def lake(spark: SparkSession, tmp_path: Path) -> HLake:
    return HLake(spark, tmp_path / "a")


@pytest.fixture
def lake2(spark: SparkSession, tmp_path: Path) -> HLake:
    return HLake(spark, tmp_path / "b")


def version_column() -> str | None:
    names = set(history_schema().fieldNames())
    return next((c for c in VERSION_COLUMNS if c in names), None)


def invariant_violations(rows: list[Row]) -> list[str]:
    """SCD2 structural invariants per key; empty list = history is well formed."""
    problems: list[str] = []
    by_key: dict[tuple[str, str], list[Row]] = {}
    for r in rows:
        by_key.setdefault((r["brand_id"], r["customer_id"]), []).append(r)
    vcol = version_column()
    for key, unsorted in by_key.items():
        versions = sorted(unsorted, key=lambda r: r["_cdc_sequence"])
        seqs = [v["_cdc_sequence"] for v in versions]
        if len(set(seqs)) != len(seqs):
            problems.append(f"{key}: duplicate _cdc_sequence {seqs}")
        if vcol and [v[vcol] for v in versions] != list(range(1, len(versions) + 1)):
            problems.append(f"{key}: versions not 1..n: {[v[vcol] for v in versions]}")
        for cur, nxt in pairwise(versions):
            if cur["valid_to"] != nxt["valid_from"]:
                problems.append(f"{key}: valid_to chain broken at seq {cur['_cdc_sequence']}")
            if cur["is_current"]:
                problems.append(f"{key}: non-last version seq {cur['_cdc_sequence']} is current")
        last = versions[-1]
        if not last["is_current"]:
            problems.append(f"{key}: last version is not current")
        if last["valid_to"] is not None:
            problems.append(f"{key}: last version has valid_to")
    return problems


# ---- (a) Istanbul -> Berlin -----------------------------------------------------------------------


@pytest.mark.parametrize("two_batches", [False, True], ids=["one_batch", "two_batches"])
def test_istanbul_to_berlin_closes_first_version_and_leaves_only_second_current(
    lake: HLake, two_batches: bool
) -> None:
    first = ev(ALPHA, C1, 1, "I", city="Istanbul")
    second = ev(ALPHA, C1, 2, "U", city="Berlin")
    if two_batches:
        lake.ingest_h([first])
        counts = lake.ingest_h([second])
    else:
        counts = lake.ingest_h([first, second])
    v1, v2 = lake.rows(ALPHA, C1)
    assert (v1["city"], v2["city"]) == ("Istanbul", "Berlin")
    assert v1["valid_to"] == v2["valid_from"]
    assert v1["valid_from"] < v2["valid_from"]
    assert (v1["is_current"], v2["is_current"]) == (False, True)
    assert v2["valid_to"] is None
    if version_column():
        assert (v1[version_column()], v2[version_column()]) == (1, 2)
    assert counts.affected_keys == 1
    assert counts.versions_written == 2
    assert invariant_violations(lake.rows()) == []


def test_first_batch_counts_report_rows_keys_and_versions(lake: HLake) -> None:
    counts = lake.ingest_h([ev(ALPHA, C1, 1), ev(ALPHA, C1, 2, "U"), ev(ALPHA, C2, 1)])
    assert counts.rows_read == 3
    assert counts.affected_keys == 2
    assert counts.versions_written == 3
    assert counts.versions_deleted == 0


# ---- (b) shuffled delivery = identical hash --------------------------------------------------------


def _scenario() -> list[Ev]:
    return [
        ev(ALPHA, C1, 1, "I", city="Istanbul"),
        ev(ALPHA, C1, 2, "U", city="Berlin"),
        ev(ALPHA, C1, 3, "U", city="Hamburg", loyalty_tier="Platinum"),
        ev(ALPHA, C1, 4, "D"),
        ev(ALPHA, C2, 1, "I"),
        ev(ALPHA, C2, 2, "U", country="FR"),
        ev(BETA, C1, 1, "I", city="Paris"),
        ev(BETA, C1, 2, "U", city="Lyon"),
    ]


def test_shuffled_delivery_across_batches_gives_identical_history_hash(
    lake: HLake, lake2: HLake
) -> None:
    events = _scenario()
    lake.ingest_h(events)  # in order, one batch

    reversed_events = list(reversed(events))  # worst case: every key arrives newest first
    third = len(reversed_events) // 3
    for chunk in (
        reversed_events[:third],
        reversed_events[third : 2 * third],
        reversed_events[2 * third :],
    ):
        lake2.ingest_h(chunk)

    assert len(lake2.hcounts) == 3
    assert lake.hash() == lake2.hash()
    assert lake.stable_rows() == lake2.stable_rows()
    assert invariant_violations(lake2.rows()) == []
    assert len(lake.rows()) == len(events)  # every valid distinct event is one version


def test_hash_detects_a_different_history(lake: HLake, lake2: HLake) -> None:
    lake.ingest_h(_scenario())
    lake2.ingest_h(
        [e for e in _scenario() if not (e.cust == C1 and e.brand == BETA and e.seq == 2)]
    )
    assert lake.hash() != lake2.hash()


# ---- (c) equal _cdc_ts: sequence decides ------------------------------------------------------------


def test_equal_cdc_ts_versions_are_ordered_by_sequence_not_timestamp(lake: HLake) -> None:
    same = {"minute": 30}
    lake.ingest_h([ev(ALPHA, C1, 3, "U", **same), ev(ALPHA, C1, 1, "I", **same)])
    lake.ingest_h([ev(ALPHA, C1, 2, "U", **same)])
    rows = lake.rows(ALPHA, C1)
    assert [r["city"] for r in rows] == ["City1", "City2", "City3"]
    assert len({r["valid_from"] for r in rows}) == 1
    assert [r["is_current"] for r in rows] == [False, False, True]
    assert rows[-1]["valid_to"] is None
    assert all(r["valid_to"] == rows[0]["valid_from"] for r in rows[:-1])
    if version_column():
        assert [r[version_column()] for r in rows] == [1, 2, 3]


# ---- (d) re-run is idempotent ------------------------------------------------------------------------


def test_rerunning_the_same_batch_adds_nothing(lake: HLake) -> None:
    batch = lake.append([ev(ALPHA, C1, 1, "I"), ev(ALPHA, C1, 2, "U"), ev(ALPHA, C2, 1, "I")])
    first = lake.process_history(batch)
    hash_before, count_before = lake.hash(), len(lake.rows())
    assert (first.versions_written, first.versions_deleted) == (3, 0)

    again = lake.process_history(batch)
    assert lake.hash() == hash_before
    assert len(lake.rows()) == count_before == 3
    assert again.rows_read == 3
    assert again.affected_keys == 2
    # An identical rebuild replaces the same rows (or skips them); it never grows the table.
    assert again.versions_written == again.versions_deleted <= count_before
    assert invariant_violations(lake.rows()) == []


def test_reprocessing_an_old_batch_after_newer_ones_keeps_the_final_history(lake: HLake) -> None:
    old = lake.append([ev(ALPHA, C1, 1, "I")])
    lake.ingest_h([ev(ALPHA, C1, 2, "U", city="Berlin")])
    before = lake.hash()
    lake.process_history(old)  # replay of an already applied, now stale batch
    assert lake.hash() == before
    assert [r["city"] for r in lake.rows(ALPHA, C1)] == ["City1", "Berlin"]


# ---- (e) tombstone and resurrection -------------------------------------------------------------------


def test_delete_adds_a_tombstone_version_with_pii_nulled(lake: HLake) -> None:
    lake.ingest_h([ev(ALPHA, C1, 1, "I", city="Istanbul"), ev(ALPHA, C1, 2, "D", city="Istanbul")])
    v1, v2 = lake.rows(ALPHA, C1)
    assert all(v1[c] is not None for c in PII)
    assert all(v2[c] is None for c in PII), "tombstone version must not carry PII"
    assert (v1["is_current"], v2["is_current"]) == (False, True)
    assert v1["valid_to"] == v2["valid_from"]
    if "is_deleted" in v2:
        assert (v1["is_deleted"], v2["is_deleted"]) == (False, True)


def test_resurrection_after_delete_opens_a_new_live_version(lake: HLake) -> None:
    lake.ingest_h([ev(ALPHA, C1, 1, "I"), ev(ALPHA, C1, 2, "D")])
    lake.ingest_h([ev(ALPHA, C1, 3, "U", city="Berlin")])
    v1, v2, v3 = lake.rows(ALPHA, C1)
    assert all(v2[c] is None for c in PII), "tombstone stays a tombstone"
    assert v3["city"] == "Berlin"
    assert v3["email_token"] is not None and v3["first_name"] is not None
    assert (v1["is_current"], v2["is_current"], v3["is_current"]) == (False, False, True)
    assert (v2["valid_to"], v3["valid_to"]) == (v3["valid_from"], None)
    assert invariant_violations(lake.rows()) == []


# ---- (f) intermediate late event ----------------------------------------------------------------------


def test_intermediate_late_event_inserts_the_missing_version_and_fixes_the_chain(
    lake: HLake,
) -> None:
    lake.ingest_h([ev(ALPHA, C1, 1, "I"), ev(ALPHA, C1, 3, "U", city="Hamburg")])
    before = lake.rows(ALPHA, C1)
    assert len(before) == 2 and before[0]["valid_to"] == before[1]["valid_from"]

    counts = lake.ingest_h([ev(ALPHA, C1, 2, "U", city="Berlin")])
    rows = lake.rows(ALPHA, C1)
    assert [r["city"] for r in rows] == ["City1", "Berlin", "Hamburg"]
    assert rows[0]["valid_to"] == rows[1]["valid_from"]
    assert rows[1]["valid_to"] == rows[2]["valid_from"]
    assert [r["is_current"] for r in rows] == [False, False, True]
    assert rows[2]["valid_to"] is None
    if version_column():
        assert [r[version_column()] for r in rows] == [1, 2, 3]
    assert counts.affected_keys == 1 and counts.versions_written == 3
    assert invariant_violations(lake.rows()) == []


# ---- (g) invalid / parse-error / duplicate rows are never versions -------------------------------------


def test_invalid_high_sequence_row_never_becomes_a_version(lake: HLake) -> None:
    lake.ingest_h(
        [
            ev(ALPHA, C1, 1, "I"),
            ev(ALPHA, C1, 9, "U", city="BadCity", bad=True),  # contract failure -> DLQ
            ev(ALPHA, C2, 5, "I", city="OnlyBad", bad=True),  # key with only invalid events
            Ev(ALPHA, C1, 0, raw=b"{not json"),  # parse error stays in Bronze
        ]
    )
    rows = lake.rows()
    assert [(r["customer_id"], r["_cdc_sequence"]) for r in rows] == [(C1, 1)]
    assert rows[0]["is_current"] is True
    lake.ingest_h([ev(ALPHA, C1, 2, "U")])  # a later rebuild must still exclude the bad row
    assert [r["_cdc_sequence"] for r in lake.rows(ALPHA, C1)] == [1, 2]
    assert not any(r["city"] == "BadCity" for r in lake.rows())
    assert lake.rows(ALPHA, C2) == []


def test_duplicates_in_batch_and_across_batches_never_create_extra_versions(lake: HLake) -> None:
    lake.ingest_h(
        [
            ev(ALPHA, C1, 1, "I"),
            ev(ALPHA, C1, 2, "U", city="Berlin"),
            ev(ALPHA, C1, 2, "U", city="Berlin"),
        ]
    )
    assert [r["city"] for r in lake.rows(ALPHA, C1)] == ["City1", "Berlin"]
    # later re-send of seq 2 with different content: the first occurrence wins, nothing is added
    lake.ingest_h([ev(ALPHA, C1, 2, "U", city="DupCity")])
    rows = lake.rows(ALPHA, C1)
    assert [r["city"] for r in rows] == ["City1", "Berlin"]
    assert rows[-1]["is_current"] and invariant_violations(lake.rows()) == []


def test_bronze_reread_of_a_source_ref_does_not_double_a_version(
    spark: SparkSession, lake: HLake
) -> None:
    batch = lake.append([ev(ALPHA, C1, 1, "I"), ev(ALPHA, C1, 2, "U")])
    lake.process_history(batch)
    # Re-read: the very same Bronze rows (same _source_ref) appended again, as after a rewound
    # checkpoint (D-057).
    lake.bronze().write.format("delta").mode("append").save(lake.bronze_path)
    lake.process_history(batch)
    assert len(lake.rows(ALPHA, C1)) == 2


# ---- (h) tenant isolation ------------------------------------------------------------------------------


def test_same_customer_id_in_two_brands_has_independent_histories(lake: HLake) -> None:
    lake.ingest_h(
        [
            ev(ALPHA, C1, 1, "I", city="Istanbul"),
            ev(BETA, C1, 1, "I", city="Paris"),
            ev(BETA, C1, 2, "D"),
        ]
    )
    beta_before = [r for r in lake.stable_rows() if r["brand_id"] == BETA]
    counts = lake.ingest_h([ev(ALPHA, C1, 2, "U", city="Berlin")])
    assert counts.affected_keys == 1
    assert [r for r in lake.stable_rows() if r["brand_id"] == BETA] == beta_before
    alpha, beta = lake.rows(ALPHA, C1), lake.rows(BETA, C1)
    assert [r["city"] for r in alpha] == ["Istanbul", "Berlin"]
    assert len(beta) == 2 and beta[1]["city"] is None  # beta's tombstone is not alpha's
    for brand_rows in (alpha, beta):
        assert sum(r["is_current"] for r in brand_rows) == 1
    assert invariant_violations(lake.rows()) == []


# ---- (i) hash contract (+ negative proof) --------------------------------------------------------------


def _naive_hash(df: DataFrame) -> str:
    """What a careless implementation would do: row order and ``_updated_at`` leak in."""
    text = "\n".join(str(tuple(r)) for r in df.collect())
    return hashlib.sha256(text.encode()).hexdigest()


def test_content_hash_ignores_updated_at_and_row_order_but_a_naive_hash_does_not(
    lake: HLake,
) -> None:
    lake.ingest_h(_scenario())
    base = lake.history()
    touched = base.withColumn("_updated_at", F.current_timestamp() + F.expr("INTERVAL 7 DAYS"))
    reordered = base.orderBy(F.col("brand_id").desc(), F.col("_cdc_sequence").desc()).repartition(3)

    assert history_content_hash(touched) == history_content_hash(base)
    assert history_content_hash(reordered) == history_content_hash(base)
    # negative: the naive reference hash is sensitive to both, so the assertions above can fail
    assert _naive_hash(touched) != _naive_hash(base)
    assert _naive_hash(reordered) != _naive_hash(base)


def test_content_hash_changes_with_any_content_column(lake: HLake) -> None:
    lake.ingest_h(_scenario())
    base = lake.history()
    expected = history_content_hash(base)
    for column, value in (
        ("city", F.lit("Elsewhere")),
        ("valid_to", F.lit(None).cast("timestamp")),
        ("is_current", ~F.col("is_current")),
        ("brand_id", F.lit(BETA)),
    ):
        assert history_content_hash(base.withColumn(column, value)) != expected, column


# ---- table / schema / rebuild ---------------------------------------------------------------------------


def test_ensure_history_table_is_idempotent_and_matches_history_schema(
    spark: SparkSession, lake: HLake
) -> None:
    lake.ingest_h([ev(ALPHA, C1, 1)])
    ensure_history_table(spark, lake.history_path)  # must not wipe or fail
    assert len(lake.rows()) == 1
    assert lake.history().schema.fieldNames() == history_schema().fieldNames()
    assert {"brand_id", "customer_id", "valid_from", "valid_to", "is_current"} <= set(
        history_schema().fieldNames()
    )


def test_rebuild_history_only_touches_the_requested_keys(spark: SparkSession, lake: HLake) -> None:
    lake.append([ev(ALPHA, C1, 1), ev(ALPHA, C1, 2, "U"), ev(ALPHA, C2, 1), ev(BETA, C1, 1)])
    keys = spark.createDataFrame([(ALPHA, C1)], "brand_id string, customer_id string")
    out = rebuild_history(spark, lake.bronze(), keys, KEY).collect()
    assert {(r["brand_id"], r["customer_id"]) for r in out} == {(ALPHA, C1)}
    assert len(out) == 2
    assert sorted(r["_cdc_sequence"] for r in out) == [1, 2]


def test_invariant_checker_detects_each_kind_of_violation() -> None:
    def row(seq: int, vf: int, vt: int | None, cur: bool, brand: str = ALPHA) -> Row:
        r: Row = {
            "brand_id": brand,
            "customer_id": C1,
            "_cdc_sequence": seq,
            "valid_from": vf,
            "valid_to": vt,
            "is_current": cur,
        }
        if version_column():
            r[version_column()] = seq
        return r

    good = [row(1, 1, 2, False), row(2, 2, None, True)]
    assert invariant_violations(good) == []
    assert invariant_violations([row(1, 1, 5, False), row(2, 2, None, True)])  # broken chain
    assert invariant_violations([row(1, 1, 2, True), row(2, 2, None, True)])  # two current
    assert invariant_violations([row(1, 1, 2, False), row(2, 2, None, False)])  # none current
    assert invariant_violations([row(1, 1, 2, False), row(2, 2, 3, True)])  # open end closed


# ---- reconciliation and shrinking keys ----------------------------------------------------------------


def test_history_merge_metrics_that_disagree_with_the_plan_raise_reconciliation_error(
    lake: HLake, monkeypatch: pytest.MonkeyPatch
) -> None:
    from delta.tables import DeltaTable

    batch = lake.append([ev(ALPHA, C1, 1, "I"), ev(ALPHA, C1, 2, "U")])
    real_history = DeltaTable.history

    def lying_history(self: DeltaTable, limit: int | None = None) -> DataFrame:
        frame = real_history(self, limit)
        if limit != 2:  # the post-MERGE metrics lookup only
            return frame
        wrong = F.create_map(
            F.lit("numTargetRowsInserted"),
            F.lit("99"),
            F.lit("numTargetRowsUpdated"),
            F.lit("0"),
            F.lit("numTargetRowsDeleted"),
            F.lit("0"),
        )
        return frame.withColumn("operationMetrics", wrong)

    with monkeypatch.context() as patch:
        patch.setattr(DeltaTable, "history", lying_history)
        with pytest.raises(ReconciliationError, match="planned 2 written"):
            lake.process_history(batch)


def test_key_whose_rebuild_shrinks_deletes_the_stale_versions(lake: HLake) -> None:
    batch = lake.append([ev(ALPHA, C1, 1, "I"), ev(ALPHA, C1, 2, "U", city="Berlin")])
    lake.process_history(batch)
    good = lake.hash()
    # A stale third version (e.g. left by an earlier, wider rebuild) that Bronze no longer backs.
    (
        lake.history()
        .where(F.col("version") == 2)
        .withColumn("version", F.lit(3))
        .withColumn("_cdc_sequence", F.lit(3).cast("long"))
        .write.format("delta")
        .mode("append")
        .save(lake.history_path)
    )
    assert len(lake.rows(ALPHA, C1)) == 3

    counts = lake.process_history(batch)
    assert (counts.versions_written, counts.versions_deleted) == (0, 1)
    assert len(lake.rows(ALPHA, C1)) == 2
    assert lake.hash() == good
    assert invariant_violations(lake.rows()) == []
