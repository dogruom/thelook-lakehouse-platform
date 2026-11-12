"""quality.dlq: constants, ``dlq_rows`` projection wiring and MERGE counters (stubbed Spark/Delta)."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from lakehouse.common.tables import CATALOG_TABLES
from lakehouse.quality import dlq
from tests.unit.quality import fake_spark
from tests.unit.quality.fake_spark import Perm


def test_status_vocabulary_is_the_documented_one() -> None:
    assert (dlq.PENDING, dlq.REPLAYED, dlq.FAILED_AGAIN, dlq.DISCARDED) == (
        "PENDING",
        "REPLAYED",
        "FAILED_AGAIN",
        "DISCARDED",
    )


def test_every_bronze_table_maps_to_an_existing_dlq_table() -> None:
    assert set(dlq.DLQ_TABLE_BY_BRONZE) == {
        "raw_customers",
        "raw_clickstream",
        "raw_orders",
        "raw_products",
    }
    for bronze, dead in dlq.DLQ_TABLE_BY_BRONZE.items():
        assert bronze in CATALOG_TABLES
        assert CATALOG_TABLES[dead].layer == "dead_letter"
        assert dead == bronze.replace("raw_", "dlq_")


def test_merge_key_is_tenant_scoped_and_null_safe() -> None:
    # brand_id is part of every key; <=> keeps a NULL brand from matching a different row.
    assert "t.brand_id <=> s.brand_id" in dlq._KEY
    assert "t._dlq_id = s._dlq_id" in dlq._KEY


class Checked(Perm):
    """A DataFrame stand-in with a ``columns`` list."""

    def __init__(self, columns: list[str]) -> None:
        super().__init__("checked")
        self.columns = columns


@pytest.mark.parametrize("columns", [["brand_id", "_source_ref"], ["brand_id", "event_id"]])
def test_dlq_rows_builds_with_and_without_an_event_id_column(
    monkeypatch: pytest.MonkeyPatch, columns: list[str]
) -> None:
    fake_spark.install(monkeypatch)
    assert dlq.dlq_rows(Checked(columns), "bronze.raw_orders", "run1") is not None  # type: ignore[arg-type]


# ---- merges -------------------------------------------------------------------------------------


class _History:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def where(self, *_a: Any) -> _History:
        return self

    def select(self, *_a: Any) -> _History:
        return self

    def collect(self) -> list[dict[str, Any]]:
        return self.rows


class _Merge:
    def __init__(self, table: FakeTable) -> None:
        self.table = table

    def merge(self, _source: Any, condition: str) -> _Merge:
        self.table.merge_condition = condition
        return self

    def whenNotMatchedInsertAll(self) -> _Merge:
        self.table.inserted_all = True
        return self

    def whenMatchedUpdate(self, condition: str, set: dict[str, str]) -> _Merge:
        self.table.update = (condition, set)
        return self

    def execute(self) -> None:
        self.table.executed = True


class FakeTable:
    is_delta = True
    version = 4
    history_rows: ClassVar[list[dict[str, Any]]] = []
    instance: FakeTable

    def __init__(self) -> None:
        self.merge_condition = ""
        self.inserted_all = False
        self.update: tuple[str, dict[str, str]] | None = None
        self.executed = False

    @classmethod
    def isDeltaTable(cls, _spark: Any, _path: str) -> bool:
        return cls.is_delta

    @classmethod
    def forPath(cls, _spark: Any, _path: str) -> FakeTable:
        return cls.instance

    def history(self, limit: int | None = None) -> _History:
        if limit == 1:
            return _History([{"version": self.version}])
        return _History(self.history_rows)

    def alias(self, _name: str) -> _Merge:
        return _Merge(self)


class Rows(Perm):
    def __init__(self) -> None:
        super().__init__("rows")
        self.created_empty = False

    def limit(self, _n: int) -> Rows:
        return self

    @property
    def write(self) -> Perm:  # type: ignore[override]
        self.created_empty = True
        return Perm("write")


@pytest.fixture
def delta(monkeypatch: pytest.MonkeyPatch) -> type[FakeTable]:
    class T(FakeTable):
        pass

    T.instance = T()
    T.history_rows = []
    fake_spark.install(monkeypatch, delta_table=T)
    return T


def test_merge_dlq_reports_the_metric_of_the_merge_at_version_before_plus_one(
    delta: type[FakeTable],
) -> None:
    delta.history_rows = [
        {"operation": "MERGE", "operationMetrics": {"numTargetRowsInserted": "3"}}
    ]
    rows = Rows()
    assert dlq.merge_dlq(None, "p", rows) == 3  # type: ignore[arg-type]
    assert delta.instance.inserted_all and delta.instance.executed
    assert "brand_id <=> s.brand_id" in delta.instance.merge_condition
    assert not rows.created_empty  # the table existed


def test_merge_dlq_creates_a_missing_table_from_the_schema_first(delta: type[FakeTable]) -> None:
    delta.is_delta = False
    delta.history_rows = []
    rows = Rows()
    assert dlq.merge_dlq(None, "p", rows) == 0  # type: ignore[arg-type]
    assert rows.created_empty


@pytest.mark.parametrize(
    "history",
    [
        [],  # no version committed: nothing inserted
        [{"operation": "OPTIMIZE", "operationMetrics": {"numTargetRowsInserted": "9"}}],
        [{"operation": "MERGE", "operationMetrics": None}],
        [{"operation": "MERGE", "operationMetrics": {}}],
    ],
)
def test_merge_dlq_counts_zero_unless_the_next_version_is_a_merge_with_the_metric(
    delta: type[FakeTable], history: list[dict[str, Any]]
) -> None:
    delta.history_rows = history
    assert dlq.merge_dlq(None, "p", Rows()) == 0  # type: ignore[arg-type]


def test_set_replay_outcome_only_updates_rows_not_yet_replayed_and_bumps_the_count(
    delta: type[FakeTable],
) -> None:
    delta.history_rows = [{"operation": "MERGE", "operationMetrics": {"numTargetRowsUpdated": "2"}}]
    assert dlq.set_replay_outcome(None, "p", Perm("outcomes")) == 2  # type: ignore[arg-type]
    condition, assignments = delta.instance.update or ("", {})
    assert condition == "t._replay_status <> 'REPLAYED'"
    assert assignments["_replay_count"] == "t._replay_count + 1"
    assert assignments["_replay_status"] == "s._replay_status"
    assert "brand_id <=> s.brand_id" in delta.instance.merge_condition


def test_set_replay_outcome_reports_zero_when_no_version_was_committed(
    delta: type[FakeTable],
) -> None:
    delta.history_rows = []
    assert dlq.set_replay_outcome(None, "p", Perm("o")) == 0  # type: ignore[arg-type]
