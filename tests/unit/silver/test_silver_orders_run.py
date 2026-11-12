"""silver_orders run(), registration and start_query on the host with fakes (no PySpark)."""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING, Any

import pytest

from lakehouse.common.jobs import JobContext
from lakehouse.common.spark import SparkSettings
from lakehouse.silver.jobs import silver_customers, silver_orders
from lakehouse.silver.jobs.silver_orders import SilverOrdersJob, _Counters
from lakehouse.silver.orders import OrderBatchCounts
from tests.unit.silver.fakes import Chain, FakeQuery, FakeSpark


if TYPE_CHECKING:
    from lakehouse.common.tables import ResolvedTable


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="silver_orders")
    SilverOrdersJob().add_arguments(parser)
    return parser.parse_args(list(argv))


def _ctx() -> JobContext:
    return JobContext("silver_orders", "run1", SparkSettings(catalog="hive"))


def _counts(**kw: int) -> OrderBatchCounts:
    base = dict.fromkeys(
        (
            "rows_read",
            "reread_rows",
            "parse_excluded",
            "dlq_excluded",
            "duplicates_removed",
            "fk_orphans",
            "silver_accepted",
            "items_inserted",
        ),
        0,
    )
    return OrderBatchCounts(**{**base, **kw})


class Env:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.missing: set[str] = set()
        self.start_args: list[tuple[Any, ...]] = []
        self.query = FakeQuery()
        self.registered: list[Any] = []


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> Env:
    e = Env()

    def exists(_s: Any, t: ResolvedTable) -> bool:
        e.events.append(f"exists:{t.location}")
        return not any(m in t.location for m in e.missing)

    monkeypatch.setattr(silver_orders, "delta_exists_at_location", exists)
    monkeypatch.setattr(
        silver_orders, "ensure_tables", lambda _s, o, i: e.events.append(f"ensure:{o}|{i}")
    )

    def fake_start(*args: Any) -> Any:
        e.events.append("start")
        e.start_args.append(args)
        args[-1].add(_counts(rows_read=4, silver_accepted=3, items_inserted=9))
        return e.query

    monkeypatch.setattr(silver_orders, "start_query", fake_start)

    def fake_register(_s: Any, _settings: Any, ns: Any) -> dict[str, str]:
        e.events.append("register")
        e.registered.append(ns)
        return {"orders": "registered"}

    monkeypatch.setattr(silver_orders, "register_silver_tables", fake_register)
    return e


def test_run_resolves_every_table_and_registers_after_success(env: Env) -> None:
    result = SilverOrdersJob().run(None, _parse("--max-files-per-trigger", "4"), _ctx())  # type: ignore[arg-type]
    _spark, paths, checkpoint, max_files, run_id, _cnt = env.start_args[0]
    assert paths == {
        "raw_orders": "s3a://lakehouse/bronze/raw_orders",
        "customers": "s3a://lakehouse/silver/customers",
        "raw_products": "s3a://lakehouse/bronze/raw_products",
        "orders": "s3a://lakehouse/silver/orders",
        "order_items": "s3a://lakehouse/silver/order_items",
        "dlq_orders": "s3a://lakehouse/dead_letter/dlq_orders",
    }
    assert checkpoint == "s3a://lakehouse/checkpoints/silver_orders/orders"
    assert (max_files, run_id) == (4, "run1")
    assert env.events.index("start") < env.events.index("register")
    assert result["batches"] == 1
    assert result["counts"]["items_inserted"] == 9
    assert result["items_path"].endswith("silver/order_items")
    assert result["dlq_path"].endswith("dead_letter/dlq_orders")
    assert result["catalog"] == {"orders": "registered"}
    assert env.query.awaited and not env.query.isActive


def test_namespace_scopes_every_path_and_checkpoint(env: Env) -> None:
    SilverOrdersJob().run(None, _parse("--namespace", "ns1"), _ctx())  # type: ignore[arg-type]
    _s, paths, checkpoint, *_ = env.start_args[0]
    assert all("/ns1/" in p for p in paths.values())
    assert checkpoint == "s3a://lakehouse/checkpoints/silver_orders/ns1/orders"
    assert env.registered == ["ns1"]


@pytest.mark.parametrize("missing", ["raw_orders", "silver/customers", "raw_products"])
def test_missing_prerequisite_fails_before_ensure_or_start(env: Env, missing: str) -> None:
    env.missing = {missing}
    with pytest.raises(RuntimeError, match="producing job never ran"):
        SilverOrdersJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert "start" not in env.events
    assert not any(e.startswith("ensure") for e in env.events)


def test_invalid_namespace_is_rejected_before_any_probe(env: Env) -> None:
    with pytest.raises(ValueError, match="invalid path part"):
        SilverOrdersJob().run(None, _parse("--namespace", ".."), _ctx())  # type: ignore[arg-type]
    assert env.events == []


def test_query_error_is_wrapped_stops_query_and_skips_registration(env: Env) -> None:
    env.query = FakeQuery(error="boom")
    with pytest.raises(RuntimeError, match="silver_orders query failed: boom"):
        SilverOrdersJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert env.query.stopped
    assert env.registered == []


def test_await_failure_stops_query_and_skips_registration(env: Env) -> None:
    env.query = FakeQuery(await_raises=RuntimeError("terminated"))
    with pytest.raises(RuntimeError, match="terminated"):
        SilverOrdersJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert env.query.stopped
    assert env.registered == []


def test_empty_catalog_result_is_omitted(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(silver_orders, "register_silver_tables", lambda *_a: {})
    assert "catalog" not in SilverOrdersJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]


# ---- counters -----------------------------------------------------------------------------------


def test_counters_accumulate_every_field_and_count_batches() -> None:
    c = _Counters()
    assert c.batches == 0
    assert set(c.totals.as_dict().values()) == {0}
    c.add(_counts(rows_read=5, fk_orphans=2, items_inserted=4))
    c.add(_counts(rows_read=1, fk_orphans=1, silver_accepted=1))
    assert c.batches == 2
    d = c.totals.as_dict()
    assert (d["rows_read"], d["fk_orphans"], d["items_inserted"], d["silver_accepted"]) == (
        6,
        3,
        4,
        1,
    )


def test_batch_counts_as_dict_omits_already_present_but_keeps_late_zero() -> None:
    d = _counts(rows_read=2).as_dict()
    assert "already_present" not in d
    assert d["late_beyond_window"] == 0
    assert len(d) == 9


# ---- register_silver_tables ---------------------------------------------------------------------


def _patch_registration(
    monkeypatch: pytest.MonkeyPatch, *, exists: bool = True, fail_on: str | None = None
) -> list[str]:
    done: list[str] = []
    monkeypatch.setattr(silver_orders, "delta_exists_at_location", lambda _s, _t: exists)

    def register(_s: Any, spec: Any, resolved: Any) -> str:
        if spec.name == fail_on:
            raise OSError("metastore down")
        done.append(spec.name)
        return str(resolved.name)

    monkeypatch.setattr(silver_orders, "register_table", register)
    return done


def test_registers_three_tables_for_the_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    done = _patch_registration(monkeypatch)
    out = silver_orders.register_silver_tables(None, SparkSettings(catalog="hive"), None)  # type: ignore[arg-type]
    assert done == ["orders", "order_items", "dlq_orders"]
    assert out == {
        "orders": "silver.orders",
        "order_items": "silver.order_items",
        "dlq_orders": "dead_letter.dlq_orders",
    }


def test_namespaced_and_in_memory_runs_are_never_registered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    done = _patch_registration(monkeypatch)
    hive = SparkSettings(catalog="hive")
    assert silver_orders.register_silver_tables(None, hive, "ns1") == {}  # type: ignore[arg-type]
    mem = SparkSettings(catalog="in_memory")
    assert silver_orders.register_silver_tables(None, mem, None) == {}  # type: ignore[arg-type]
    assert done == []


def test_tables_without_data_are_not_registered(monkeypatch: pytest.MonkeyPatch) -> None:
    done = _patch_registration(monkeypatch, exists=False)
    assert silver_orders.register_silver_tables(None, SparkSettings(catalog="hive"), None) == {}  # type: ignore[arg-type]
    assert done == []


def test_registration_failure_is_wrapped_naming_the_table(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_registration(monkeypatch, fail_on="order_items")
    with pytest.raises(silver_customers.CatalogRegistrationError, match="silver.order_items"):
        silver_orders.register_silver_tables(None, SparkSettings(catalog="hive"), None)  # type: ignore[arg-type]


# ---- start_query --------------------------------------------------------------------------------

PATHS = {
    "raw_orders": "B",
    "customers": "C",
    "raw_products": "P",
    "orders": "O",
    "order_items": "I",
    "dlq_orders": "D",
}


def test_start_query_wires_availablenow_checkpoint_and_callback() -> None:
    chain = Chain()
    spark = FakeSpark(chain)
    q = silver_orders.start_query(spark, PATHS, "ck", 9, "r", _Counters())  # type: ignore[arg-type]
    assert q.id == "qid-1"
    assert spark.readStream.loads == ["B"]
    assert spark.readStream.options == [("maxFilesPerTrigger", 9)]
    by = {n: (a, kw) for n, a, kw in chain.calls}
    assert by["queryName"][0] == ("silver_orders",)
    assert by["trigger"][1] == {"availableNow": True}
    assert by["option"][0] == ("checkpointLocation", "ck")


def test_callback_reads_full_tables_passes_paths_and_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain = Chain()
    spark = FakeSpark(chain)
    seen: list[tuple[Any, ...]] = []

    def fake_process(*args: Any) -> OrderBatchCounts:
        seen.append(args)
        return _counts(rows_read=2, silver_accepted=2)

    monkeypatch.setattr(silver_orders, "process_orders_batch", fake_process)
    counters = _Counters()
    silver_orders.start_query(spark, PATHS, "ck", 1, "run-9", counters)  # type: ignore[arg-type]
    chain.callback("BATCH", 0)
    sp, df, bronze, customers, products, o, i, d, run_id, now = seen[0]
    assert (sp, df, o, i, d, run_id) == (spark, "BATCH", "O", "I", "D", "run-9")
    assert (bronze.path, customers.path, products.path) == ("B", "C", "P")
    assert now.tzinfo is not None
    assert counters.batches == 1
    assert counters.totals.silver_accepted == 2


def test_callback_failure_propagates_and_counts_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    chain = Chain()
    spark = FakeSpark(chain)

    def boom(*_a: Any) -> OrderBatchCounts:
        raise RuntimeError("silver.orders batch: mismatch")

    monkeypatch.setattr(silver_orders, "process_orders_batch", boom)
    counters = _Counters()
    silver_orders.start_query(spark, PATHS, "ck", 1, "r", counters)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="mismatch"):
        chain.callback("DF", 0)
    assert counters.batches == 0
