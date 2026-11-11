"""silver_clickstream and silver_customers_history: run(), registration, start_query (fakes)."""

from __future__ import annotations

import argparse
from typing import Any

import pytest

from lakehouse.common.jobs import JobContext
from lakehouse.common.spark import SparkSettings
from lakehouse.silver.clickstream import ClickstreamBatchCounts
from lakehouse.silver.customers_history import HistoryBatchCounts
from lakehouse.silver.jobs import silver_clickstream as click
from lakehouse.silver.jobs import silver_customers as cust
from lakehouse.silver.jobs import silver_customers_history as hist
from tests.unit.silver.fakes import Chain, FakeQuery, FakeSpark


KEY = b"k" * 40
HIVE = SparkSettings(catalog="hive")


def _ctx() -> JobContext:
    return JobContext("j", "run1", HIVE)


def _click_counts(**kw: int) -> ClickstreamBatchCounts:
    base = dict.fromkeys(
        (
            "rows_read",
            "reread_rows",
            "parse_excluded",
            "dlq_excluded",
            "duplicates_removed",
            "late_beyond_window",
            "silver_accepted",
        ),
        0,
    )
    return ClickstreamBatchCounts(**{**base, **kw})


def _hist_counts(**kw: int) -> HistoryBatchCounts:
    fields = HistoryBatchCounts.__dataclass_fields__
    return HistoryBatchCounts(**{**dict.fromkeys(fields, 0), **kw})


def _parse(job: Any, *argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    job().add_arguments(parser)
    return parser.parse_args(list(argv))


# ---- clickstream --------------------------------------------------------------------------------


class ClickEnv:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.args: list[tuple[Any, ...]] = []
        self.query = FakeQuery()
        self.bronze_exists = True
        self.registered: list[Any] = []


@pytest.fixture
def cenv(monkeypatch: pytest.MonkeyPatch) -> ClickEnv:
    e = ClickEnv()
    monkeypatch.setattr(
        click,
        "delta_exists_at_location",
        lambda _s, _t: e.events.append("exists") or e.bronze_exists,
    )
    monkeypatch.setattr(click, "ensure_table", lambda _s, p: e.events.append(f"ensure:{p}"))

    def start(*a: Any) -> Any:
        e.events.append("start")
        e.args.append(a)
        a[-1].add(_click_counts(rows_read=5, silver_accepted=4, late_beyond_window=1))
        return e.query

    monkeypatch.setattr(click, "start_query", start)

    def register(_s: Any, _st: Any, ns: Any) -> dict[str, str]:
        e.events.append("register")
        e.registered.append(ns)
        return {"clickstream": "silver.clickstream"}

    monkeypatch.setattr(click, "register_silver_table", register)
    return e


def test_clickstream_run_passes_paths_lookback_and_registers_last(cenv: ClickEnv) -> None:
    args = _parse(click.SilverClickstreamJob, "--lookback-days", "3")
    out = click.SilverClickstreamJob().run(None, args, _ctx())  # type: ignore[arg-type]
    _s, bronze, silver, ck, mx, lookback, _c = cenv.args[0]
    assert bronze == "s3a://lakehouse/bronze/raw_clickstream"
    assert silver == "s3a://lakehouse/silver/clickstream"
    assert ck == "s3a://lakehouse/checkpoints/silver_clickstream/clickstream"
    assert (mx, lookback) == (100, 3)
    assert cenv.events.index("start") < cenv.events.index("register")
    assert out["lookback_days"] == 3
    assert out["counts"]["late_beyond_window"] == 1
    assert out["catalog"] == {"clickstream": "silver.clickstream"}
    assert not cenv.query.isActive


def test_clickstream_namespace_scopes_paths(cenv: ClickEnv) -> None:
    args = _parse(click.SilverClickstreamJob, "--namespace", "ns1")
    click.SilverClickstreamJob().run(None, args, _ctx())  # type: ignore[arg-type]
    _s, bronze, silver, ck, *_ = cenv.args[0]
    assert "/ns1/" in bronze and "/ns1/" in silver and "/ns1/" in ck
    assert cenv.registered == ["ns1"]


def test_clickstream_missing_bronze_fails_before_ensure(cenv: ClickEnv) -> None:
    cenv.bronze_exists = False
    with pytest.raises(RuntimeError, match="bronze_stream never ran"):
        click.SilverClickstreamJob().run(None, _parse(click.SilverClickstreamJob), _ctx())  # type: ignore[arg-type]
    assert cenv.events == ["exists"]


def test_clickstream_query_error_is_wrapped_and_skips_registration(cenv: ClickEnv) -> None:
    cenv.query = FakeQuery(error="merge failed")
    with pytest.raises(RuntimeError, match="silver_clickstream query failed: merge failed"):
        click.SilverClickstreamJob().run(None, _parse(click.SilverClickstreamJob), _ctx())  # type: ignore[arg-type]
    assert cenv.query.stopped
    assert cenv.registered == []


def test_clickstream_omits_empty_catalog(cenv: ClickEnv, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(click, "register_silver_table", lambda *_a: {})
    out = click.SilverClickstreamJob().run(None, _parse(click.SilverClickstreamJob), _ctx())  # type: ignore[arg-type]
    assert "catalog" not in out


def test_clickstream_counters_accumulate() -> None:
    c = click._Counters()
    c.add(_click_counts(rows_read=3, reread_rows=1))
    c.add(_click_counts(rows_read=2, silver_accepted=2))
    assert c.batches == 2
    assert c.totals.as_dict()["rows_read"] == 5
    assert c.totals.as_dict()["reread_rows"] == 1
    assert c.totals.as_dict()["silver_accepted"] == 2


@pytest.mark.parametrize(("raw", "ok"), [("0", True), ("7", True), ("-1", False), ("x", False)])
def test_non_negative_int_validator(raw: str, ok: bool) -> None:
    if ok:
        assert click._non_negative_int(raw) == int(raw)
    else:
        with pytest.raises(argparse.ArgumentTypeError):
            click._non_negative_int(raw)


def _patch_reg(mod: Any, monkeypatch: pytest.MonkeyPatch, exists: bool, fail: bool) -> None:
    monkeypatch.setattr(mod, "delta_exists_at_location", lambda _s, _t: exists)

    def reg(_s: Any, spec: Any, resolved: Any) -> str:
        if fail:
            raise OSError("metastore down")
        return str(resolved.name)

    monkeypatch.setattr(mod, "register_table", reg)


@pytest.mark.parametrize(
    ("mod", "fn", "table"),
    [
        (click, "register_silver_table", "clickstream"),
        (hist, "register_history_table", "customers_history"),
    ],
)
class TestRegistration:
    def test_registers_named_table(
        self, monkeypatch: pytest.MonkeyPatch, mod: Any, fn: str, table: str
    ) -> None:
        _patch_reg(mod, monkeypatch, True, False)
        assert getattr(mod, fn)(None, HIVE, None) == {table: f"silver.{table}"}

    def test_namespace_and_in_memory_are_skipped(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mod: Any,
        fn: str,
        table: str,  # noqa: ARG002
    ) -> None:
        _patch_reg(mod, monkeypatch, True, True)  # would raise if it were attempted
        assert getattr(mod, fn)(None, HIVE, "ns1") == {}
        assert getattr(mod, fn)(None, SparkSettings(catalog="in_memory"), None) == {}

    def test_missing_data_is_not_registered(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mod: Any,
        fn: str,
        table: str,  # noqa: ARG002
    ) -> None:
        _patch_reg(mod, monkeypatch, False, True)
        assert getattr(mod, fn)(None, HIVE, None) == {}

    def test_failure_is_wrapped(
        self, monkeypatch: pytest.MonkeyPatch, mod: Any, fn: str, table: str
    ) -> None:
        _patch_reg(mod, monkeypatch, True, True)
        with pytest.raises(cust.CatalogRegistrationError, match=f"silver.{table}"):
            getattr(mod, fn)(None, HIVE, None)


# ---- clickstream start_query --------------------------------------------------------------------


def test_clickstream_start_query_and_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    chain = Chain()
    spark = FakeSpark(chain)
    seen: list[tuple[Any, ...]] = []

    def process(*a: Any) -> ClickstreamBatchCounts:
        seen.append(a)
        return _click_counts(rows_read=2, silver_accepted=2)

    monkeypatch.setattr(click, "process_clickstream_batch", process)
    counters = click._Counters()
    q = click.start_query(spark, "B", "S", "ck", 5, 2, counters)  # type: ignore[arg-type]
    assert q.id == "qid-1"
    assert spark.readStream.loads == ["B"]
    assert spark.readStream.options == [("maxFilesPerTrigger", 5)]
    by = {n: (a, kw) for n, a, kw in chain.calls}
    assert by["trigger"][1] == {"availableNow": True}
    assert by["option"][0] == ("checkpointLocation", "ck")
    chain.callback("DF", 1)
    sp, df, path, lookback, now = seen[0]
    assert (sp, df, path, lookback) == (spark, "DF", "S", 2)
    assert now.tzinfo is not None
    assert counters.batches == 1


def test_clickstream_callback_failure_counts_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    chain = Chain()

    def boom(*_a: Any) -> Any:
        raise RuntimeError("bad")

    monkeypatch.setattr(click, "process_clickstream_batch", boom)
    counters = click._Counters()
    click.start_query(FakeSpark(chain), "B", "S", "ck", 1, 7, counters)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError):
        chain.callback("DF", 0)
    assert counters.batches == 0


# ---- customers history --------------------------------------------------------------------------


class HistEnv:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.query = FakeQuery()
        self.registered: list[Any] = []


@pytest.fixture
def henv(monkeypatch: pytest.MonkeyPatch) -> HistEnv:
    e = HistEnv()
    monkeypatch.setattr(hist, "load_pii_key", lambda: KEY)
    monkeypatch.setattr(hist, "delta_exists_at_location", lambda _s, _t: True)
    monkeypatch.setattr(hist, "ensure_history_table", lambda _s, _p: e.events.append("ensure"))

    def start(*a: Any) -> Any:
        e.events.append("start")
        a[-1].add(_hist_counts(**{next(iter(HistoryBatchCounts.__dataclass_fields__)): 2}))
        return e.query

    monkeypatch.setattr(hist, "start_query", start)

    def register(_s: Any, _st: Any, ns: Any) -> dict[str, str]:
        e.events.append("register")
        e.registered.append(ns)
        return {"customers_history": "silver.customers_history"}

    monkeypatch.setattr(hist, "register_history_table", register)
    return e


def test_history_run_returns_counts_and_catalog_after_start(henv: HistEnv) -> None:
    out = hist.SilverCustomersHistoryJob().run(
        None,  # type: ignore[arg-type]
        _parse(hist.SilverCustomersHistoryJob),
        _ctx(),
    )
    assert henv.events == ["ensure", "start", "register"]
    assert out["batches"] == 1
    assert out["path"] == "s3a://lakehouse/silver/customers_history"
    assert out["catalog"] == {"customers_history": "silver.customers_history"}


def test_history_query_error_is_wrapped_and_skips_registration(henv: HistEnv) -> None:
    henv.query = FakeQuery(error="rebuild failed")
    with pytest.raises(RuntimeError, match="silver_customers_history query failed"):
        hist.SilverCustomersHistoryJob().run(
            None,  # type: ignore[arg-type]
            _parse(hist.SilverCustomersHistoryJob),
            _ctx(),
        )
    assert henv.query.stopped
    assert henv.registered == []


def test_history_start_query_and_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    chain = Chain()
    spark = FakeSpark(chain)
    seen: list[tuple[Any, ...]] = []

    def process(*a: Any) -> HistoryBatchCounts:
        seen.append(a)
        return _hist_counts()

    monkeypatch.setattr(hist, "process_history_batch", process)
    counters = hist._Counters()
    hist.start_query(spark, "B", "S", "ck", 3, KEY, counters)  # type: ignore[arg-type]
    assert spark.readStream.options == [("maxFilesPerTrigger", 3)]
    chain.callback("DF", 0)
    sp, df, bronze, silver, key, now = seen[0]
    assert (sp, df, silver, key) == (spark, "DF", "S", KEY)
    assert bronze.path == "B"
    assert now.tzinfo is not None
    assert counters.batches == 1


def test_history_counters_accumulate() -> None:
    c = hist._Counters()
    name = next(iter(HistoryBatchCounts.__dataclass_fields__))
    c.add(_hist_counts(**{name: 2}))
    c.add(_hist_counts(**{name: 3}))
    assert c.totals.as_dict()[name] == 5
    assert c.batches == 2
