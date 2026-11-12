"""silver_customers job wiring on the host without PySpark: CLI, run() ordering, counters, callback."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from lakehouse.common.jobs import JobContext, resolve_job
from lakehouse.common.pii import PII_KEY_ENV, PiiKeyError
from lakehouse.common.spark import SparkSettings
from lakehouse.silver import customers
from lakehouse.silver.customers import CustomerBatchCounts
from lakehouse.silver.jobs import silver_customers
from lakehouse.silver.jobs.silver_customers import SilverCustomersJob, _Counters


if TYPE_CHECKING:
    from datetime import datetime


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = "lakehouse.silver.jobs.silver_customers"
BLOCK = """
import importlib, sys

class _Block:
    def find_spec(self, name, path=None, target=None):
        if name == "pyspark" or name.startswith("pyspark.") or name == "delta":
            raise ImportError("pyspark/delta is blocked in this test")
        return None

sys.meta_path.insert(0, _Block())
module = importlib.import_module({module!r})
assert module.JOB.name == "silver_customers"
import lakehouse.silver.customers
"""
KEY = b"k" * 40


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="silver_customers")
    SilverCustomersJob().add_arguments(parser)
    return parser.parse_args(list(argv))


def _counts(**kw: int) -> CustomerBatchCounts:
    base = {
        "rows_read": 0,
        "reread_rows": 0,
        "parse_excluded": 0,
        "dlq_excluded": 0,
        "duplicates_removed": 0,
        "stale_cdc_noop": 0,
        "silver_accepted": 0,
    }
    return CustomerBatchCounts(**{**base, **kw})


def test_job_resolves_by_name() -> None:
    assert resolve_job("silver_customers") is SilverCustomersJob
    assert silver_customers.JOB is SilverCustomersJob


def test_modules_import_without_pyspark() -> None:
    result = subprocess.run(
        [sys.executable, "-c", BLOCK.format(module=MODULE)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-1500:]


def test_defaults_and_parsing() -> None:
    args = _parse()
    assert (args.namespace, args.max_files_per_trigger) == (None, 100)
    args = _parse("--namespace", "itest_ns", "--max-files-per-trigger", "5")
    assert (args.namespace, args.max_files_per_trigger) == ("itest_ns", 5)


@pytest.mark.parametrize("bad", ["0", "-3", "x", "1.5"])
def test_max_files_per_trigger_must_be_a_positive_integer(bad: str) -> None:
    with pytest.raises(SystemExit):
        _parse("--max-files-per-trigger", bad)


# ---- counters / CustomerBatchCounts -------------------------------------------------------------


def test_counts_as_dict_has_every_field_and_defaults_late_to_zero() -> None:
    d = _counts(rows_read=9, silver_accepted=4).as_dict()
    assert d["rows_read"] == 9
    assert d["silver_accepted"] == 4
    assert d["late_beyond_window"] == 0
    assert len(d) == 8


def test_counts_are_frozen() -> None:
    with pytest.raises(AttributeError):
        _counts().rows_read = 1  # type: ignore[misc]


def test_counters_start_empty_and_accumulate_every_field() -> None:
    c = _Counters()
    assert c.batches == 0
    assert set(c.totals.as_dict().values()) == {0}
    c.add(_counts(rows_read=5, reread_rows=1, parse_excluded=1, silver_accepted=3))
    c.add(_counts(rows_read=2, dlq_excluded=1, duplicates_removed=1))
    assert c.batches == 2
    assert c.totals.as_dict() == {
        "rows_read": 7,
        "reread_rows": 1,
        "parse_excluded": 1,
        "dlq_excluded": 1,
        "duplicates_removed": 1,
        "stale_cdc_noop": 0,
        "silver_accepted": 3,
        "late_beyond_window": 0,
    }


def test_error_types_are_distinct_runtime_errors_and_keep_their_message() -> None:
    assert issubclass(customers.ReconciliationError, RuntimeError)
    assert issubclass(customers.MultipleMatchError, RuntimeError)
    assert not issubclass(customers.ReconciliationError, customers.MultipleMatchError)
    assert "silver.customers batch" in str(
        customers.ReconciliationError("silver.customers batch: bad")
    )
    assert "2 distinct" in str(customers.MultipleMatchError("3 rows for 2 distinct keys"))


# ---- run() with fakes ---------------------------------------------------------------------------


class FakeQuery:
    def __init__(self, error: Any = None, await_raises: Exception | None = None) -> None:
        self.id = "qid-1"
        self.isActive = True
        self.stopped = False
        self._error = error
        self._await_raises = await_raises
        self.awaited = False

    def awaitTermination(self) -> None:
        self.awaited = True
        if self._await_raises:
            raise self._await_raises

    def exception(self) -> Any:
        return self._error

    def stop(self) -> None:
        self.stopped = True
        self.isActive = False


def _ctx() -> JobContext:
    return JobContext("silver_customers", "run1", SparkSettings(catalog="hive"))


class Env:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.start_args: list[dict[str, Any]] = []
        self.query = FakeQuery()
        self.registered: list[Any] = []
        self.bronze_exists = True


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> Env:
    e = Env()
    monkeypatch.setattr(silver_customers, "load_pii_key", lambda: e.events.append("key") or KEY)
    monkeypatch.setattr(
        silver_customers,
        "delta_exists_at_location",
        lambda _s, _t: e.events.append("exists") or e.bronze_exists,
    )
    monkeypatch.setattr(
        silver_customers, "ensure_table", lambda _s, p: e.events.append(f"ensure:{p}")
    )

    def fake_start(spark: Any, b: str, s: str, ck: str, mx: int, key: bytes, cnt: Any) -> Any:
        e.events.append("start")
        e.start_args.append({"b": b, "s": s, "ck": ck, "mx": mx, "key": key})
        cnt.add(_counts(rows_read=3, silver_accepted=3))
        return e.query

    monkeypatch.setattr(silver_customers, "start_query", fake_start)

    def fake_register(_spark: Any, _settings: Any, namespace: Any) -> dict[str, str]:
        e.events.append("register")
        e.registered.append(namespace)
        return {"customers": "registered"}

    monkeypatch.setattr(silver_customers, "register_silver_table", fake_register)
    return e


def test_run_scopes_paths_and_registers_after_success(env: Env) -> None:
    result = SilverCustomersJob().run(None, _parse("--max-files-per-trigger", "7"), _ctx())  # type: ignore[arg-type]
    a = env.start_args[0]
    assert a["b"] == "s3a://lakehouse/bronze/raw_customers"
    assert a["s"] == "s3a://lakehouse/silver/customers"
    assert a["ck"] == "s3a://lakehouse/checkpoints/silver_customers/customers"
    assert (a["mx"], a["key"]) == (7, KEY)
    assert env.query.awaited
    assert env.events.index("key") < env.events.index("start") < env.events.index("register")
    assert result["catalog"] == {"customers": "registered"}
    assert result["batches"] == 1
    assert result["counts"]["silver_accepted"] == 3
    assert result["query_id"] == "qid-1"
    assert result["path"] == a["s"]
    assert not env.query.isActive


def test_namespace_scopes_bronze_in_silver_out_and_checkpoint(env: Env) -> None:
    SilverCustomersJob().run(None, _parse("--namespace", "itest_ns"), _ctx())  # type: ignore[arg-type]
    a = env.start_args[0]
    assert a["b"] == "s3a://lakehouse/bronze/itest_ns/raw_customers"
    assert a["s"] == "s3a://lakehouse/silver/itest_ns/customers"
    assert a["ck"] == "s3a://lakehouse/checkpoints/silver_customers/itest_ns/customers"
    assert env.registered == ["itest_ns"]  # passed through; register_silver_table skips it


def test_run_omits_catalog_when_nothing_registered(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(silver_customers, "register_silver_table", lambda *_a: {})
    result = SilverCustomersJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert "catalog" not in result


def test_missing_key_fails_before_any_read_or_query(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom() -> bytes:
        env.events.append("key")
        raise PiiKeyError(f"{PII_KEY_ENV} is not set")

    monkeypatch.setattr(silver_customers, "load_pii_key", boom)
    with pytest.raises(PiiKeyError):
        SilverCustomersJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert env.events == ["key"]  # no existence probe, no ensure_table, no start_query


def test_invalid_namespace_is_rejected_before_any_query(env: Env) -> None:
    with pytest.raises(ValueError, match="invalid path part"):
        SilverCustomersJob().run(None, _parse("--namespace", ".."), _ctx())  # type: ignore[arg-type]
    assert "start" not in env.events
    assert not any(e.startswith("ensure") for e in env.events)


def test_missing_bronze_table_fails_before_creating_silver_or_starting(env: Env) -> None:
    env.bronze_exists = False
    with pytest.raises(RuntimeError, match="bronze_stream never ran"):
        SilverCustomersJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert "start" not in env.events
    assert not any(e.startswith("ensure") for e in env.events)


def test_query_exception_raises_stops_query_and_skips_registration(env: Env) -> None:
    env.query = FakeQuery(error="merge exploded")
    with pytest.raises(RuntimeError, match="query failed: merge exploded"):
        SilverCustomersJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert env.query.stopped
    assert env.registered == []


def test_await_failure_stops_query_and_skips_registration(env: Env) -> None:
    env.query = FakeQuery(await_raises=RuntimeError("terminated"))
    with pytest.raises(RuntimeError, match="terminated"):
        SilverCustomersJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert env.query.stopped
    assert env.registered == []


def test_registration_failure_propagates(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: Any) -> dict[str, str]:
        raise silver_customers.CatalogRegistrationError("committed; registration failed")

    monkeypatch.setattr(silver_customers, "register_silver_table", boom)
    with pytest.raises(silver_customers.CatalogRegistrationError, match="committed"):
        SilverCustomersJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]


# ---- start_query / callback ---------------------------------------------------------------------


class _Chain:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.callback: Any = None

    def __getattr__(self, name: str) -> Any:
        def method(*args: Any, **kw: Any) -> Any:
            self.calls.append((name, args, kw))
            if name == "foreachBatch":
                self.callback = args[0]
            if name == "start":
                return FakeQuery()
            return self

        return method


class _Reader:
    """Shared fake of ``spark.read`` and ``spark.readStream``."""

    def __init__(self, stream: _Chain) -> None:
        self.stream = stream
        self.loads: list[str] = []
        self.options: list[tuple[str, Any]] = []
        self.formats: list[str] = []

    def format(self, fmt: str) -> _Reader:
        self.formats.append(fmt)
        return self

    def option(self, k: str, v: Any) -> _Reader:
        self.options.append((k, v))
        return self

    def load(self, path: str) -> Any:
        self.loads.append(path)
        return type("Src", (), {"writeStream": self.stream, "path": path})()


class _Spark:
    def __init__(self, chain: _Chain) -> None:
        self.read = _Reader(chain)
        self.readStream = _Reader(chain)


def test_start_query_wires_availablenow_checkpoint_and_callback() -> None:
    chain = _Chain()
    spark = _Spark(chain)
    q = silver_customers.start_query(spark, "b", "s", "ck", 9, KEY, _Counters())  # type: ignore[arg-type]
    assert q.id == "qid-1"
    assert spark.readStream.loads == ["b"]
    assert spark.readStream.options == [("maxFilesPerTrigger", 9)]
    by = {n: (a, kw) for n, a, kw in chain.calls}
    assert by["queryName"][0] == ("silver_customers",)
    assert by["trigger"][1] == {"availableNow": True}
    assert by["option"][0] == ("checkpointLocation", "ck")
    assert chain.calls[-1][0] == "start"


def test_callback_processes_batch_with_key_and_clock_and_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain = _Chain()
    spark = _Spark(chain)
    seen: list[Any] = []

    def fake_process(
        sp: Any, df: Any, bronze: Any, path: str, key: bytes, now: datetime
    ) -> CustomerBatchCounts:
        seen.append((sp, df, bronze, path, key, now))
        return _counts(rows_read=4, silver_accepted=2, stale_cdc_noop=2)

    monkeypatch.setattr(silver_customers, "process_batch", fake_process)
    counters = _Counters()
    silver_customers.start_query(spark, "bronze_p", "silver_p", "ck", 1, KEY, counters)  # type: ignore[arg-type]
    chain.callback("BATCH_DF", 3)
    sp, df, bronze, path, key, now = seen[0]
    assert (sp, df, path, key) == (spark, "BATCH_DF", "silver_p", KEY)
    assert bronze.path == "bronze_p"  # full Bronze re-read for history lookups
    assert spark.read.formats == ["delta"]
    assert now.tzinfo is not None
    assert counters.batches == 1
    assert counters.totals.silver_accepted == 2


def test_callback_failure_propagates_and_counts_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain = _Chain()
    spark = _Spark(chain)

    def boom(*_a: Any) -> CustomerBatchCounts:
        raise customers.ReconciliationError("silver.customers batch: mismatch")

    monkeypatch.setattr(silver_customers, "process_batch", boom)
    counters = _Counters()
    silver_customers.start_query(spark, "b", "s", "ck", 1, KEY, counters)  # type: ignore[arg-type]
    with pytest.raises(customers.ReconciliationError):
        chain.callback("DF", 0)
    assert counters.batches == 0
