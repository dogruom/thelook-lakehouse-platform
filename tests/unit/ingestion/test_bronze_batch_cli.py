"""bronze_batch job wiring on the host without PySpark: CLI, paths, counters, run(), callback."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from lakehouse.common.jobs import JobContext, resolve_job
from lakehouse.common.spark import SparkSettings
from lakehouse.ingestion import batch
from lakehouse.ingestion.jobs import bronze_batch
from lakehouse.ingestion.jobs.bronze_batch import BronzeBatchJob, _Counters


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = "lakehouse.ingestion.jobs.bronze_batch"
BLOCK = """
import importlib, sys

class _Block:
    def find_spec(self, name, path=None, target=None):
        if name == "pyspark" or name.startswith("pyspark."):
            raise ImportError("pyspark is blocked in this test")
        return None

sys.meta_path.insert(0, _Block())
module = importlib.import_module({module!r})
assert module.JOB.name == "bronze_batch"
import lakehouse.ingestion.batch
"""


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="bronze_batch")
    BronzeBatchJob().add_arguments(parser)
    return parser.parse_args(list(argv))


def test_job_resolves_by_name() -> None:
    assert resolve_job("bronze_batch") is BronzeBatchJob


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


def test_defaults() -> None:
    args = _parse()
    assert args.namespace is None
    assert args.max_files_per_trigger == 100


def test_arguments_are_parsed() -> None:
    args = _parse("--namespace", "itest_ns", "--max-files-per-trigger", "5")
    assert (args.namespace, args.max_files_per_trigger) == ("itest_ns", 5)


@pytest.mark.parametrize("bad", ["0", "-3", "x"])
def test_max_files_per_trigger_must_be_a_positive_integer(bad: str) -> None:
    with pytest.raises(SystemExit):
        _parse("--max-files-per-trigger", bad)


def test_targets_and_constants() -> None:
    assert bronze_batch.TARGETS == (batch.ORDERS, batch.PRODUCTS)
    assert (batch.ORDERS.entity, batch.ORDERS.table) == ("orders", "raw_orders")
    assert batch.ORDERS.partition_by == ("brand_id",)  # tenant partitioning for orders only
    assert (batch.PRODUCTS.entity, batch.PRODUCTS.table) == ("products", "raw_products")
    assert batch.PRODUCTS.partition_by == ()
    assert batch.ORDERS.contract != batch.PRODUCTS.contract


def test_query_names_are_distinct_per_table() -> None:
    names = {bronze_batch.query_name(t) for t in bronze_batch.TARGETS}
    assert names == {"bronze_batch_raw_orders", "bronze_batch_raw_products"}


def test_source_glob_spans_brands_and_dates_for_one_entity() -> None:
    assert (
        bronze_batch.source_glob("s3a://lakehouse/landing", batch.ORDERS)
        == "s3a://lakehouse/landing/*/orders/*/*"
    )
    assert bronze_batch.source_glob("s3a://b/landing/", batch.PRODUCTS).endswith(
        "/landing/*/products/*/*"
    )


def test_counters_accumulate_per_brand_and_total() -> None:
    c = _Counters()
    bc = batch.BrandCounts
    c.add(batch.FileBatchCounts(2, 10, 1, {"b": bc(1, 6, 1), "a": bc(1, 4, 0)}))
    c.add(batch.FileBatchCounts(1, 3, 0, {"a": bc(1, 3, 0)}))
    assert (c.files, c.lines, c.parse_errors, c.batches) == (3, 13, 1, 2)
    assert c.brands() == {
        "a": {"files": 2, "lines": 7, "parse_errors": 0},
        "b": {"files": 1, "lines": 6, "parse_errors": 1},
    }
    assert list(c.brands()) == ["a", "b"]  # sorted


def test_counters_start_empty() -> None:
    c = _Counters()
    assert (c.files, c.lines, c.parse_errors, c.batches, c.brands()) == (0, 0, 0, 0, {})


# ---- run() with fakes -------------------------------------------------------------------------


class FakeQuery:
    def __init__(self, qid: str) -> None:
        self.id = qid
        self.isActive = True
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True
        self.isActive = False


def _ctx(settings: SparkSettings | None = None) -> JobContext:
    return JobContext("bronze_batch", "run1", settings or SparkSettings(catalog="hive"))


@pytest.fixture
def started(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def fake_start(spark: Any, glob: str, target: Any, path: str, ckpt: str, mx: int, cnt: Any):
        calls.append({"glob": glob, "table": target.table, "path": path, "ckpt": ckpt, "max": mx})
        cnt.add(batch.FileBatchCounts(1, 2, 1, {"brand_a": batch.BrandCounts(1, 2, 1)}))
        return FakeQuery(f"q-{target.table}")

    monkeypatch.setattr(bronze_batch, "start_query", fake_start)
    monkeypatch.setattr(bronze_batch, "_await_all", lambda _q: None)
    monkeypatch.setattr(bronze_batch, "register_batch_tables", lambda *_a: {})
    return calls


def test_run_uses_one_checkpoint_per_table_and_pathglob_sources(
    started: list[dict[str, Any]],
) -> None:
    result = BronzeBatchJob().run(None, _parse("--max-files-per-trigger", "7"), _ctx())  # type: ignore[arg-type]
    by_table = {c["table"]: c for c in started}
    assert by_table["raw_orders"]["ckpt"] == "s3a://lakehouse/checkpoints/bronze_batch/raw_orders"
    assert (
        by_table["raw_products"]["ckpt"] == "s3a://lakehouse/checkpoints/bronze_batch/raw_products"
    )
    assert by_table["raw_orders"]["ckpt"] != by_table["raw_products"]["ckpt"]
    assert by_table["raw_orders"]["path"] == "s3a://lakehouse/bronze/raw_orders"
    assert by_table["raw_orders"]["glob"] == "s3a://lakehouse/landing/*/orders/*/*"
    assert by_table["raw_products"]["glob"] == "s3a://lakehouse/landing/*/products/*/*"
    assert {c["max"] for c in started} == {7}
    q = result["queries"]["raw_orders"]
    assert (q["files"], q["lines"], q["parse_errors"], q["batches"]) == (1, 2, 1, 1)
    assert q["per_brand"] == {"brand_a": {"files": 1, "lines": 2, "parse_errors": 1}}
    assert q["query_id"] == "q-raw_orders"
    assert "catalog" not in result


def test_namespace_isolates_paths_and_checkpoints(started: list[dict[str, Any]]) -> None:
    BronzeBatchJob().run(None, _parse("--namespace", "itest_ns"), _ctx())  # type: ignore[arg-type]
    by_table = {c["table"]: c for c in started}
    assert by_table["raw_orders"]["path"] == "s3a://lakehouse/bronze/itest_ns/raw_orders"
    assert "/bronze_batch/itest_ns/raw_orders" in by_table["raw_orders"]["ckpt"]
    # the namespace scopes the landing source too: reads landing/<ns>/<brand>/<entity>/...
    assert by_table["raw_orders"]["glob"] == "s3a://lakehouse/landing/itest_ns/*/orders/*/*"


def test_invalid_namespace_segment_is_rejected_before_any_query(
    started: list[dict[str, Any]],
) -> None:
    with pytest.raises(ValueError, match="invalid path part"):
        BronzeBatchJob().run(None, _parse("--namespace", ".."), _ctx())  # type: ignore[arg-type]
    assert started == []


def test_run_returns_catalog_after_ingest(
    monkeypatch: pytest.MonkeyPatch, started: list[dict[str, Any]]
) -> None:
    seen: list[Any] = []

    def fake_register(_spark: Any, _settings: Any, targets: Any, namespace: Any) -> dict[str, str]:
        seen.append((targets, namespace, len(started)))
        return {"raw_orders": "registered"}

    monkeypatch.setattr(bronze_batch, "register_batch_tables", fake_register)
    result = BronzeBatchJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert result["catalog"] == {"raw_orders": "registered"}
    assert seen == [(bronze_batch.TARGETS, None, 2)]  # registered after both queries started


def test_await_failure_stops_queries_and_skips_registration(
    monkeypatch: pytest.MonkeyPatch, started: list[dict[str, Any]]
) -> None:
    created: list[FakeQuery] = []
    monkeypatch.setattr(
        bronze_batch,
        "start_query",
        lambda *a: created.append(FakeQuery(a[2].table)) or created[-1],
    )

    def boom(_q: Any) -> None:
        raise RuntimeError("query for raw_orders failed")

    registered: list[int] = []
    monkeypatch.setattr(bronze_batch, "_await_all", boom)
    monkeypatch.setattr(bronze_batch, "register_batch_tables", lambda *_a: registered.append(1))
    with pytest.raises(RuntimeError, match="failed"):
        BronzeBatchJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert len(created) == 2
    assert all(q.stopped for q in created)
    assert registered == []


def test_second_query_start_failure_stops_the_first(monkeypatch: pytest.MonkeyPatch) -> None:
    first = FakeQuery("a")
    n = {"i": 0}

    def fake_start(*_a: Any) -> FakeQuery:
        n["i"] += 1
        if n["i"] == 1:
            return first
        raise RuntimeError("cannot start")

    monkeypatch.setattr(bronze_batch, "start_query", fake_start)
    with pytest.raises(RuntimeError, match="cannot start"):
        BronzeBatchJob().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert first.stopped


# ---- start_query / callback --------------------------------------------------------------------


class _Chain:
    """Records the writeStream builder calls; ``foreachBatch`` keeps the callback."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.callback: Any = None

    def __getattr__(self, name: str) -> Any:
        def method(*args: Any, **_kw: Any) -> Any:
            self.calls.append((name, args))
            if name == "foreachBatch":
                self.callback = args[0]
            if name == "start":
                return FakeQuery("qid-1")
            return self

        return method


class _Active:
    def __init__(self, name: str, qid: str) -> None:
        self.name, self.id = name, qid


class _Spark:
    def __init__(self, active: list[Any]) -> None:
        self.streams = type("S", (), {"active": active})()


def test_start_query_wires_availablenow_checkpoint_and_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain = _Chain()
    frame = type("F", (), {"writeStream": chain})()
    got: dict[str, Any] = {}

    def fake_stream(_spark: Any, glob: str, mx: int) -> Any:
        got.update(glob=glob, mx=mx)
        return frame

    monkeypatch.setattr(bronze_batch, "file_stream", fake_stream)
    counters = _Counters()
    query = bronze_batch.start_query(
        _Spark([]),
        "g/*",
        batch.ORDERS,
        "s3a://b/bronze/raw_orders",
        "s3a://b/ck",
        9,
        counters,  # type: ignore[arg-type]
    )
    assert query.id == "qid-1"
    assert got == {"glob": "g/*", "mx": 9}
    names = dict(chain.calls)
    assert names["queryName"] == ("bronze_batch_raw_orders",)
    assert names["trigger"] == ()  # availableNow passed as keyword
    assert names["option"] == ("checkpointLocation", "s3a://b/ck")
    assert [n for n, _ in chain.calls][-1] == "start"


def test_callback_writes_with_query_id_and_batch_id_and_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain = _Chain()
    monkeypatch.setattr(
        bronze_batch, "file_stream", lambda *_a: type("F", (), {"writeStream": chain})()
    )
    writes: list[dict[str, Any]] = []

    def fake_write(df: Any, path: str, *, target: Any, app_id: str, batch_id: int) -> Any:
        writes.append({"df": df, "path": path, "t": target, "app": app_id, "batch": batch_id})
        return batch.FileBatchCounts(2, 5, 1, {"a": batch.BrandCounts(2, 5, 1)})

    monkeypatch.setattr(bronze_batch, "write_bronze_files", fake_write)
    counters = _Counters()
    spark = _Spark([_Active("other", "x"), _Active("bronze_batch_raw_orders", "QID")])
    bronze_batch.start_query(spark, "g", batch.ORDERS, "p", "c", 1, counters)  # type: ignore[arg-type]
    chain.callback("DF", 4)
    assert writes == [{"df": "DF", "path": "p", "t": batch.ORDERS, "app": "QID", "batch": 4}]
    assert (counters.files, counters.lines, counters.parse_errors) == (2, 5, 1)


def test_callback_fails_when_query_id_is_ambiguous_or_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain = _Chain()
    monkeypatch.setattr(
        bronze_batch, "file_stream", lambda *_a: type("F", (), {"writeStream": chain})()
    )
    monkeypatch.setattr(
        bronze_batch, "write_bronze_files", lambda *_a, **_k: pytest.fail("must not write")
    )
    name = "bronze_batch_raw_orders"
    for active in ([], [_Active(name, "1"), _Active(name, "2")]):
        bronze_batch.start_query(_Spark(active), "g", batch.ORDERS, "p", "c", 1, _Counters())  # type: ignore[arg-type]
        with pytest.raises(RuntimeError, match="exactly one active query"):
            chain.callback("DF", 0)
