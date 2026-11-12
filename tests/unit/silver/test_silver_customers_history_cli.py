"""silver_customers_history job wiring on the host without PySpark: CLI, import hygiene, run() order.

The interface is fixed by the task card (``JOB``, ``--namespace``, ``--max-files-per-trigger``,
``HistoryBatchCounts``); run()-level tests monkeypatch the collaborators by the same names as
``silver_customers`` and skip when the module does not expose them.
"""

from __future__ import annotations

import argparse
import dataclasses
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from lakehouse.common.jobs import JobContext, resolve_job
from lakehouse.common.spark import SparkSettings
from lakehouse.silver import customers_history
from lakehouse.silver.customers_history import HistoryBatchCounts
from lakehouse.silver.jobs import silver_customers_history as job_module


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = "lakehouse.silver.jobs.silver_customers_history"
BLOCK = """
import importlib, sys

class _Block:
    def find_spec(self, name, path=None, target=None):
        if name == "pyspark" or name.startswith("pyspark.") or name == "delta":
            raise ImportError("pyspark/delta is blocked in this test")
        return None

sys.meta_path.insert(0, _Block())
module = importlib.import_module({module!r})
assert module.JOB.name == "silver_customers_history"
import lakehouse.silver.customers_history
"""
KEY = b"k" * 40
COUNT_FIELDS = {"rows_read", "affected_keys", "versions_written", "versions_deleted"}


def _job() -> Any:
    return job_module.JOB()


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="silver_customers_history")
    _job().add_arguments(parser)
    return parser.parse_args(list(argv))


def _ctx() -> JobContext:
    return JobContext("silver_customers_history", "run1", SparkSettings(catalog="hive"))


def test_job_resolves_by_name() -> None:
    assert resolve_job("silver_customers_history") is job_module.JOB
    assert job_module.JOB.name == "silver_customers_history"


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


def test_history_counts_expose_the_four_documented_fields() -> None:
    assert dataclasses.is_dataclass(HistoryBatchCounts)
    assert {f.name for f in dataclasses.fields(HistoryBatchCounts)} >= COUNT_FIELDS
    counts = HistoryBatchCounts(**{name: i for i, name in enumerate(sorted(COUNT_FIELDS), start=1)})
    assert {getattr(counts, n) for n in COUNT_FIELDS} == {1, 2, 3, 4}


# ---- run() with fakes (names mirror silver_customers; skipped if the module differs) ---------------


class FakeQuery:
    id = "qid-1"

    def __init__(self) -> None:
        self.isActive = True
        self.awaited = False

    def awaitTermination(self) -> None:
        self.awaited = True

    def exception(self) -> Any:
        return None

    def stop(self) -> None:
        self.isActive = False


class Env:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.start_args: list[tuple[Any, ...]] = []
        self.bronze_exists = True
        self.query = FakeQuery()


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> Env:
    needed = ("load_pii_key", "delta_exists_at_location", "ensure_history_table", "start_query")
    missing = [n for n in needed if not hasattr(job_module, n)]
    if missing:
        pytest.skip(f"job module does not expose {missing}; run() wiring not testable by name")
    e = Env()
    monkeypatch.setattr(job_module, "load_pii_key", lambda: e.events.append("key") or KEY)
    monkeypatch.setattr(
        job_module,
        "delta_exists_at_location",
        lambda _s, _t: e.events.append("exists") or e.bronze_exists,
    )
    monkeypatch.setattr(
        job_module, "ensure_history_table", lambda _s, p: e.events.append(f"ensure:{p}")
    )

    def fake_start(*args: Any) -> Any:
        e.events.append("start")
        e.start_args.append(args)
        return e.query

    monkeypatch.setattr(job_module, "start_query", fake_start)
    if hasattr(job_module, "register_silver_table"):
        monkeypatch.setattr(job_module, "register_silver_table", lambda *_a: {})
    return e


def test_namespace_scopes_bronze_history_and_checkpoint(env: Env) -> None:
    _job().run(None, _parse("--namespace", "itest_ns"), _ctx())  # type: ignore[arg-type]
    strings = [a for a in env.start_args[0] if isinstance(a, str)]
    assert "s3a://lakehouse/bronze/itest_ns/raw_customers" in strings
    assert any(s.startswith("s3a://lakehouse/silver/itest_ns/customers_history") for s in strings)
    assert any("checkpoints/silver_customers_history/itest_ns" in s for s in strings)
    assert env.query.awaited and not env.query.isActive


def test_missing_bronze_table_fails_before_creating_history_or_starting(env: Env) -> None:
    env.bronze_exists = False
    with pytest.raises(RuntimeError, match="bronze_stream never ran"):
        _job().run(None, _parse(), _ctx())  # type: ignore[arg-type]
    assert "start" not in env.events
    assert not any(e.startswith("ensure") for e in env.events)


def test_invalid_namespace_is_rejected_before_any_query(env: Env) -> None:
    with pytest.raises(ValueError, match="invalid path part"):
        _job().run(None, _parse("--namespace", ".."), _ctx())  # type: ignore[arg-type]
    assert "start" not in env.events


def test_module_exposes_the_rebuild_interface() -> None:
    for name in (
        "history_schema",
        "rebuild_history",
        "process_history_batch",
        "ensure_history_table",
        "history_content_hash",
        "HistoryBatchCounts",
    ):
        assert hasattr(customers_history, name), name
