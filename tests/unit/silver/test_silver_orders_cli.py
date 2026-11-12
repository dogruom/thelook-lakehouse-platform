"""silver_orders job wiring on the host without PySpark: CLI parsing, job registry, counter dataclass.

Derived from the W3.D17 design (Interface section), not from the job code. Spark behaviour is
covered by the Spark and integration suites.
"""

from __future__ import annotations

import argparse
import dataclasses
import subprocess
import sys
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

from lakehouse.common.jobs import resolve_job


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = "lakehouse.silver.jobs.silver_orders"
JOB_NAME = "silver_orders"
BLOCK = """
import importlib, sys

class _Block:
    def find_spec(self, name, path=None, target=None):
        if name == "pyspark" or name.startswith("pyspark.") or name == "delta":
            raise ImportError("pyspark/delta is blocked in this test")
        return None

sys.meta_path.insert(0, _Block())
module = importlib.import_module({module!r})
assert module.JOB.name == {job!r}
import lakehouse.silver.orders
"""


def _job() -> Any:
    return import_module(MODULE).JOB


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=JOB_NAME)
    _job()().add_arguments(parser)
    return parser.parse_args(list(argv))


def test_job_resolves_by_name() -> None:
    assert resolve_job(JOB_NAME) is _job()
    assert _job().name == JOB_NAME


def test_modules_import_without_pyspark() -> None:
    result = subprocess.run(
        [sys.executable, "-c", BLOCK.format(module=MODULE, job=JOB_NAME)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-1500:]


@pytest.mark.parametrize("bad", ["../x", "a/b", ""])
def test_namespace_is_a_single_path_segment_at_run_time_not_parse_time(bad: str) -> None:
    """Parsing accepts any string; ``resolve`` rejects unsafe parts before any query starts."""
    assert _parse("--namespace", bad).namespace == bad


def test_defaults_and_parsing() -> None:
    args = _parse()
    assert (args.namespace, args.max_files_per_trigger) == (None, 100)
    args = _parse("--namespace", "itest_ns", "--max-files-per-trigger", "5")
    assert (args.namespace, args.max_files_per_trigger) == ("itest_ns", 5)


@pytest.mark.parametrize("bad", ["0", "-3", "x", "1.5"])
def test_max_files_per_trigger_must_be_a_positive_integer(bad: str) -> None:
    with pytest.raises(SystemExit):
        _parse("--max-files-per-trigger", bad)


def test_counts_dataclass_is_frozen_and_has_every_counter() -> None:
    cls = import_module("lakehouse.silver.orders").OrderBatchCounts
    assert cls.__dataclass_params__.frozen  # type: ignore[attr-defined]
    names = {f.name for f in dataclasses.fields(cls)}
    assert {
        "rows_read",
        "reread_rows",
        "parse_excluded",
        "dlq_excluded",
        "duplicates_removed",
        "fk_orphans",
        "silver_accepted",
        "items_inserted",
    } <= names
    zero = cls(**dict.fromkeys(names, 0))
    with pytest.raises(dataclasses.FrozenInstanceError):
        zero.rows_read = 1
    assert dataclasses.asdict(zero)["rows_read"] == 0
