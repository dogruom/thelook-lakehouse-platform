"""replay job wiring on the host without PySpark: CLI parsing, job registry, counter dataclass.

Derived from the W3.D17 design (Interface section), not from the job code. Spark behaviour is
covered by the Spark and integration suites.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

from lakehouse.common.jobs import resolve_job


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = "lakehouse.quality.jobs.replay"
JOB_NAME = "replay"
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
import lakehouse.quality.dlq
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
    assert _parse("--table", "orders", "--namespace", bad).namespace == bad


def test_defaults_and_parsing() -> None:
    args = _parse("--table", "orders")
    assert (args.table, args.namespace, args.max_replays) == ("orders", None, 3)
    assert getattr(args, "brand", None) is None
    args = _parse(
        "--table",
        "orders",
        "--brand",
        "brand_alpha",
        "--namespace",
        "itest_ns",
        "--max-replays",
        "5",
    )
    assert (args.brand, args.namespace, args.max_replays) == ("brand_alpha", "itest_ns", 5)


@pytest.mark.parametrize("bad", ["0", "-1", "x", "1.5"])
def test_max_replays_must_be_a_positive_integer(bad: str) -> None:
    with pytest.raises(SystemExit):
        _parse("--table", "orders", "--max-replays", bad)


@pytest.mark.parametrize("table", ["customers", "clickstream", "raw_orders", "products"])
def test_only_orders_can_be_replayed(table: str) -> None:
    """Design: stream tables are not retryable; only the order foreign key is (D-077)."""
    with pytest.raises(SystemExit):
        _parse("--table", table)


def test_dlq_status_constants_are_the_contract_strings() -> None:
    from lakehouse.quality import dlq

    assert (dlq.PENDING, dlq.REPLAYED, dlq.FAILED_AGAIN, dlq.DISCARDED) == (
        "PENDING",
        "REPLAYED",
        "FAILED_AGAIN",
        "DISCARDED",
    )


def test_dlq_table_by_bronze_maps_the_four_bronze_tables() -> None:
    from lakehouse.quality.dlq import DLQ_TABLE_BY_BRONZE

    assert {
        "raw_customers": "dlq_customers",
        "raw_clickstream": "dlq_clickstream",
        "raw_orders": "dlq_orders",
        "raw_products": "dlq_products",
    } == DLQ_TABLE_BY_BRONZE
