"""Fault-injecting variant of ``bronze_stream`` (test support only).

Run as ``python scripts/submit_job.py bronze_stream_fault --fault <kind> --fault-table <table>
--fault-batch <n> [bronze_stream args...]``. It is the real job code path (same queries, same
``write_bronze_batch``, same checkpoint and Delta idempotent-write key) with one injected failure
at a chosen (table, batch): the restart and replay tests use it to prove that a crash around the
Delta commit neither loses nor duplicates Bronze rows.

It is inert unless invoked by name. A separate job was chosen over a ``--fault`` flag on the
production job so that the production CLI has no way to crash itself.

Faults
------
* ``crash_before_write``: the Python driver exits with ``os._exit(137)`` before the Delta write.
* ``crash_after_write``: the same, after the Delta commit and before the offset commit.
* ``raise_in_callback``: ``RuntimeError`` raised from the callback after the Delta write.
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Any

from lakehouse.common.logging import get_logger
from lakehouse.ingestion.bronze import CLICKSTREAM, CUSTOMERS
from lakehouse.ingestion.jobs.bronze_stream import BronzeStreamJob


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import SparkSession

    from lakehouse.common.jobs import JobContext

FAULTS = ("crash_after_write", "crash_before_write", "raise_in_callback")
FAULT_TABLES = (CUSTOMERS.table, CLICKSTREAM.table)
CRASH_EXIT_CODE = 137
_log = get_logger(__name__)


def _non_negative_int(value: str) -> int:
    import argparse as _argparse

    try:
        number = int(value)
    except ValueError:
        raise _argparse.ArgumentTypeError(f"{value!r} is not an integer") from None
    if number < 0:
        raise _argparse.ArgumentTypeError(f"must be >= 0, got {number}")
    return number


class BronzeStreamFaultJob(BronzeStreamJob):
    """``bronze_stream`` with one injected fault at (``--fault-table``, ``--fault-batch``)."""

    name = "bronze_stream_fault"

    def __init__(self) -> None:
        self._fault: str | None = None
        self._fault_table: str | None = None
        self._fault_batch: int | None = None

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        super().add_arguments(parser)
        parser.add_argument("--fault", required=True, choices=FAULTS, help="fault to inject")
        parser.add_argument(
            "--fault-table", required=True, choices=FAULT_TABLES, help="table to fault"
        )
        parser.add_argument(
            "--fault-batch",
            required=True,
            type=_non_negative_int,
            help="micro-batch id (>= 0) at which the fault fires",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        self._fault = args.fault
        self._fault_table = args.fault_table
        self._fault_batch = args.fault_batch
        return super().run(spark, args, ctx)

    def _matches(self, table: str, batch_id: int) -> bool:
        return table == self._fault_table and batch_id == self._fault_batch

    def _crash(self, table: str, batch_id: int) -> None:
        _log.error("bronze_stream_fault.crash", fault=self._fault, table=table, batch_id=batch_id)
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(CRASH_EXIT_CODE)

    def _before_write(self, table: str, batch_id: int) -> None:
        if self._fault == "crash_before_write" and self._matches(table, batch_id):
            self._crash(table, batch_id)

    def _after_write(self, table: str, batch_id: int) -> None:
        if not self._matches(table, batch_id):
            return
        if self._fault == "crash_after_write":
            self._crash(table, batch_id)
        if self._fault == "raise_in_callback":
            raise RuntimeError(f"injected fault after write: {table} batch {batch_id}")


JOB = BronzeStreamFaultJob
