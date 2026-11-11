"""bronze_report CLI: ``--table`` is restricted to the Bronze stream tables (Q-004)."""

from __future__ import annotations

import argparse

import pytest

from lakehouse.ops.jobs.bronze_report import DEFAULT_TABLES, BronzeReportJob


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    BronzeReportJob().add_arguments(parser)
    return parser


def test_table_accepts_the_bronze_tables_repeatably() -> None:
    args = _parser().parse_args(["--table", "raw_customers", "--table", "raw_clickstream"])
    assert args.table == ["raw_customers", "raw_clickstream"]
    assert set(DEFAULT_TABLES) == {"raw_customers", "raw_clickstream"}


def test_table_defaults_to_none_so_the_job_reports_all_tables() -> None:
    assert _parser().parse_args([]).table is None


def test_unknown_table_is_rejected() -> None:
    with pytest.raises(SystemExit):
        _parser().parse_args(["--table", "raw_orders"])
