"""content_hash job: name and CLI (offline, W3.D18)."""

from __future__ import annotations

import argparse

import pytest

from lakehouse.ops.jobs.content_hash import JOB


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="content_hash")
    JOB().add_arguments(parser)
    return parser.parse_args(list(argv))


def test_job_name_matches_module() -> None:
    assert JOB.name == "content_hash"


def test_table_is_repeatable() -> None:
    args = _parse("--namespace", "it_1", "--table", "orders", "--table", "customers")
    assert args.namespace == "it_1"
    assert list(args.table) == ["orders", "customers"]


def test_unknown_arguments_are_rejected() -> None:
    with pytest.raises(SystemExit):
        _parse("--no-such-flag")
