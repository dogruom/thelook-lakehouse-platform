"""isolation_check job: name and CLI (offline, W3.D18)."""

from __future__ import annotations

import argparse
import json
from typing import Any

import pytest

from lakehouse.quality.jobs.isolation_check import JOB


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="isolation_check")
    JOB().add_arguments(parser)
    return parser.parse_args(list(argv))


def _expected(args: argparse.Namespace) -> Any:
    value = args.expected_json
    return json.loads(value) if isinstance(value, str) else value


def test_job_name_matches_module() -> None:
    assert JOB.name == "isolation_check"


def test_expected_json_is_accepted_and_round_trips() -> None:
    want = {"orders": {"brand_alpha": 3, "brand_beta": 2}}
    args = _parse("--namespace", "it_1", "--expected-json", json.dumps(want))
    assert args.namespace == "it_1"
    assert _expected(args) == want


def test_malformed_expected_json_is_a_usage_error_or_value_error() -> None:
    with pytest.raises((SystemExit, ValueError)):
        _expected(_parse("--expected-json", "{not json"))
