"""reconcile job: name and CLI (offline, W3.D18)."""

from __future__ import annotations

import argparse

import pytest

from lakehouse.quality.jobs.reconcile import JOB


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="reconcile")
    JOB().add_arguments(parser)
    return parser.parse_args(list(argv))


def test_job_name_matches_module() -> None:
    assert JOB.name == "reconcile"


def test_namespace_is_optional_and_defaults_to_none() -> None:
    assert _parse().namespace is None
    assert _parse("--namespace", "it_12345678").namespace == "it_12345678"


def test_unknown_arguments_are_rejected() -> None:
    with pytest.raises(SystemExit):
        _parse("--no-such-flag")
