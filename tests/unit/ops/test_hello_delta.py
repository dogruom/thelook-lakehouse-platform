"""Pure unit tests of the hello_delta job helpers (no pyspark)."""

from __future__ import annotations

import argparse

import pytest

from lakehouse.ops.jobs.hello_delta import JOB, assert_packaged


ZIP_ORIGIN = "/opt/spark/work/app-1/lakehouse_app-0123456789ab.zip/lakehouse/__init__.py"
SRC_ORIGIN = "/opt/spark-apps/src/lakehouse/__init__.py"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    JOB().add_arguments(parser)
    return parser


def test_assert_packaged_accepts_zip_origins() -> None:
    assert_packaged([ZIP_ORIGIN, ZIP_ORIGIN])


def test_assert_packaged_rejects_source_mount_origin() -> None:
    with pytest.raises(RuntimeError, match="code not loaded from lakehouse_app-"):
        assert_packaged([SRC_ORIGIN])


def test_assert_packaged_rejects_mixed_origins() -> None:
    with pytest.raises(RuntimeError):
        assert_packaged([ZIP_ORIGIN, SRC_ORIGIN])


def test_assert_packaged_rejects_empty() -> None:
    with pytest.raises(RuntimeError, match="no code origins"):
        assert_packaged([])


def test_rows_zero_is_rejected() -> None:
    with pytest.raises(SystemExit):
        _parser().parse_args(["--rows", "0"])


def test_defaults() -> None:
    args = _parser().parse_args([])
    assert (args.rows, args.path, args.expect_packaged) == (5, None, False)
