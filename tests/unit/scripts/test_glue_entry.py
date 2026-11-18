"""``scripts/cloud/glue_entry.py``: Glue argument parsing and the job hand-off (W5.D27)."""

from __future__ import annotations

import json
import os
import sys
import types
from typing import TYPE_CHECKING

import pytest

from .conftest import load_script


if TYPE_CHECKING:
    from types import ModuleType


@pytest.fixture(scope="module")
def entry() -> ModuleType:
    return load_script("cloud/glue_entry")


GLUE_ARGV = [
    "--JOB_NAME",
    "crm-lakehouse-dev-silver_orders",
    "--conf",
    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--lakehouse_job",
    "silver_orders",
    "--lakehouse_bucket",
    "crm-lakehouse-dev-123456789012",
    "--TempDir",
    "s3://x/glue-temp/",
]


def test_parse_ignores_glue_reserved_arguments(entry: ModuleType) -> None:
    job, bucket, args = entry.parse_glue_argv(GLUE_ARGV)
    assert (job, bucket, args) == ("silver_orders", "crm-lakehouse-dev-123456789012", [])


def test_parse_json_args_and_equals_form(entry: ModuleType) -> None:
    argv = [
        "--lakehouse_job=dlq_gate",
        "--lakehouse_bucket=lake-bkt",
        "--lakehouse_args",
        json.dumps(["--table", "raw_orders"]),
    ]
    assert entry.parse_glue_argv(argv) == ("dlq_gate", "lake-bkt", ["--table", "raw_orders"])


@pytest.mark.parametrize(
    "job", ["Silver", "1abc", "a", "has-dash", "x" * 64, "silver orders", "../etc", ""]
)
def test_parse_rejects_bad_job_names(entry: ModuleType, job: str) -> None:
    with pytest.raises(entry.GlueArgError):
        entry.parse_glue_argv(["--lakehouse_job", job, "--lakehouse_bucket", "lake-bkt"])


@pytest.mark.parametrize("raw", ["not json", '{"a": 1}', "[1, 2]", '["ok", null]', '"str"'])
def test_parse_rejects_non_string_list_args(entry: ModuleType, raw: str) -> None:
    argv = ["--lakehouse_job", "reconcile", "--lakehouse_bucket", "lake-bkt"]
    with pytest.raises(entry.GlueArgError, match="JSON list of strings"):
        entry.parse_glue_argv([*argv, "--lakehouse_args", raw])


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["--lakehouse_job", "reconcile"],
        ["--lakehouse_bucket", "lake-bkt"],
        ["--lakehouse_job", "reconcile", "--lakehouse_bucket", "BAD_BUCKET"],
        ["--lakehouse_job"],
        ["--lakehouse_job", "a_b", "--lakehouse_job", "c_d", "--lakehouse_bucket", "lake-bkt"],
    ],
)
def test_parse_requires_job_and_bucket(entry: ModuleType, argv: list[str]) -> None:
    with pytest.raises(entry.GlueArgError):
        entry.parse_glue_argv(argv)


def test_main_sets_env_and_returns_the_job_exit_code(
    entry: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    def fake_main(argv: list[str]) -> int:
        calls.append(argv)
        return 1

    fake = types.ModuleType("lakehouse.common.jobs")
    fake.main = fake_main  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "lakehouse.common.jobs", fake)
    for key in ("LAKEHOUSE_ENV", "LAKEHOUSE_CATALOG", "LAKEHOUSE_BUCKET"):
        monkeypatch.setenv(key, "placeholder")  # restored by monkeypatch after the test
    argv = [*GLUE_ARGV, "--lakehouse_args", '["--table","raw_orders"]']
    assert entry.main(argv) == 1  # non-zero => Glue marks the run FAILED
    assert calls == [["silver_orders", "--table", "raw_orders"]]
    assert os.environ["LAKEHOUSE_ENV"] == "aws"
    assert os.environ["LAKEHOUSE_CATALOG"] == "glue"
    assert os.environ["LAKEHOUSE_BUCKET"] == "crm-lakehouse-dev-123456789012"


def test_main_usage_error_exits_2_without_running_a_job(
    entry: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    assert entry.main(["--lakehouse_job", "BAD"]) == 2
    assert "glue_entry:" in capsys.readouterr().err
