"""bronze_stream / bronze_report job wiring that must work on the host without PySpark."""

from __future__ import annotations

import argparse
import importlib
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from lakehouse.common.jobs import resolve_job
from lakehouse.common.spark import SparkSettings


REPO_ROOT = Path(__file__).resolve().parents[3]
STREAM_MODULE = "lakehouse.ingestion.jobs.bronze_stream"

_BLOCK_PYSPARK = """
import importlib, sys

class _Block:
    def find_spec(self, name, path=None, target=None):
        if name == "pyspark" or name.startswith("pyspark."):
            raise ImportError("pyspark is blocked in this test")
        return None

sys.meta_path.insert(0, _Block())
for mod in list(sys.modules):
    if mod == "pyspark" or mod.startswith("pyspark."):
        del sys.modules[mod]
module = importlib.import_module({module!r})
assert module.JOB.name == {name!r}
"""


def _run_blocked(module: str, name: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", _BLOCK_PYSPARK.format(module=module, name=name)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _parse(job_name: str, *argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=job_name)
    resolve_job(job_name)().add_arguments(parser)
    return parser.parse_args(list(argv))


@pytest.mark.parametrize(
    ("module", "name"),
    [(STREAM_MODULE, "bronze_stream"), ("lakehouse.ops.jobs.bronze_report", "bronze_report")],
)
def test_job_module_imports_without_pyspark(module: str, name: str) -> None:
    result = _run_blocked(module, name)
    assert result.returncode == 0, result.stderr[-1500:]


def test_pyspark_blocker_really_makes_a_pyspark_import_fail() -> None:
    """Negative: the blocker is effective, so the import tests above can fail."""
    result = _run_blocked("pyspark.sql", "x")
    assert result.returncode != 0
    assert "pyspark is blocked" in result.stderr


@pytest.mark.parametrize("name", ["bronze_stream", "bronze_report"])
def test_resolve_job_finds_the_job_and_its_name_matches(name: str) -> None:
    assert resolve_job(name).name == name


def test_stream_module_job_name_attribute() -> None:
    assert importlib.import_module(STREAM_MODULE).JOB.name == "bronze_stream"


def test_stream_defaults_use_the_contract_topics_and_a_positive_trigger_limit() -> None:
    args = _parse("bronze_stream", "--namespace", "ns1")
    assert args.namespace == "ns1"
    assert args.cdc_topic == "customer_cdc"
    assert args.clickstream_topic == "clickstream_events"
    assert isinstance(args.max_offsets_per_trigger, int) and args.max_offsets_per_trigger > 0


def test_stream_accepts_explicit_topics_and_limit() -> None:
    args = _parse(
        "bronze_stream",
        "--namespace",
        "ns1",
        "--cdc-topic",
        "c",
        "--clickstream-topic",
        "k",
        "--max-offsets-per-trigger",
        "100",
    )
    assert (args.cdc_topic, args.clickstream_topic, args.max_offsets_per_trigger) == ("c", "k", 100)


@pytest.mark.parametrize("value", ["0", "-1", "abc"])
def test_stream_rejects_non_positive_max_offsets_per_trigger(value: str) -> None:
    with pytest.raises(SystemExit):
        _parse("bronze_stream", "--namespace", "ns1", "--max-offsets-per-trigger", value)


def test_report_accepts_a_namespace() -> None:
    assert _parse("bronze_report", "--namespace", "ns1").namespace == "ns1"


# ---- SparkSettings.kafka_bootstrap_servers ---------------------------------------------------


def test_kafka_bootstrap_servers_default_is_the_in_network_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LAKEHOUSE_KAFKA_BOOTSTRAP", raising=False)
    assert SparkSettings().kafka_bootstrap_servers == "kafka:9092"


def test_kafka_bootstrap_servers_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAKEHOUSE_KAFKA_BOOTSTRAP", "broker.example:19092")
    assert SparkSettings().kafka_bootstrap_servers == "broker.example:19092"


def test_kafka_bootstrap_servers_kwarg_is_accepted() -> None:
    kwargs: dict[str, Any] = {"kafka_bootstrap_servers": "k:1"}
    assert SparkSettings(**kwargs).kafka_bootstrap_servers == "k:1"
