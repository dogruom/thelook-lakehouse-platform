"""Session factory, Delta round trip and the hello_delta job on local[2]."""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession


def test_session_carries_delta_utc_and_run_id_conf(spark: SparkSession) -> None:
    assert spark.conf.get("spark.sql.extensions") == "io.delta.sql.DeltaSparkSessionExtension"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    run_id = spark.conf.get("spark.lakehouse.run_id")
    assert run_id.startswith("spark_tests_")
    assert spark.sparkContext.getLocalProperty("lakehouse.run_id") == run_id


def test_python_minor_version_is_identical_on_driver_and_executors(spark: SparkSession) -> None:
    """Runtime parity: a mismatch makes UDFs/RDD lambdas fail only on the cluster."""
    driver = sys.version_info[:2]
    executors = set(
        spark.sparkContext.parallelize(range(2), 2)
        .map(lambda _: __import__("sys").version_info[:2])
        .collect()
    )
    assert driver == (3, 11)
    assert executors == {driver}


def test_delta_write_read_round_trip(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "t")
    spark.range(5).write.format("delta").save(path)
    assert spark.read.format("delta").load(path).count() == 5


def test_delta_errorifexists_rejects_second_write(spark: SparkSession, tmp_path: Path) -> None:
    """Negative: the Delta format is really in use and refuses to overwrite silently."""
    path = str(tmp_path / "t")
    spark.range(1).write.format("delta").save(path)
    with pytest.raises(Exception, match="(?i)exist"):
        spark.range(1).write.format("delta").mode("errorifexists").save(path)


def test_hello_delta_job_writes_and_reads_matching_counts(
    spark: SparkSession, tmp_path: Path
) -> None:
    from lakehouse.common.jobs import JobContext
    from lakehouse.common.spark import SparkSettings, new_run_id
    from lakehouse.ops.jobs.hello_delta import JOB

    job = JOB()
    parser = argparse.ArgumentParser()
    job.add_arguments(parser)
    path = str(tmp_path / "hello")
    args = parser.parse_args(["--rows", "7", "--path", path])
    settings = SparkSettings(master="local[2]")
    ctx = JobContext("hello_delta", new_run_id("hello_delta"), settings)

    metrics = job.run(spark, args, ctx)

    assert metrics["rows_written"] == 7
    assert metrics["rows_read"] == 7
    assert metrics["delta_version"] == 0
    assert metrics["path"] == path
    ids = {
        r[0] for r in spark.read.format("delta").load(path).select("run_id").distinct().collect()
    }
    assert ids == {ctx.run_id}
