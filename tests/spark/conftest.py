"""Shared local[2] SparkSession for Spark tests (runs inside the Spark image only)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    pytest.importorskip("pyspark")
    from lakehouse.common.spark import SparkSettings, get_spark_session, new_run_id

    session = get_spark_session(
        SparkSettings(master="local[2]"),
        app_name="spark-tests",
        run_id=new_run_id("spark_tests"),
        extra_conf={"spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"},
    )
    yield session
    session.stop()
