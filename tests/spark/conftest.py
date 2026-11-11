"""Shared local[2] SparkSession for Spark tests (runs inside the Spark image only)."""

from __future__ import annotations

import contextlib
import gc
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
        extra_conf={
            "spark.ui.enabled": "false",
            # Whole-suite runs degraded at the 1g default (Delta log and plan caches pile up).
            # The in-process builder starts the JVM, so this setting takes effect here.
            "spark.driver.memory": "3g",
            # Tiny test data: fewer partitions/tasks per Delta log replay and shuffle.
            "spark.databricks.delta.snapshotPartitions": "2",
            "spark.sql.shuffle.partitions": "1",
            "spark.default.parallelism": "1",
            "spark.sql.adaptive.enabled": "false",
        },
    )
    yield session
    session.stop()


@pytest.fixture(scope="module", autouse=True)
def _release_spark_caches() -> Iterator[None]:
    """Drop cached data and Delta log snapshots after every test module (the session is shared)."""
    yield
    try:
        from pyspark.sql import SparkSession

        active = SparkSession.getActiveSession()
        if active is not None:
            active.catalog.clearCache()
            with contextlib.suppress(Exception):  # internal Delta API, best effort
                active._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()  # type: ignore[union-attr]
    except ImportError:
        pass
    gc.collect()
