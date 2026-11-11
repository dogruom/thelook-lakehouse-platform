"""Log-safe tenant key (``lakehouse.common.brand_key``): contract values pass, the rest is masked."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest


pytest.importorskip("pyspark")

from lakehouse.common.brand_key import INVALID_BRAND_KEY, NULL_BRAND_KEY, brand_key  # noqa: E402


if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_brand_key_masks_null_and_invalid_values(spark: SparkSession) -> None:
    values = [
        "brand_alpha",
        None,
        "Brand_alpha",
        "brand_x\n",  # Java ``$`` would accept a trailing newline; ``\z`` must not
        "jane.doe@example.com",
        "",
    ]
    df = spark.createDataFrame([(v,) for v in values], "brand_id string")
    got = [r["k"] for r in df.select(brand_key().alias("k")).collect()]
    assert got == [
        "brand_alpha",
        NULL_BRAND_KEY,
        INVALID_BRAND_KEY,
        INVALID_BRAND_KEY,
        INVALID_BRAND_KEY,
        INVALID_BRAND_KEY,
    ]


def test_brand_key_reads_another_column(spark: SparkSession) -> None:
    df = spark.createDataFrame([("brand_beta",)], "other string")
    assert df.select(brand_key("other").alias("k")).collect()[0]["k"] == "brand_beta"
