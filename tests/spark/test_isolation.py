"""Tenant isolation helpers (W3.D18, D-085), from the design, on crafted id collisions.

``brand_counts(df)`` counts rows per ``brand_id``. ``assert_no_cross_tenant_rows(df, left_brand,
right_brand)`` raises ``CrossTenantLeakError`` when a joined frame pairs a ``left_brand`` row with
a ``right_brand`` row. The joined frame carries the left side's ``brand_id`` and the right side's
brand as ``right_brand_id`` (assumption: the exact column naming is not fixed by the design).
Runs inside the Spark image.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest


pytest.importorskip("pyspark")

from lakehouse.quality.isolation import (  # noqa: E402
    CrossTenantLeakError,
    assert_no_cross_tenant_rows,
    brand_counts,
)


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

A, B = "brand_alpha", "brand_beta"
SHARED = "c-shared"  # same customer id in both brands (cross_tenant_collision)


def _customers(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [(A, SHARED, "Ada"), (B, SHARED, "Bob"), (A, "c-a", "Al"), (B, "c-b", "Bo")],
        "brand_id string, customer_id string, name string",
    )


def _orders(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [(A, "o1", SHARED), (B, "o2", SHARED), (A, "o3", "c-a")],
        "brand_id string, order_id string, customer_id string",
    )


def leaky_join(spark: SparkSession) -> DataFrame:
    """Negative fixture: orders joined to customers on customer_id only."""
    o, c = _orders(spark), _customers(spark)
    return o.join(c, o["customer_id"] == c["customer_id"]).select(
        o["brand_id"], o["order_id"], c["brand_id"].alias("right_brand_id")
    )


def composite_join(spark: SparkSession) -> DataFrame:
    o, c = _orders(spark), _customers(spark)
    return o.join(
        c, (o["brand_id"] == c["brand_id"]) & (o["customer_id"] == c["customer_id"])
    ).select(o["brand_id"], o["order_id"], c["brand_id"].alias("right_brand_id"))


def test_brand_counts_returns_exact_rows_per_brand(spark: SparkSession) -> None:
    assert dict(brand_counts(_customers(spark))) == {A: 2, B: 2}
    assert dict(brand_counts(_orders(spark))) == {A: 2, B: 1}


def test_leaky_join_produces_cross_tenant_rows_and_is_detected(spark: SparkSession) -> None:
    leaky = leaky_join(spark)
    assert leaky.filter("brand_id <> right_brand_id").count() == 2  # the collision pairs
    with pytest.raises(CrossTenantLeakError):
        assert_no_cross_tenant_rows(leaky, A, B)


def test_composite_join_passes_the_check(spark: SparkSession) -> None:
    joined = composite_join(spark)
    assert joined.count() == 3  # every order matches exactly its own tenant's customer
    assert_no_cross_tenant_rows(joined, A, B)


def test_single_brand_frames_pass_and_empty_frames_pass(spark: SparkSession) -> None:
    only_a = _customers(spark).filter(f"brand_id = '{A}'")
    assert_no_cross_tenant_rows(only_a.withColumn("right_brand_id", only_a["brand_id"]), A, B)
    empty = leaky_join(spark).filter("1 = 0")
    assert_no_cross_tenant_rows(empty, A, B)


def test_leak_error_is_an_exception_that_names_the_brands(spark: SparkSession) -> None:
    with pytest.raises(CrossTenantLeakError) as info:
        assert_no_cross_tenant_rows(leaky_join(spark), A, B)
    assert isinstance(info.value, Exception)
    assert A in str(info.value) and B in str(info.value)


# ---- isolation_check's self-join over one customers frame (regression: ambiguous self-join) ----


def test_job_self_join_on_one_frame_resolves_and_finds_the_collision(spark: SparkSession) -> None:
    from lakehouse.quality.jobs.isolation_check import _self_join

    customers = _customers(spark)
    composite = _self_join(customers, composite=True)
    assert composite.columns == ["brand_id", "right_brand_id"]
    assert composite.count() == 4  # every row matches only itself
    assert_no_cross_tenant_rows(composite, A, B)

    by_id_only = _self_join(customers, composite=False)
    assert by_id_only.filter("NOT (brand_id <=> right_brand_id)").count() == 2  # A<->B on SHARED
    with pytest.raises(CrossTenantLeakError):
        assert_no_cross_tenant_rows(by_id_only, A, B)
