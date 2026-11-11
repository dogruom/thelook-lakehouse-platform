"""``tenant_view_sql`` (W3.D18, D-085): exact DDL and identifier validation, offline."""

from __future__ import annotations

import pytest

from lakehouse.silver.views import tenant_view_sql


def test_named_view_selects_one_brand_from_the_silver_table() -> None:
    assert tenant_view_sql("orders", "brand_alpha", temp=False) == (
        "CREATE OR REPLACE VIEW silver.v_orders__brand_alpha AS "
        "SELECT * FROM silver.orders WHERE brand_id = 'brand_alpha'"
    )


def test_temp_view_has_no_database_prefix_on_the_view_name() -> None:
    sql = tenant_view_sql("order_items", "brand_beta", temp=True)
    assert sql == (
        "CREATE OR REPLACE TEMP VIEW v_order_items__brand_beta AS "
        "SELECT * FROM silver.order_items WHERE brand_id = 'brand_beta'"
    )


def test_view_filters_exactly_one_brand_literal() -> None:
    sql = tenant_view_sql("customers", "brand_alpha", temp=False)
    assert sql.count("brand_id =") == 1 and "brand_beta" not in sql and " OR " not in sql


@pytest.mark.parametrize(
    "table",
    ["orders; DROP TABLE x", "Orders", "o rders", "", "a.b", "orders--", "../x", "ord'ers"],
)
def test_invalid_table_names_are_rejected(table: str) -> None:
    with pytest.raises(ValueError):
        tenant_view_sql(table, "brand_alpha", temp=False)


@pytest.mark.parametrize(
    "brand",
    ["brand' OR '1'='1", "brand_alpha'; --", "Brand", "", "a b", "brand-alpha", "a" * 200, "x\n"],
)
def test_invalid_brand_values_are_rejected_so_no_injection_reaches_the_sql(brand: str) -> None:
    with pytest.raises(ValueError):
        tenant_view_sql("orders", brand, temp=True)


def test_different_brands_give_different_views_and_filters() -> None:
    a = tenant_view_sql("orders", "brand_alpha", temp=False)
    b = tenant_view_sql("orders", "brand_beta", temp=False)
    assert a != b and "brand_alpha" not in b
