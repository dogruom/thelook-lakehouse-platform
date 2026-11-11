"""Rule definitions as data: stable codes, categories, retry flags, registry shape (no Spark)."""

from __future__ import annotations

import re

import pytest

from lakehouse.quality import rules
from lakehouse.quality.dlq import DLQ_TABLE_BY_BRONZE
from lakehouse.quality.rules import ErrorCategory, Rule
from tests.unit.quality import fake_spark


TABLES = ["raw_customers", "raw_clickstream", "raw_orders", "raw_products"]


@pytest.fixture
def spark_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_spark.install(monkeypatch)


def test_registry_covers_exactly_the_tables_that_have_a_dlq_table() -> None:
    assert sorted(rules.RULES_BY_TABLE) == sorted(TABLES)
    assert set(rules.RULES_BY_TABLE) == set(DLQ_TABLE_BY_BRONZE)


@pytest.mark.parametrize("table", TABLES)
def test_every_entry_builds_a_non_empty_list_of_rules(table: str) -> None:
    built = rules.RULES_BY_TABLE[table]()
    assert built
    assert all(isinstance(r, Rule) for r in built)


@pytest.mark.parametrize("table", TABLES)
def test_codes_are_unique_lower_snake_and_never_embed_values(table: str) -> None:
    codes = [r.code for r in rules.RULES_BY_TABLE[table]()]
    assert len(codes) == len(set(codes)), sorted({c for c in codes if codes.count(c) > 1})
    assert all(re.fullmatch(r"[a-z][a-z0-9_]*", c) for c in codes)


@pytest.mark.parametrize("table", TABLES)
def test_categories_are_known_and_gate_rules_are_not_retryable(table: str) -> None:
    for r in rules.RULES_BY_TABLE[table]():
        assert isinstance(r.category, ErrorCategory), r.code
        # FK orphans are the only retryable outcome (D-077) and are decided in Silver, not here.
        assert r.retryable is False, r.code
        assert r.category is not rules.FK_ERROR_CATEGORY, r.code


def test_fk_outcome_constants_are_distinct_and_not_gate_codes() -> None:
    assert rules.CUSTOMER_FK_MISSING != rules.PRODUCT_FK_MISSING
    all_codes = {r.code for t in TABLES for r in rules.RULES_BY_TABLE[t]()}
    assert not {rules.CUSTOMER_FK_MISSING, rules.PRODUCT_FK_MISSING} & all_codes
    assert rules.FK_ERROR_CATEGORY is ErrorCategory.REFERENTIAL_INTEGRITY_VIOLATION


def test_error_category_values_match_their_names() -> None:
    assert all(c.value == c.name for c in ErrorCategory)


def test_order_rules_contain_the_amount_and_item_checks() -> None:
    codes = {r.code for r in rules.order_rules()}
    assert {
        "gross_amount_not_positive",
        "discount_amount_negative",
        "net_amount_mismatch",
        "order_timestamp_in_future",
    } <= codes
    assert len(codes) > len(rules.product_rules())


def test_signup_after_produced_is_a_business_rule_on_customers() -> None:
    by_code = {r.code: r for r in rules.cdc_rules()}
    assert by_code["signup_after_produced"].category is ErrorCategory.BUSINESS_RULE_VIOLATION


def test_known_divergences_are_documented_text() -> None:
    assert len(rules.KNOWN_DIVERGENCES) >= 3
    assert all(isinstance(s, str) and s for s in rules.KNOWN_DIVERGENCES)


# ---- expression builders run without raising (stubbed Spark) ------------------------------------


@pytest.mark.parametrize("table", TABLES)
def test_every_failed_factory_builds_a_column_expression(table: str, spark_stub: None) -> None:
    for r in rules.RULES_BY_TABLE[table]():
        assert r.failed() is not None, r.code


def test_apply_rules_rejects_an_empty_rule_list(spark_stub: None) -> None:
    with pytest.raises(ValueError, match="at least one rule"):
        rules.apply_rules(fake_spark.Perm("df"), [])  # type: ignore[arg-type]


@pytest.mark.parametrize("table", TABLES)
def test_apply_rules_builds_the_four_output_columns(table: str, spark_stub: None) -> None:
    out = rules.apply_rules(fake_spark.Perm("df"), rules.RULES_BY_TABLE[table]())  # type: ignore[arg-type]
    assert out is not None


def test_helper_columns_build(spark_stub: None) -> None:
    col = fake_spark.Perm("c")
    assert rules._canonical_ts(col) is not None  # type: ignore[arg-type]
    assert rules._java("^a$").endswith(r"\z")
    with pytest.raises(ValueError, match="must end with"):
        rules._java(r"a\z")
