"""Contract rules of the DLQ gate for file entities (W3.D17, D-077), derived from DATA_CONTRACTS
sections 2.2-2.4 and the approved design, not from the rule implementation.

Cases are landed lines run through the production ``to_bronze_rows`` of the batch ingest and then
``apply_rules``. Cases marked ``subset`` may legitimately co-fire further codes (the design does
not fix which); every other case must fire exactly its codes, in rule order.

Runs inside the Spark image (``docker compose --profile tools run --rm spark-test``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pytest


pytest.importorskip("pyspark")

from lakehouse.ingestion.batch import ORDERS, PRODUCTS  # noqa: E402
from lakehouse.quality import rules as rules_mod  # noqa: E402
from lakehouse.quality.rules import (  # noqa: E402
    RULES_BY_TABLE,
    ErrorCategory,
    Rule,
    apply_rules,
    order_rules,
    product_rules,
)

from .order_data import (  # noqa: E402
    ABSENT,
    ALPHA,
    BETA,
    FUTURE_TS,
    O1,
    bronze_files,
    item,
    order,
    order_path,
    product,
    product_path,
    uid,
)


if TYPE_CHECKING:
    from pyspark.sql import Row, SparkSession


ENVELOPE = [
    "brand_id_null_or_wrong_type",
    "brand_id_invalid",
    "payload_brand_id_null_or_wrong_type",
    "payload_brand_id_invalid",
    "brand_path_mismatch",
    "schema_version_invalid",
]
# Relative rule order that the design fixes: envelope, order fields, money, business, timestamp,
# items, optional-field wrong types. (The full code list is the implementation's; only the
# relative order of these codes is asserted.)
ORDER_FIELD_CODES = [
    *ENVELOPE,
    "order_id_null_or_wrong_type",
    "order_id_invalid",
    "customer_id_null_or_wrong_type",
    "customer_id_invalid",
    "order_timestamp_null_or_wrong_type",
    "order_timestamp_invalid",
    "order_status_null_or_wrong_type",
    "order_status_invalid",
    "payment_method_null_or_wrong_type",
    "payment_method_invalid",
    "currency_null_or_wrong_type",
    "currency_invalid",
    "gross_amount_null_or_wrong_type",
    "gross_amount_invalid",
    "discount_amount_null_or_wrong_type",
    "discount_amount_invalid",
    "net_amount_null_or_wrong_type",
    "net_amount_invalid",
]
ORDER_ITEM_CODES = [
    "items_null_or_wrong_type",
    "items_count_invalid",
    "item_null_or_wrong_type",
    "item_invalid",
    "item_parent_mismatch",
    "item_line_total_mismatch",
    "item_id_duplicate",
]
BUSINESS = ["gross_amount_not_positive", "discount_amount_negative", "net_amount_mismatch"]
FUTURE = "order_timestamp_in_future"
PRODUCT_CODES = [
    *ENVELOPE,
    "product_id_null_or_wrong_type",
    "product_id_invalid",
    "product_name_null_or_wrong_type",
    "product_name_invalid",
    "category_null_or_wrong_type",
    "category_invalid",
    "retail_price_null_or_wrong_type",
    "retail_price_invalid",
]


@dataclass(frozen=True)
class Case:
    cid: str
    line: dict[str, Any] | str
    expected: list[str]
    path: str | None = None  # default: a well-formed landing path of ``brand``
    brand: str = ALPHA
    subset: bool = False
    tags: list[str] = field(default_factory=list)


def oc(cid: str, expected: list[str], *, subset: bool = False, **kw: Any) -> Case:
    """Order case; ``kw`` overrides order fields (use ``ABSENT`` to drop one)."""
    path = kw.pop("path", None)
    brand = kw.pop("brand", ALPHA)
    items = kw.pop("items_value", None)
    payload = order(**kw)
    if items is not None:
        payload["items"] = items
    return Case(cid, payload, expected, path, brand, subset)


def pc(cid: str, expected: list[str], *, subset: bool = False, **kw: Any) -> Case:
    path = kw.pop("path", None)
    brand = kw.pop("brand", ALPHA)
    return Case(cid, product(**kw), expected, path, brand, subset)


def _items(*overs: dict[str, Any]) -> list[dict[str, Any]]:
    """Items of ``O1``; each dict overrides the defaults of one item."""
    out = []
    for n, over in enumerate(overs, start=1):
        base = item(n, O1)
        base.update(over)
        out.append(base)
    return out


ORDER_CASES: list[Case] = [
    oc("valid", []),
    oc("valid_without_sales_channel", [], sales_channel=ABSENT),
    oc("valid_sales_channel_null", [], sales_channel=None),
    oc("valid_sales_channel_app", [], sales_channel="app"),
    oc("valid_zero_discount", [], discount_amount="0.00", net_amount="25.50"),
    oc(
        "valid_max_money_boundary",
        [],
        gross_amount="99999999.99",
        discount_amount="0.00",
        net_amount="99999999.99",
        items_value=_items(
            {"quantity": 1, "unit_price": "99999999.99", "line_total": "99999999.99"}
        ),
    ),
    oc(
        "valid_five_items",
        [],
        gross_amount="100.00",
        discount_amount="0.00",
        net_amount="100.00",
        items_value=_items(*({"quantity": 1, "unit_price": "20.00", "line_total": "20.00"},) * 5),
    ),
    # -- envelope -----------------------------------------------------------------------------
    oc("payload_brand_null", ["payload_brand_id_null_or_wrong_type"], brand_id=None, subset=True),
    oc(
        "payload_brand_absent",
        ["payload_brand_id_null_or_wrong_type"],
        brand_id=ABSENT,
        subset=True,
    ),
    oc(
        "payload_brand_uppercase",
        ["payload_brand_id_invalid", "brand_path_mismatch"],
        brand_id="Brand_Alpha",
        subset=True,
    ),
    # The payload claims BETA while its items still name ALPHA: both the tenant check and the
    # item/parent consistency check fire (rule order: envelope first).
    oc(
        "payload_brand_other_tenant",
        ["brand_path_mismatch", "item_parent_mismatch"],
        brand_id=BETA,
    ),
    oc(
        "path_brand_unknown_layout",
        ["brand_id_null_or_wrong_type"],
        path="landing/orders-only.ndjson",
        subset=True,
    ),
    oc("schema_version_2", ["schema_version_invalid"], schema_version=2),
    oc("schema_version_absent", ["schema_version_invalid"], schema_version=ABSENT),
    # -- fields -------------------------------------------------------------------------------
    oc("order_id_null", ["order_id_null_or_wrong_type"], order_id=None, subset=True),
    oc("order_id_not_uuid", ["order_id_invalid"], order_id="not-a-uuid", subset=True),
    oc("order_id_uppercase", ["order_id_invalid"], order_id=uid(0xAB).upper(), subset=True),
    oc("customer_id_null", ["customer_id_null_or_wrong_type"], customer_id=None),
    oc("customer_id_invalid", ["customer_id_invalid"], customer_id="C-1"),
    oc(
        "order_timestamp_null",
        ["order_timestamp_null_or_wrong_type"],
        order_timestamp=None,
        subset=True,
    ),
    oc(
        "order_timestamp_naive",
        ["order_timestamp_invalid"],
        order_timestamp="2026-03-01T10:00:00",
        subset=True,
    ),
    oc(
        "order_timestamp_garbage",
        ["order_timestamp_invalid"],
        order_timestamp="yesterday",
        subset=True,
    ),
    oc("status_unknown", ["order_status_invalid"], order_status="shipped"),
    oc("status_null", ["order_status_null_or_wrong_type"], order_status=None),
    oc("payment_unknown", ["payment_method_invalid"], payment_method="bitcoin"),
    oc("currency_unknown", ["currency_invalid"], currency="GBP"),
    oc("currency_lowercase", ["currency_invalid"], currency="eur"),
    # -- money --------------------------------------------------------------------------------
    oc("gross_one_decimal", ["gross_amount_invalid"], gross_amount="25.5", subset=True),
    oc("gross_three_decimals", ["gross_amount_invalid"], gross_amount="25.500", subset=True),
    oc("gross_scientific", ["gross_amount_invalid"], gross_amount="2.55e1", subset=True),
    oc("gross_null", ["gross_amount_null_or_wrong_type"], gross_amount=None, subset=True),
    oc(
        "gross_zero",
        ["gross_amount_not_positive", "net_amount_mismatch"],
        gross_amount="0.00",
        subset=True,
    ),
    oc(
        "gross_negative_consistent_net",
        ["gross_amount_not_positive"],
        gross_amount="-25.50",
        net_amount="-28.00",
    ),
    oc(
        "gross_nine_integer_digits",
        ["gross_amount_invalid"],
        gross_amount="123456789.00",
        subset=True,
    ),
    oc(
        "discount_negative",
        ["discount_amount_negative"],
        discount_amount="-1.00",
        net_amount="26.50",
    ),
    oc("net_mismatch", ["net_amount_mismatch"], net_amount="23.01"),
    oc("net_invalid_pattern", ["net_amount_invalid"], net_amount="23", subset=True),
    # -- timestamp ----------------------------------------------------------------------------
    oc("order_in_the_future", [FUTURE], order_timestamp=FUTURE_TS),
    oc("order_in_the_past_is_fine", [], order_timestamp="2025-01-01T00:00:00Z"),
    # -- items --------------------------------------------------------------------------------
    oc("items_is_a_string", ["items_null_or_wrong_type"], items="nope", subset=True),
    oc("items_null", ["items_null_or_wrong_type"], items=None, subset=True),
    oc("items_empty", ["items_count_invalid"], items=[], subset=True),
    oc(
        "items_six",
        ["items_count_invalid"],
        gross_amount="120.00",
        discount_amount="0.00",
        net_amount="120.00",
        items_value=_items(*({"quantity": 1, "unit_price": "20.00", "line_total": "20.00"},) * 6),
        subset=True,
    ),
    oc("item_null_element", ["item_null_or_wrong_type"], items_value=[None], subset=True),
    oc(
        "item_quantity_wrong_type",
        ["item_null_or_wrong_type"],
        subset=True,
        items_value=_items({"quantity": "abc"}, {}),
    ),
    oc(
        "item_price_field_absent",
        ["item_null_or_wrong_type"],
        subset=True,
        items_value=[{k: v for k, v in item(1, O1).items() if k != "unit_price"}],
    ),
    oc(
        "item_quantity_zero",
        ["item_invalid"],
        subset=True,
        items_value=_items({"quantity": 0, "line_total": "0.00"}),
    ),
    oc(
        "item_product_pattern",
        ["item_invalid"],
        subset=True,
        items_value=_items({"product_id": "X-1"}),
    ),
    oc(
        "item_price_pattern",
        ["item_invalid"],
        subset=True,
        items_value=_items({"unit_price": "10.0"}),
    ),
    oc(
        "item_price_zero",
        ["item_invalid"],
        subset=True,
        items_value=_items({"unit_price": "0.00", "line_total": "0.00"}),
    ),
    oc("item_other_order", ["item_parent_mismatch"], items_value=_items({"order_id": uid(0xEE)})),
    oc("item_other_brand", ["item_parent_mismatch"], items_value=_items({"brand_id": BETA})),
    oc(
        "item_line_total_wrong",
        ["item_line_total_mismatch"],
        items_value=_items({"line_total": "19.99"}),
    ),
    oc(
        "item_id_duplicate",
        ["item_id_duplicate"],
        items_value=[item(1, O1), {**item(2, O1), "item_id": item(1, O1)["item_id"]}],
    ),
    # -- V-20: optional fields that are present but of the wrong type -------------------------
    oc("sales_channel_unknown_enum", ["sales_channel_invalid"], sales_channel="kiosk"),
]

PRODUCT_CASES: list[Case] = [
    pc("valid", []),
    pc("valid_cost_price_null", [], cost_price=None),
    pc("valid_cost_price_absent", [], cost_price=ABSENT),
    pc("valid_inactive", [], is_active=False),
    pc("valid_name_255", [], product_name="n" * 255),
    pc("payload_brand_other_tenant", ["brand_path_mismatch"], brand_id=BETA),
    pc("schema_version_2", ["schema_version_invalid"], schema_version=2),
    pc("product_id_pattern", ["product_id_invalid"], product_id="SKU-1"),
    pc("product_id_null", ["product_id_null_or_wrong_type"], product_id=None, subset=True),
    pc("name_empty", ["product_name_invalid"], product_name=""),
    pc("name_256", ["product_name_invalid"], product_name="n" * 256),
    pc("category_unknown", ["category_invalid"], category="Toys"),
    pc("retail_zero", ["retail_price_invalid"], retail_price="0.00", subset=True),
    pc("retail_negative", ["retail_price_invalid"], retail_price="-1.00", subset=True),
    pc("retail_pattern", ["retail_price_invalid"], retail_price="49.9", subset=True),
    pc("cost_negative", ["cost_price_invalid"], cost_price="-1.00", subset=True),
    pc("cost_zero", ["cost_price_invalid"], cost_price="0.00", subset=True),
    pc("is_active_wrong_type", ["is_active_wrong_type"], is_active="yes", subset=True),
]


def _evaluate(
    spark: SparkSession, target: Any, rules: list[Rule], cases: list[Case], entity: str
) -> dict[str, Row]:
    files: dict[str, list[dict[str, Any] | str]] = {}
    path_of: dict[str, str] = {}
    for i, c in enumerate(cases, start=1):
        if c.path is not None:
            path = c.path
        elif entity == "orders":
            path = order_path(c.brand, i)
        else:
            path = product_path(c.brand, f"2026-03-{i:02d}")
        files[path] = [c.line]
        path_of[c.cid] = path
    out = apply_rules(bronze_files(spark, target, files), rules).collect()
    by_path = {str(r["_source_ref"]).rsplit("#L", 1)[0]: r for r in out}
    assert len(out) == len(cases)  # a row is never dropped
    return {c.cid: by_path[path_of[c.cid]] for c in cases}


@pytest.fixture(scope="module")
def order_results(spark: SparkSession) -> dict[str, Row]:
    assert len({c.cid for c in ORDER_CASES}) == len(ORDER_CASES), "duplicate case id"
    return _evaluate(spark, ORDERS, order_rules(), ORDER_CASES, "orders")


@pytest.fixture(scope="module")
def product_results(spark: SparkSession) -> dict[str, Row]:
    assert len({c.cid for c in PRODUCT_CASES}) == len(PRODUCT_CASES), "duplicate case id"
    return _evaluate(spark, PRODUCTS, product_rules(), PRODUCT_CASES, "products")


# ---- rule catalogue -------------------------------------------------------------------------


def _relative_order_holds(codes: list[str], wanted: list[str]) -> bool:
    missing = [c for c in wanted if c not in codes]
    assert not missing, f"missing rule codes: {missing}"
    positions = [codes.index(c) for c in wanted]
    return positions == sorted(positions)


def test_order_rules_contain_the_contract_codes_in_the_designed_order() -> None:
    codes = [r.code for r in order_rules()]
    assert len(set(codes)) == len(codes)
    assert _relative_order_holds(codes, ORDER_FIELD_CODES)
    assert _relative_order_holds(codes, ORDER_ITEM_CODES)
    for code in (*BUSINESS, FUTURE, "sales_channel_invalid"):
        assert code in codes, code


def test_product_rules_contain_the_contract_codes_in_the_designed_order() -> None:
    codes = [r.code for r in product_rules()]
    assert len(set(codes)) == len(codes)
    assert _relative_order_holds(codes, PRODUCT_CODES)
    assert "cost_price_invalid" in codes
    assert "is_active_wrong_type" in codes


def test_v20_optional_wrong_type_codes_exist_for_orders_and_products() -> None:
    assert "sales_channel_wrong_type" in [r.code for r in order_rules()]
    assert {"cost_price_wrong_type", "is_active_wrong_type"} <= {r.code for r in product_rules()}


def test_rules_by_table_includes_the_file_tables() -> None:
    assert set(RULES_BY_TABLE) == {"raw_customers", "raw_clickstream", "raw_orders", "raw_products"}
    assert [r.code for r in RULES_BY_TABLE["raw_orders"]()] == [r.code for r in order_rules()]
    assert [r.code for r in RULES_BY_TABLE["raw_products"]()] == [r.code for r in product_rules()]


def test_file_rules_are_never_retryable_and_have_the_contract_categories() -> None:
    for rule in [*order_rules(), *product_rules()]:
        assert rule.retryable is False, rule.code
        if rule.code in BUSINESS:
            assert rule.category == ErrorCategory.BUSINESS_RULE_VIOLATION, rule.code
        elif rule.code == FUTURE:
            assert rule.category == "TIMESTAMP_OUT_OF_BOUNDS", rule.code
        else:
            assert rule.category == ErrorCategory.SCHEMA_VIOLATION, rule.code


def test_foreign_key_constants_are_silver_only_and_not_gate_rules() -> None:
    gate_codes = {r.code for r in [*order_rules(), *product_rules()]}
    for name in ("CUSTOMER_FK_MISSING", "PRODUCT_FK_MISSING"):
        value = getattr(rules_mod, name)
        code = getattr(value, "code", value)
        assert code not in gate_codes, f"{name} must not be evaluated by the gate"


def test_every_designed_code_has_a_case_that_fires_it() -> None:
    fired = {code for c in ORDER_CASES for code in c.expected}
    wanted = {
        "payload_brand_id_null_or_wrong_type",
        "payload_brand_id_invalid",
        "brand_path_mismatch",
        "schema_version_invalid",
        *BUSINESS,
        FUTURE,
        *ORDER_ITEM_CODES,
        "sales_channel_invalid",
    }
    assert wanted - fired == set()
    assert any(c.expected == [] for c in ORDER_CASES)
    assert any(c.expected == [] for c in PRODUCT_CASES)


# ---- one positive / one negative per rule ---------------------------------------------------


def _check(case: Case, row: Row, codes_in_order: list[str]) -> None:
    got = list(row["_rejection_codes"])
    if case.subset:
        assert set(case.expected) <= set(got), (case.cid, got)
    else:
        assert got == case.expected, case.cid
    # The reported codes are always in rule order and the reason joins them.
    assert got == sorted(got, key=codes_in_order.index), case.cid
    if not got:
        assert row["_rejection_codes"] is not None  # [] and never NULL
        assert row["_error_category"] is None
        assert row["_rejection_reason"] is None
        assert row["_retryable"] is None
    else:
        assert row["_rejection_reason"] == ", ".join(got)
        assert row["_retryable"] is False


@pytest.mark.parametrize("case", ORDER_CASES, ids=lambda c: c.cid)
def test_order_case_fires_the_expected_codes(case: Case, order_results: dict[str, Row]) -> None:
    _check(case, order_results[case.cid], [r.code for r in order_rules()])


@pytest.mark.parametrize("case", PRODUCT_CASES, ids=lambda c: c.cid)
def test_product_case_fires_the_expected_codes(case: Case, product_results: dict[str, Row]) -> None:
    _check(case, product_results[case.cid], [r.code for r in product_rules()])


# ---- categories, parse errors, tenant ------------------------------------------------------


def test_future_order_is_a_timestamp_violation_and_negative_a_business_violation(
    order_results: dict[str, Row],
) -> None:
    future = order_results["order_in_the_future"]
    assert future["_error_category"] == "TIMESTAMP_OUT_OF_BOUNDS"
    negative = order_results["gross_negative_consistent_net"]
    assert negative["_error_category"] == "BUSINESS_RULE_VIOLATION"
    assert order_results["order_in_the_past_is_fine"]["_error_category"] is None


def test_wrong_brand_in_payload_is_rejected_even_when_everything_else_is_valid(
    order_results: dict[str, Row], product_results: dict[str, Row]
) -> None:
    """Tenant isolation: the path brand wins; a payload claiming another tenant never passes."""
    for row in (
        order_results["payload_brand_other_tenant"],
        product_results["payload_brand_other_tenant"],
    ):
        assert row["_rejection_codes"][0] == "brand_path_mismatch"
        assert row["brand_id"] == ALPHA  # Bronze brand is the path segment


def test_negative_control_a_valid_row_is_not_rejected(order_results: dict[str, Row]) -> None:
    """Guards against a rule set that rejects everything (every case above would pass then)."""
    assert list(order_results["valid"]["_rejection_codes"]) == []
    assert order_results["valid"]["_retryable"] is None


def test_malformed_line_is_a_parse_error_and_not_gated(spark: SparkSession) -> None:
    frame = bronze_files(spark, ORDERS, {order_path(): ["{not json"]})
    row = apply_rules(frame, order_rules()).collect()[0]
    assert row["_parse_error"] == "malformed_json"
    assert list(row["_rejection_codes"]) == []  # parse failures stay in Bronze only


def test_multi_failure_row_lists_codes_in_rule_order_and_first_category(
    spark: SparkSession,
) -> None:
    case = oc(
        "multi",
        [],
        order_status="shipped",
        order_timestamp=FUTURE_TS,
        gross_amount="-25.50",
        net_amount="-28.00",
    )
    row = _evaluate(spark, ORDERS, order_rules(), [case], "orders")["multi"]
    codes = list(row["_rejection_codes"])
    index = [r.code for r in order_rules()].index
    assert {"order_status_invalid", "gross_amount_not_positive", FUTURE} <= set(codes)
    assert codes == sorted(codes, key=index)
    assert row["_error_category"] == next(r.category for r in order_rules() if r.code == codes[0])
    assert row["_rejection_reason"] == ", ".join(codes)


def test_rejection_reason_never_contains_field_values(spark: SparkSession) -> None:
    leaked = "SECRET-NAME-7731"
    case = pc("leak", [], product_name="", category=leaked, subset=True)
    row = _evaluate(spark, PRODUCTS, product_rules(), [case], "products")["leak"]
    assert leaked not in str(row["_rejection_reason"])
    assert leaked not in " ".join(row["_rejection_codes"])


def test_negative_a_cross_brand_item_is_not_hidden_by_other_valid_items(
    order_results: dict[str, Row],
) -> None:
    """One bad line item rejects the whole order (no partial acceptance of an order)."""
    row = order_results["item_other_brand"]
    assert "item_parent_mismatch" in list(row["_rejection_codes"])
