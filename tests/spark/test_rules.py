"""Contract rules of the DLQ gate (W2.D10.2), derived from DATA_CONTRACTS section 2 and the pydantic
reference models, not from the rule implementation.

Every rule code has at least one case that must fire it (and exactly it, apart from codes that
legitimately co-fire) plus valid boundary cases that must not fire anything. A differential test
proves "Spark rejects <=> pydantic rejects" on generated events of several seeds.

Runs inside the Spark image (``docker compose --profile tools run --rm spark-test``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest


pytest.importorskip("pyspark")

from pydantic import ValidationError  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse.contracts import ClickstreamEvent, CustomerCdcEvent  # noqa: E402
from lakehouse.ingestion.bronze import CLICKSTREAM, CUSTOMERS, to_bronze_rows  # noqa: E402
from lakehouse.quality.rules import (  # noqa: E402
    RULES_BY_TABLE,
    ErrorCategory,
    Rule,
    apply_rules,
    cdc_rules,
    clickstream_rules,
)


if TYPE_CHECKING:
    from pyspark.sql import Row, SparkSession

    from lakehouse.ingestion.bronze import BronzeTarget


KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField(
            "headers",
            ArrayType(
                StructType([StructField("key", StringType()), StructField("value", BinaryType())])
            ),
        ),
    ]
)
KAFKA_TS = datetime(2026, 3, 5, 10, 0, 0, tzinfo=UTC)

ENVELOPE_CODES = [
    "brand_id_null_or_wrong_type",
    "brand_id_invalid",
    "kafka_key_missing",
    "brand_key_mismatch",
    "brand_header_missing",
    "brand_header_mismatch",
    "event_id_null_or_wrong_type",
    "event_id_invalid",
    "schema_version_invalid",
    "produced_at_null_or_wrong_type",
    "produced_at_invalid",
]
CDC_CODES = [
    *ENVELOPE_CODES,
    "customer_id_null_or_wrong_type",
    "customer_id_invalid",
    "first_name_null_or_wrong_type",
    "first_name_invalid",
    "last_name_null_or_wrong_type",
    "last_name_invalid",
    "email_null_or_wrong_type",
    "email_invalid",
    "phone_invalid",
    "country_null_or_wrong_type",
    "country_invalid",
    "loyalty_tier_null_or_wrong_type",
    "loyalty_tier_invalid",
    "signup_date_null_or_wrong_type",
    "signup_date_invalid",
    "cdc_action_null_or_wrong_type",
    "cdc_action_invalid",
    "cdc_sequence_null_or_wrong_type",
    "cdc_sequence_invalid",
    "cdc_ts_null_or_wrong_type",
    "cdc_ts_invalid",
    "signup_after_produced",
]
CLICK_CODES = [
    *ENVELOPE_CODES,
    "session_id_null_or_wrong_type",
    "session_id_invalid",
    "customer_id_invalid",
    "event_type_null_or_wrong_type",
    "event_type_invalid",
    "device_category_null_or_wrong_type",
    "device_category_invalid",
    "event_timestamp_null_or_wrong_type",
    "event_timestamp_invalid",
]

ABSENT = object()  # drop the field from the JSON object
KEEP = "__keep__"  # header/key brand follows the default
# Transport-level rules (Kafka key/header, Q-002): no Pydantic counterpart, so they are outside the
# differential parity tests below; the fixtures always carry key and header (asserted there).
TRANSPORT_CODES = {"kafka_key_missing", "brand_header_missing"}
CUSTOMER = "22222222-2222-4222-8222-222222222222"
SESSION = "44444444-4444-4444-8444-444444444444"

BASE_CDC: dict[str, Any] = {
    "event_id": "11111111-1111-4111-8111-111111111111",
    "customer_id": CUSTOMER,
    "brand_id": "brand_alpha",
    "schema_version": 1,
    "produced_at": "2026-01-01T10:00:00.123Z",
    "first_name": "Ada",
    "last_name": "Lovelace",
    "email": "ada@example.com",
    "phone": "+447700900123",
    "city": "Berlin",
    "country": "DE",
    "loyalty_tier": "Gold",
    "signup_date": "2025-12-01",
    "_cdc_action": "I",
    "_cdc_sequence": 7,
    "_cdc_ts": "2026-01-01T10:00:00.000Z",
}
BASE_CLICK: dict[str, Any] = {
    "event_id": "33333333-3333-4333-8333-333333333333",
    "brand_id": "brand_alpha",
    "schema_version": 1,
    "produced_at": "2026-01-01T10:00:00.500Z",
    "session_id": SESSION,
    "customer_id": None,
    "event_type": "page_view",
    "page_url": "/home",
    "device_category": "mobile",
    "event_timestamp": "2026-01-01T09:59:59.000Z",
}


@dataclass(frozen=True)
class Case:
    cid: str
    value: bytes | None
    key: str | None
    headers: list[tuple[str, bytes | None]] | None
    expected: list[str]


def _case(
    base: dict[str, Any],
    entity_field: str,
    cid: str,
    expected: list[str],
    over: dict[str, Any] | None = None,
    *,
    key_brand: str | None = "brand_alpha",
    header_brand: str | None = "brand_alpha",
    raw_key: Any = KEEP,
    raw_headers: Any = KEEP,
) -> Case:
    payload = dict(base)
    for name, value in (over or {}).items():
        if value is ABSENT:
            payload.pop(name, None)
        else:
            payload[name] = value
    key_entity = base[entity_field] if base[entity_field] is not None else ""
    return Case(
        cid=cid,
        value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        key=raw_key
        if raw_key is not KEEP
        else (None if key_brand is None else f"{key_brand}:{key_entity}"),
        headers=raw_headers
        if raw_headers is not KEEP
        else (None if header_brand is None else [("brand_id", header_brand.encode("utf-8"))]),
        expected=expected,
    )


def cdc(cid: str, expected: list[str], over: dict[str, Any] | None = None, **kw: Any) -> Case:
    return _case(BASE_CDC, "customer_id", cid, expected, over, **kw)


def click(cid: str, expected: list[str], over: dict[str, Any] | None = None, **kw: Any) -> Case:
    return _case(BASE_CLICK, "session_id", cid, expected, over, **kw)


def _brand_cases(make: Any) -> list[Case]:
    """Envelope brand cases, identical for both tables (``make`` is ``cdc`` or ``click``)."""

    def with_brand(cid: str, brand: Any, expected: list[str]) -> Case:
        shown = brand if isinstance(brand, str) else "brand_alpha"
        return make(cid, expected, {"brand_id": brand}, key_brand=shown, header_brand=shown)

    return [
        make("brand_null", ["brand_id_null_or_wrong_type"], {"brand_id": None}),
        with_brand("brand_absent_key_and_header_agree", ABSENT, ["brand_id_null_or_wrong_type"]),
        # Payload brand null + key/header naming another tenant: only the null code fires.
        make(
            "brand_null_with_foreign_key_and_header",
            ["brand_id_null_or_wrong_type"],
            {"brand_id": None},
            key_brand="brand_beta",
            header_brand="brand_beta",
        ),
        with_brand("brand_uppercase", "Brand_alpha", ["brand_id_invalid"]),
        with_brand("brand_trailing_newline", "brand_x\n", ["brand_id_invalid"]),
        with_brand("brand_prefix_only", "brand_", ["brand_id_invalid"]),
        with_brand("brand_hyphen", "brand_a-b", ["brand_id_invalid"]),
        with_brand("brand_41_chars", "brand_" + "a" * 41, ["brand_id_invalid"]),
        with_brand("brand_40_chars_valid", "brand_" + "a" * 40, []),
        with_brand("brand_short_valid", "brand_x", []),
        with_brand("brand_digits_underscore_valid", "brand_a1_b2", []),
        make("key_brand_mismatch", ["brand_key_mismatch"], key_brand="brand_beta"),
        make("header_brand_mismatch", ["brand_header_mismatch"], header_brand="brand_beta"),
        # Transport rules (Q-002): a missing key / header fires only its own *_missing code.
        make("key_null", ["kafka_key_missing"], key_brand=None),
        make("key_empty_bytes", ["kafka_key_missing"], raw_key=""),
        make("key_present_passes", []),
        # Non-empty key that is not valid UTF-8: no readable brand prefix -> mismatch, not a pass.
        make("key_not_utf8", ["brand_key_mismatch"], raw_key=b"\xff\xfe:entity"),
        make("header_absent", ["brand_header_missing"], header_brand=None),
        make("header_list_empty", ["brand_header_missing"], raw_headers=[]),
        make(
            "header_other_key_only",
            ["brand_header_missing"],
            raw_headers=[("schema_version", b"1")],
        ),
        make("header_null_value", ["brand_header_missing"], raw_headers=[("brand_id", None)]),
        make("header_present_passes", [], raw_headers=[("brand_id", b"brand_alpha")]),
        make(
            "key_and_header_missing_both_fire_in_rule_order",
            ["kafka_key_missing", "brand_header_missing"],
            key_brand=None,
            header_brand=None,
        ),
        make(
            "key_and_header_mismatch_both_fire",
            ["brand_key_mismatch", "brand_header_mismatch"],
            key_brand="brand_beta",
            header_brand="brand_gamma",
        ),
        make("event_id_null", ["event_id_null_or_wrong_type"], {"event_id": None}),
        make("event_id_absent", ["event_id_null_or_wrong_type"], {"event_id": ABSENT}),
        make("event_id_not_a_uuid", ["event_id_invalid"], {"event_id": "not-a-uuid"}),
        make(
            "event_id_uppercase",
            ["event_id_invalid"],
            {"event_id": "11111111-1111-4111-8111-11111111111A"},
        ),
        make(
            "event_id_uuid_v1",
            ["event_id_invalid"],
            {"event_id": "11111111-1111-1111-8111-111111111111"},
        ),
        make(
            "event_id_bad_variant",
            ["event_id_invalid"],
            {"event_id": "11111111-1111-4111-c111-111111111111"},
        ),
        make("schema_version_null", ["schema_version_invalid"], {"schema_version": None}),
        make("schema_version_absent", ["schema_version_invalid"], {"schema_version": ABSENT}),
        make("schema_version_2", ["schema_version_invalid"], {"schema_version": 2}),
        make("schema_version_0", ["schema_version_invalid"], {"schema_version": 0}),
        make("schema_version_string_1", ["schema_version_invalid"], {"schema_version": "1"}),
        make("produced_at_null", ["produced_at_null_or_wrong_type"], {"produced_at": None}),
        make("produced_at_absent", ["produced_at_null_or_wrong_type"], {"produced_at": ABSENT}),
        make(
            "produced_at_without_offset",
            ["produced_at_invalid"],
            {"produced_at": "2026-01-01T10:00:00"},
        ),
        make("produced_at_date_only", ["produced_at_invalid"], {"produced_at": "2026-01-01"}),
        make("produced_at_garbage", ["produced_at_invalid"], {"produced_at": "yesterday"}),
        make("produced_at_numeric_offset_valid", [], {"produced_at": "2026-01-01T10:00:00+01:00"}),
        make("produced_at_zulu_no_fraction_valid", [], {"produced_at": "2026-01-01T10:00:00Z"}),
    ]


CDC_CASES: list[Case] = [
    cdc("cdc_baseline_valid", []),
    *_brand_cases(cdc),
    cdc("customer_id_null", ["customer_id_null_or_wrong_type"], {"customer_id": None}),
    cdc("customer_id_absent", ["customer_id_null_or_wrong_type"], {"customer_id": ABSENT}),
    cdc("customer_id_invalid", ["customer_id_invalid"], {"customer_id": "123"}),
    cdc("first_name_null", ["first_name_null_or_wrong_type"], {"first_name": None}),
    cdc("first_name_empty", ["first_name_invalid"], {"first_name": ""}),
    cdc("first_name_51_chars", ["first_name_invalid"], {"first_name": "a" * 51}),
    cdc("first_name_50_chars_valid", [], {"first_name": "a" * 50}),
    cdc("first_name_1_char_valid", [], {"first_name": "A"}),
    cdc("last_name_null", ["last_name_null_or_wrong_type"], {"last_name": None}),
    cdc("last_name_empty", ["last_name_invalid"], {"last_name": ""}),
    cdc("last_name_51_chars", ["last_name_invalid"], {"last_name": "a" * 51}),
    cdc("last_name_50_chars_valid", [], {"last_name": "a" * 50}),
    cdc("email_null", ["email_null_or_wrong_type"], {"email": None}),
    cdc("email_absent", ["email_null_or_wrong_type"], {"email": ABSENT}),
    # Bronze (from_json) coerces a JSON number into the string "12345": the wrong type is no
    # longer visible, the value is still rejected (spec gap reported: *_wrong_type is unreachable).
    cdc("email_number_is_coerced_to_string_and_rejected", ["email_invalid"], {"email": 12345}),
    cdc("email_no_domain", ["email_invalid"], {"email": "user@"}),
    cdc("email_no_at", ["email_invalid"], {"email": "missing-at.com"}),
    cdc("email_double_at", ["email_invalid"], {"email": "two@@example.com"}),
    cdc("email_with_spaces", ["email_invalid"], {"email": "spaces in@example.com"}),
    cdc("email_no_tld", ["email_invalid"], {"email": "no-tld@example"}),
    cdc("email_plus_tag_valid", [], {"email": "ada+tag@mail.example.org"}),
    cdc("phone_absent_is_valid", [], {"phone": ABSENT}),
    cdc("phone_null_is_valid", [], {"phone": None}),
    cdc("phone_without_plus", ["phone_invalid"], {"phone": "447700900123"}),
    cdc("phone_leading_zero_country", ["phone_invalid"], {"phone": "+0123456"}),
    cdc("phone_with_spaces", ["phone_invalid"], {"phone": "+44 7700 900123"}),
    cdc("country_null", ["country_null_or_wrong_type"], {"country": None}),
    cdc("country_lowercase", ["country_invalid"], {"country": "de"}),
    cdc("country_three_letters", ["country_invalid"], {"country": "DEU"}),
    cdc("country_digit", ["country_invalid"], {"country": "D1"}),
    cdc("loyalty_tier_null", ["loyalty_tier_null_or_wrong_type"], {"loyalty_tier": None}),
    cdc("loyalty_tier_unknown", ["loyalty_tier_invalid"], {"loyalty_tier": "Diamond"}),
    cdc("loyalty_tier_wrong_case", ["loyalty_tier_invalid"], {"loyalty_tier": "gold"}),
    cdc("loyalty_tier_platinum_valid", [], {"loyalty_tier": "Platinum"}),
    cdc("signup_date_null", ["signup_date_null_or_wrong_type"], {"signup_date": None}),
    cdc("signup_date_month_13", ["signup_date_invalid"], {"signup_date": "2025-13-45"}),
    cdc("signup_date_wrong_format", ["signup_date_invalid"], {"signup_date": "01/12/2025"}),
    cdc("signup_date_text", ["signup_date_invalid"], {"signup_date": "not-a-date"}),
    cdc("cdc_action_null", ["cdc_action_null_or_wrong_type"], {"_cdc_action": None}),
    cdc("cdc_action_unknown", ["cdc_action_invalid"], {"_cdc_action": "X"}),
    cdc("cdc_action_lowercase", ["cdc_action_invalid"], {"_cdc_action": "i"}),
    cdc("cdc_action_delete_valid", [], {"_cdc_action": "D"}),
    cdc("cdc_sequence_null", ["cdc_sequence_null_or_wrong_type"], {"_cdc_sequence": None}),
    cdc("cdc_sequence_absent", ["cdc_sequence_null_or_wrong_type"], {"_cdc_sequence": ABSENT}),
    cdc("cdc_sequence_zero", ["cdc_sequence_invalid"], {"_cdc_sequence": 0}),
    cdc("cdc_sequence_negative", ["cdc_sequence_invalid"], {"_cdc_sequence": -3}),
    cdc("cdc_sequence_one_valid", [], {"_cdc_sequence": 1}),
    cdc("cdc_ts_null", ["cdc_ts_null_or_wrong_type"], {"_cdc_ts": None}),
    cdc("cdc_ts_without_offset", ["cdc_ts_invalid"], {"_cdc_ts": "2026-01-01T10:00:00"}),
    cdc("cdc_ts_garbage", ["cdc_ts_invalid"], {"_cdc_ts": "soon"}),
    cdc("cdc_ts_offset_valid", [], {"_cdc_ts": "2026-01-01T10:00:00+02:00"}),
    # signup_after_produced compares against the UTC date of produced_at.
    cdc("signup_equals_produced_date_valid", [], {"signup_date": "2026-01-01"}),
    cdc("signup_one_day_after_produced", ["signup_after_produced"], {"signup_date": "2026-01-02"}),
    cdc(
        "signup_equals_utc_date_of_offset_produced_at_valid",
        [],
        {"produced_at": "2026-01-01T23:30:00-05:00", "signup_date": "2026-01-02"},
    ),
    cdc(
        "signup_after_utc_date_of_offset_produced_at",
        ["signup_after_produced"],
        {"produced_at": "2026-01-01T23:30:00-05:00", "signup_date": "2026-01-03"},
    ),
    # Canonicalised produced_at: offsets without colon, lower-case t / z designators.
    cdc(
        "produced_at_0100_signup_utc_date_valid",
        [],
        {"produced_at": "2026-01-01T00:30:00+0100", "signup_date": "2025-12-31"},
    ),
    cdc(
        "produced_at_0100_signup_after_utc_date",
        ["signup_after_produced"],
        {"produced_at": "2026-01-01T00:30:00+0100", "signup_date": "2026-01-01"},
    ),
    cdc(
        "produced_at_lower_z_signup_equal_valid",
        [],
        {"produced_at": "2026-01-01T10:00:00z", "signup_date": "2026-01-01"},
    ),
    cdc(
        "produced_at_lower_z_signup_after",
        ["signup_after_produced"],
        {"produced_at": "2026-01-01T10:00:00z", "signup_date": "2026-01-02"},
    ),
    cdc(
        "produced_at_lower_t_signup_equal_valid",
        [],
        {"produced_at": "2026-01-01t10:00:00Z", "signup_date": "2026-01-01"},
    ),
    cdc(
        "produced_at_lower_t_signup_after",
        ["signup_after_produced"],
        {"produced_at": "2026-01-01t10:00:00Z", "signup_date": "2026-01-02"},
    ),
    cdc(
        "signup_far_future",
        ["signup_after_produced"],
        {"signup_date": "2030-06-01"},
    ),
]

CLICK_CASES: list[Case] = [
    click("click_baseline_valid", []),
    *_brand_cases(click),
    click("session_id_null", ["session_id_null_or_wrong_type"], {"session_id": None}),
    click("session_id_absent", ["session_id_null_or_wrong_type"], {"session_id": ABSENT}),
    click("session_id_invalid", ["session_id_invalid"], {"session_id": "abc"}),
    click("customer_id_absent_is_valid", [], {"customer_id": ABSENT}),
    click("customer_id_null_is_valid", [], {"customer_id": None}),
    click("customer_id_valid_uuid", [], {"customer_id": CUSTOMER}),
    click("customer_id_invalid", ["customer_id_invalid"], {"customer_id": "abc"}),
    click("event_type_null", ["event_type_null_or_wrong_type"], {"event_type": None}),
    click("event_type_unknown", ["event_type_invalid"], {"event_type": "view"}),
    click("event_type_purchase_valid", [], {"event_type": "purchase"}),
    click("device_null", ["device_category_null_or_wrong_type"], {"device_category": None}),
    click("device_unknown", ["device_category_invalid"], {"device_category": "phablet"}),
    click("device_tablet_valid", [], {"device_category": "tablet"}),
    click(
        "event_timestamp_null", ["event_timestamp_null_or_wrong_type"], {"event_timestamp": None}
    ),
    click(
        "event_timestamp_without_offset",
        ["event_timestamp_invalid"],
        {"event_timestamp": "2026-01-01T09:59:59"},
    ),
    click("event_timestamp_garbage", ["event_timestamp_invalid"], {"event_timestamp": "later"}),
    click("event_timestamp_offset_valid", [], {"event_timestamp": "2026-01-01T09:59:59+01:00"}),
    # Multi-failure: all codes in rule order.
    click(
        "session_null_and_device_unknown",
        ["session_id_null_or_wrong_type", "device_category_invalid"],
        {"session_id": None, "device_category": "phablet"},
    ),
]


def _evaluate(
    spark: SparkSession,
    target: BronzeTarget,
    rules: list[Rule],
    cases: list[Case],
) -> dict[str, Row]:
    rows = [
        (
            None if c.key is None else c.key if isinstance(c.key, bytes) else c.key.encode("utf-8"),
            c.value,
            "t",
            0,
            i,
            KAFKA_TS,
            c.headers,
        )
        for i, c in enumerate(cases)
    ]
    frame = spark.createDataFrame(rows, KAFKA_SCHEMA)
    out = apply_rules(to_bronze_rows(frame, target, "batch-1"), rules).collect()
    by_offset = {int(r["_source_ref"].rsplit("/", 1)[1]): r for r in out}
    assert len(by_offset) == len(cases)  # a row is never dropped
    return {c.cid: by_offset[i] for i, c in enumerate(cases)}


@pytest.fixture(scope="module")
def cdc_results(spark: SparkSession) -> dict[str, Row]:
    assert len({c.cid for c in CDC_CASES}) == len(CDC_CASES), "duplicate case id"
    return _evaluate(spark, CUSTOMERS, cdc_rules(), CDC_CASES)


@pytest.fixture(scope="module")
def click_results(spark: SparkSession) -> dict[str, Row]:
    assert len({c.cid for c in CLICK_CASES}) == len(CLICK_CASES), "duplicate case id"
    return _evaluate(spark, CLICKSTREAM, clickstream_rules(), CLICK_CASES)


# ---- rule catalogue -------------------------------------------------------------------------


def test_cdc_rule_codes_and_order_follow_the_contract() -> None:
    assert [r.code for r in cdc_rules()] == CDC_CODES


def test_clickstream_rule_codes_and_order_follow_the_contract() -> None:
    assert [r.code for r in clickstream_rules()] == CLICK_CODES


def test_rules_by_table_maps_bronze_tables_to_rule_sets() -> None:
    assert set(RULES_BY_TABLE) == {"raw_customers", "raw_clickstream"}
    assert [r.code for r in RULES_BY_TABLE["raw_customers"]()] == CDC_CODES
    assert [r.code for r in RULES_BY_TABLE["raw_clickstream"]()] == CLICK_CODES


@pytest.mark.parametrize("rules", [cdc_rules(), clickstream_rules()], ids=["cdc", "click"])
def test_rule_metadata_is_consistent(rules: list[Rule]) -> None:
    codes = [r.code for r in rules]
    assert len(set(codes)) == len(codes)
    for rule in rules:
        assert rule.retryable is False, rule.code
        expected = (
            ErrorCategory.BUSINESS_RULE_VIOLATION
            if rule.code == "signup_after_produced"
            else ErrorCategory.SCHEMA_VIOLATION
        )
        assert rule.category == expected, rule.code


def test_error_category_values_are_the_contract_strings() -> None:
    assert ErrorCategory.SCHEMA_VIOLATION == "SCHEMA_VIOLATION"
    assert ErrorCategory.BUSINESS_RULE_VIOLATION == "BUSINESS_RULE_VIOLATION"


def test_every_rule_has_a_positive_and_a_negative_case() -> None:
    fired = {code for c in [*CDC_CASES, *CLICK_CASES] for code in c.expected}
    missing = (set(CDC_CODES) | set(CLICK_CODES)) - fired
    assert not missing, sorted(missing)
    assert any(c.expected == [] for c in CDC_CASES)
    assert any(c.expected == [] for c in CLICK_CASES)


# ---- one positive / one negative per rule ---------------------------------------------------


@pytest.mark.parametrize("case", CDC_CASES, ids=lambda c: c.cid)
def test_cdc_case_fires_exactly_the_expected_codes(case: Case, cdc_results: dict[str, Row]) -> None:
    row = cdc_results[case.cid]
    assert list(row["_rejection_codes"]) == case.expected, case.cid
    _assert_row_shape(row, case.expected)


@pytest.mark.parametrize("case", CLICK_CASES, ids=lambda c: c.cid)
def test_clickstream_case_fires_exactly_the_expected_codes(
    case: Case, click_results: dict[str, Row]
) -> None:
    row = click_results[case.cid]
    assert list(row["_rejection_codes"]) == case.expected, case.cid
    _assert_row_shape(row, case.expected)


def _assert_row_shape(row: Row, expected: list[str]) -> None:
    if not expected:
        assert row["_rejection_codes"] is not None  # [] and never NULL
        assert row["_error_category"] is None
        assert row["_rejection_reason"] is None
        assert row["_retryable"] is None
    else:
        assert row["_rejection_reason"] == ", ".join(expected)
        assert row["_retryable"] is False


# ---- apply_rules output contract ------------------------------------------------------------


def test_multi_failure_row_lists_all_codes_in_rule_order_and_first_category(
    spark: SparkSession,
) -> None:
    leaked = "leak7731user@"
    case = cdc(
        "multi",
        [
            "brand_header_mismatch",
            "first_name_invalid",
            "email_invalid",
            "signup_after_produced",
        ],
        {"email": leaked, "first_name": "", "signup_date": "2026-02-01"},
        header_brand="brand_beta",
    )
    row = _evaluate(spark, CUSTOMERS, cdc_rules(), [case])["multi"]
    assert list(row["_rejection_codes"]) == case.expected
    assert row["_error_category"] == "SCHEMA_VIOLATION"  # first failing rule, not the business one
    assert row["_rejection_reason"] == ", ".join(case.expected)
    assert leaked not in row["_rejection_reason"]


@pytest.mark.parametrize(
    ("build", "target", "rules"),
    [(cdc, CUSTOMERS, cdc_rules), (click, CLICKSTREAM, clickstream_rules)],
    ids=["cdc", "click"],
)
def test_missing_key_and_header_list_both_codes_and_stay_schema_violations(
    spark: SparkSession, build: Any, target: BronzeTarget, rules: Any
) -> None:
    case = build(
        "no_transport",
        ["kafka_key_missing", "brand_header_missing"],
        key_brand=None,
        header_brand=None,
    )
    row = _evaluate(spark, target, rules(), [case])["no_transport"]
    assert list(row["_rejection_codes"]) == ["kafka_key_missing", "brand_header_missing"]
    assert row["_error_category"] == "SCHEMA_VIOLATION"
    assert row["_retryable"] is False
    assert row["_rejection_reason"] == "kafka_key_missing, brand_header_missing"


def test_transport_rules_do_not_gate_parse_errors(spark: SparkSession) -> None:
    """A tombstone without key and header stays in Bronze only: no DLQ codes (parse errors skip)."""
    frame = spark.createDataFrame([(None, None, "t", 0, 0, KAFKA_TS, None)], KAFKA_SCHEMA)
    out = apply_rules(to_bronze_rows(frame, CUSTOMERS, "b"), cdc_rules()).collect()[0]
    assert out["_parse_error"] == "null_value"
    assert list(out["_rejection_codes"]) == []


def test_business_rule_alone_gives_business_category(spark: SparkSession) -> None:
    case = cdc("biz", ["signup_after_produced"], {"signup_date": "2026-02-01"})
    row = _evaluate(spark, CUSTOMERS, cdc_rules(), [case])["biz"]
    assert list(row["_rejection_codes"]) == ["signup_after_produced"]
    assert row["_error_category"] == "BUSINESS_RULE_VIOLATION"
    assert row["_retryable"] is False


def test_rejection_reason_never_contains_field_values(spark: SparkSession) -> None:
    secret_email = "secret.person.4242@@example.com"
    secret_name = "Zebediah" + "x" * 60  # 68 chars: invalid
    case = cdc(
        "pii",
        ["first_name_invalid", "email_invalid"],
        {
            "email": secret_email,
            "first_name": secret_name,
        },
    )
    row = _evaluate(spark, CUSTOMERS, cdc_rules(), [case])["pii"]
    reason = row["_rejection_reason"]
    assert reason == "first_name_invalid, email_invalid"
    for value in (secret_email, secret_name, "secret", "Zebediah", "4242"):
        assert value not in reason


def test_apply_rules_adds_columns_and_keeps_every_input_column(spark: SparkSession) -> None:
    rows = [(b"brand_alpha:x", json.dumps(BASE_CDC).encode(), "t", 0, 0, KAFKA_TS, None)]
    bronze = to_bronze_rows(spark.createDataFrame(rows, KAFKA_SCHEMA), CUSTOMERS, "b")
    out = apply_rules(bronze, cdc_rules())
    assert out.columns[: len(bronze.columns)] == bronze.columns
    assert set(out.columns) - set(bronze.columns) == {
        "_rejection_codes",
        "_error_category",
        "_rejection_reason",
        "_retryable",
    }
    assert out.schema["_rejection_codes"].dataType.simpleString() == "array<string>"
    assert out.count() == bronze.count()


@pytest.mark.parametrize(
    "value",
    [None, b"\xff\xfeabc", b"{not json", b"[1,2]"],
    ids=["tombstone", "invalid_utf8", "malformed_json", "json_array"],
)
def test_parse_error_rows_do_not_crash_and_have_non_null_codes(
    spark: SparkSession, value: bytes | None
) -> None:
    rows = [(b"brand_alpha:k", value, "t", 0, 0, KAFKA_TS, [("brand_id", b"brand_alpha")])]
    frame = spark.createDataFrame(rows, KAFKA_SCHEMA)
    for target, rules in ((CUSTOMERS, cdc_rules()), (CLICKSTREAM, clickstream_rules())):
        bronze = to_bronze_rows(frame, target, "b")
        assert bronze.collect()[0]["_parse_error"] is not None
        out = apply_rules(bronze, rules).collect()
        assert len(out) == 1
        assert out[0]["_rejection_codes"] is not None


# ---- NULL safety ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("target", "make_rules"),
    [(CUSTOMERS, cdc_rules), (CLICKSTREAM, clickstream_rules)],
    ids=["cdc", "click"],
)
@pytest.mark.parametrize(
    ("key", "headers"),
    [(None, None), (b"", []), (b"nocolon", None), (b"brand_alpha:", [("brand_id", b"")])],
    ids=["null_key_no_headers", "empty_key", "key_without_colon", "empty_header_value"],
)
def test_every_rule_column_is_a_non_null_boolean_on_an_all_null_row(
    spark: SparkSession,
    target: BronzeTarget,
    make_rules: Any,
    key: bytes | None,
    headers: list[tuple[str, bytes]] | None,
) -> None:
    frame = spark.createDataFrame([(key, b"{}", "t", 0, 0, KAFKA_TS, headers)], KAFKA_SCHEMA)
    bronze = to_bronze_rows(frame, target, "b")
    assert bronze.collect()[0]["_parse_error"] is None  # `{}` parses; every field is NULL
    rules: list[Rule] = make_rules()
    row = bronze.select(*[r.failed().alias(r.code) for r in rules]).collect()[0]
    for rule in rules:
        assert row[rule.code] is not None, f"{rule.code} is NULL on an all-null row"
        assert isinstance(row[rule.code], bool), rule.code
    # An empty object violates the contract: at least the required-field codes fire.
    assert row["brand_id_null_or_wrong_type"] is True
    assert row["event_id_null_or_wrong_type"] is True
    # Conditional rules must not fire on absent optional data / null payload brand.
    assert row["brand_key_mismatch"] is False
    assert row["brand_header_mismatch"] is False
    # Missing-ness follows the input exactly (empty key / header without a value count as missing).
    assert row["kafka_key_missing"] is (not key)
    assert row["brand_header_missing"] is (
        not any(k == "brand_id" and v is not None for k, v in headers or [])
    )


def test_all_null_row_gets_a_non_empty_code_array(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(None, b"{}", "t", 0, 0, KAFKA_TS, None)], KAFKA_SCHEMA)
    out = apply_rules(to_bronze_rows(frame, CUSTOMERS, "b"), cdc_rules()).collect()[0]
    assert "brand_id_null_or_wrong_type" in out["_rejection_codes"]
    assert out["_error_category"] == "SCHEMA_VIOLATION"
    assert "signup_after_produced" not in out["_rejection_codes"]  # nulls never fire it


# ---- differential against the pydantic reference --------------------------------------------

FIXTURES = sorted(
    (Path(__file__).resolve().parents[1] / "fixtures" / "rules_diff").glob("seed_*.json")
)


@dataclass(frozen=True)
class Msg:
    key: str
    value: bytes
    headers: list[tuple[str, bytes]]


def _load(section: dict[str, Any]) -> list[Msg]:
    return [
        Msg(
            m["key"],
            m["value"].encode("utf-8"),
            [(k, v.encode("utf-8")) for k, v in m["headers"]],
        )
        for m in section["messages"]
    ]


def _bronze(spark: SparkSession, target: BronzeTarget, messages: list[Msg]) -> Any:
    rows = [
        (m.key.encode("utf-8"), m.value, "t", 0, i, KAFKA_TS, m.headers)
        for i, m in enumerate(messages)
    ]
    return to_bronze_rows(spark.createDataFrame(rows, KAFKA_SCHEMA), target, "b")


def _by_offset(rows: list[Row]) -> dict[int, Row]:
    return {int(r["_source_ref"].rsplit("/", 1)[1]): r for r in rows}


def _pydantic_rejects(model: Any, raw: bytes) -> bool:
    try:
        model.model_validate_json(raw)
    except ValidationError:
        return True
    return False


def test_differential_fixtures_carry_key_and_header() -> None:
    # Pydantic parity is only meaningful when the transport rules cannot fire (Q-002).
    for path in FIXTURES:
        data = json.loads(path.read_text(encoding="utf-8"))
        for section in (data["cdc"], data["click"]):
            for m in _load(section):
                assert m.key
                assert any(k == "brand_id" and v for k, v in m.headers)


def test_differential_fixtures_exist() -> None:
    # Frozen generator output (the Spark image has no Faker); freshness is guarded by
    # tests/unit/quality/test_rules_fixture.py on the host.
    assert [p.name for p in FIXTURES] == ["seed_1.json", "seed_42.json"]


@pytest.mark.parametrize("path", FIXTURES, ids=lambda p: p.stem)
def test_cdc_rules_agree_with_pydantic_and_manifest(spark: SparkSession, path: Path) -> None:
    data = json.loads(path.read_text(encoding="utf-8"))["cdc"]
    messages = _load(data)
    out = _by_offset(apply_rules(_bronze(spark, CUSTOMERS, messages), cdc_rules()).collect())
    assert len(out) == len(messages)

    rejected: set[int] = set()
    for i, message in enumerate(messages):
        row = out[i]
        assert row["_parse_error"] is None
        spark_rejects = bool(set(row["_rejection_codes"]) - TRANSPORT_CODES)  # not in pydantic
        assert spark_rejects == _pydantic_rejects(CustomerCdcEvent, message.value), (
            i,
            list(row["_rejection_codes"]),
        )
        if spark_rejects:
            rejected.add(i)
    assert data["dlq_indexes"], "fixture must contain DLQ defects"
    assert rejected == set(data["dlq_indexes"])
    by_defect = {
        "invalid_email": "email_invalid",
        "null_email": "email_null_or_wrong_type",
        "future_signup": "signup_after_produced",
    }
    for index, defect in data["defect_by_index"].items():
        if defect in by_defect:
            assert list(out[int(index)]["_rejection_codes"]) == [by_defect[defect]], index
        else:  # duplicates, out-of-order, collisions are valid payloads
            assert list(out[int(index)]["_rejection_codes"]) == [], (index, defect)
    for defect, code in by_defect.items():
        got = sum(code in r["_rejection_codes"] for r in out.values())
        assert got == data["counts"][defect], defect


@pytest.mark.parametrize("path", FIXTURES, ids=lambda p: p.stem)
def test_clickstream_rules_agree_with_pydantic_and_manifest(
    spark: SparkSession, path: Path
) -> None:
    data = json.loads(path.read_text(encoding="utf-8"))["click"]
    messages = _load(data)
    frame = _bronze(spark, CLICKSTREAM, messages)
    out = _by_offset(apply_rules(frame, clickstream_rules()).collect())
    assert len(out) == len(messages)

    parse_errors = {i for i, r in out.items() if r["_parse_error"] is not None}
    assert parse_errors == set(data["parse_error_indexes"])
    assert parse_errors
    rejected: set[int] = set()
    for i, message in enumerate(messages):
        if i in parse_errors:
            continue
        row = out[i]
        spark_rejects = bool(set(row["_rejection_codes"]) - TRANSPORT_CODES)  # not in pydantic
        assert spark_rejects == _pydantic_rejects(ClickstreamEvent, message.value), (
            i,
            list(row["_rejection_codes"]),
        )
        if spark_rejects:
            rejected.add(i)
    assert rejected == set(data["dlq_indexes"])
    assert rejected
    for i in rejected:
        assert list(out[i]["_rejection_codes"]) == ["session_id_null_or_wrong_type"]
    # Late events and duplicates are valid events, never rejected.
    for index, defect in data["defect_by_index"].items():
        if defect in {"late", "duplicate"}:
            assert list(out[int(index)]["_rejection_codes"]) == [], (index, defect)


def test_differential_oracle_detects_a_tampered_event(spark: SparkSession) -> None:
    """Negative: turning one valid generated event invalid is seen by pydantic and by the rules."""
    data = json.loads(FIXTURES[0].read_text(encoding="utf-8"))["cdc"]
    clean = next(i for i in range(len(data["messages"])) if i not in set(data["dlq_indexes"]))
    original = _load(data)[clean]
    payload = json.loads(original.value)
    payload["email"] = "tampered@"
    tampered = Msg(original.key, json.dumps(payload).encode("utf-8"), original.headers)
    assert _pydantic_rejects(CustomerCdcEvent, tampered.value)
    row = _by_offset(apply_rules(_bronze(spark, CUSTOMERS, [tampered]), cdc_rules()).collect())[0]
    assert list(row["_rejection_codes"]) == ["email_invalid"]
