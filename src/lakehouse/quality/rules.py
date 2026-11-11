"""Contract validation rules for Bronze rows as pure Spark column expressions (D10).

Pydantic (``lakehouse.contracts.models``) is the reference; these rules re-express it on the
Bronze columns so a whole table can be checked in one batch job without a Python UDF. Each
:class:`Rule` has a stable ``code`` and a ``failed`` factory returning a BOOLEAN column that is
TRUE when the row violates the rule and is **never NULL** (every expression is coalesced).

Why two codes per field: ``from_json`` cannot tell a missing field from a field of the wrong JSON
type (both become NULL). ``<field>_null_or_wrong_type`` therefore means "NULL after parsing", and
``<field>_invalid`` means "present but outside the contract" (it never fires on NULL, except
``schema_version_invalid`` which is a ``Literal[1]`` check).

Order matters: the shared envelope rules come first, then the entity rules, then cross-field
business rules. ``_error_category`` of a rejected row is the category of its *first* failing rule.
Reasons contain codes only, never values (PII).

Envelope rules by design (Q-002): a NULL or empty Kafka key fires ``kafka_key_missing`` and a
missing ``brand_id`` header (or one with a NULL value) fires ``brand_header_missing``; the two
``*_mismatch`` rules only compare what is present, so an absent key or header never fires them.
Only the key's brand prefix (text before the first ``:``) is compared, not the entity id; only the
first ``brand_id`` header is read (as Bronze does). These four are transport-level (Kafka) checks
with no Pydantic counterpart. A DLQ row is attributed to the *payload* brand, never to the key or
header.

Regexes are the Pydantic patterns with the final ``$`` replaced by ``\\z``: Java's ``$`` also
matches before a trailing newline (``"brand_x\\n"``), Rust's (Pydantic) does not.

Timestamps (``AwareDatetime``) are accepted when they match an ISO-8601 shape with ``Z`` or a
``+HH:MM`` / ``+HHMM`` offset and Spark can cast them. Measured divergences from Pydantic are
listed in :data:`KNOWN_DIVERGENCES`. Not checked, by design (D-047): unknown JSON fields and
trailing content after the JSON object (``from_json`` ignores both).

PySpark is imported lazily, so this module imports on hosts without it. Rows with
``_parse_error`` set are never rejected here (they stay in Bronze); ``apply_rules`` gives them an
empty code list. ``apply_rules`` needs the Bronze columns ``_kafka_key``, ``_kafka_headers`` and
``_parse_error`` plus the parsed contract fields.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from lakehouse.contracts.enums import CdcAction, ClickEventType, DeviceCategory, LoyaltyTier
from lakehouse.contracts.models import (
    BRAND_ID_PATTERN,
    COUNTRY_PATTERN,
    EMAIL_PATTERN,
    PHONE_E164_PATTERN,
    UUID_V4_PATTERN,
)


if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import Column, DataFrame


class ErrorCategory(StrEnum):
    """Subset of the DLQ ``_error_category`` enum (DATA_CONTRACTS section 3.2) used by D10."""

    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    BUSINESS_RULE_VIOLATION = "BUSINESS_RULE_VIOLATION"


@dataclass(frozen=True)
class Rule:
    """One contract rule: ``failed()`` is a BOOLEAN column, TRUE = the row violates the rule."""

    code: str
    category: ErrorCategory
    retryable: bool
    failed: Callable[[], Column]


KNOWN_DIVERGENCES = (
    # Measured against Pydantic 2 on Spark 3.5.9 (probe of ~100 edge values, see D10 evidence).
    "Stricter here (valid event rejected): epoch-number timestamps, a missing "
    "seconds part ('10:00Z'), offsets beyond +-18:00, and a date given as a datetime string.",
    "Stricter here: from_json NULLs an integer field sent as a string, float or boolean "
    "(_cdc_sequence '5', 5.0, true), reported as <field>_null_or_wrong_type.",
    "Looser here (invalid event accepted): from_json turns a JSON number into text, so a string "
    "field sent as a number passes unless the text breaks a pattern (first_name 5).",
)

# Aware ISO-8601 timestamp shape; Spark's cast then rejects impossible values (month 13, Feb 30).
_ISO_DATE = r"(?!0000)[0-9]{4}-[0-9]{2}-[0-9]{2}"  # Pydantic rejects year 0
_ISO_TIME = r"[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?"
_AWARE_TS_RLIKE = rf"^{_ISO_DATE}[Tt ]{_ISO_TIME}([Zz]|[+-][0-9]{{2}}:?[0-9]{{2}})\z"
_DATE_RLIKE = rf"^{_ISO_DATE}\z"
_BRAND_HEADER = "brand_id"
_EMAIL_MAX = 254
_NAME_MAX = 50


def _java(pattern: str) -> str:
    """Pydantic pattern -> Java regex: ``$`` at the end becomes ``\\z`` (see module docstring)."""
    if not pattern.endswith("$"):
        raise ValueError(f"pattern must end with '$': {pattern!r}")
    return pattern.removesuffix("$") + r"\z"


def _never_null(expr: Column) -> Column:
    from pyspark.sql import functions as F

    return F.coalesce(expr, F.lit(False))


def _present_and(name: str, ok: Callable[[Column], Column]) -> Callable[[], Column]:
    """``failed`` factory: the field is non-NULL and ``ok`` is not TRUE for it."""

    def failed() -> Column:
        from pyspark.sql import functions as F

        col = F.col(name)
        return _never_null(col.isNotNull() & ~_never_null(ok(col)))

    return failed


def _is_null(name: str) -> Callable[[], Column]:
    def failed() -> Column:
        from pyspark.sql import functions as F

        return F.col(name).isNull()

    return failed


def _canonical_ts(col: Column) -> Column:
    """Text Spark's cast can parse: upper-case T/Z and a colon in the offset (Pydantic needs
    neither). Shared by the validity check and by every rule that reads the timestamp's value."""
    from pyspark.sql import functions as F

    return F.regexp_replace(F.upper(col), r"([+-][0-9]{2})([0-9]{2})\z", "$1:$2")


def _aware_ts_ok(col: Column) -> Column:
    from pyspark.sql import functions as F

    return col.rlike(_AWARE_TS_RLIKE) & F.to_timestamp(_canonical_ts(col)).isNotNull()


def _date_ok(col: Column) -> Column:
    return col.rlike(_DATE_RLIKE) & col.cast("date").isNotNull()


def _pattern_ok(pattern: str, max_len: int | None = None) -> Callable[[Column], Column]:
    regex = _java(pattern)

    def ok(col: Column) -> Column:
        from pyspark.sql import functions as F

        matches = col.rlike(regex)
        return matches if max_len is None else (F.length(col) <= max_len) & matches

    return ok


def _length_ok(low: int, high: int) -> Callable[[Column], Column]:
    def ok(col: Column) -> Column:
        from pyspark.sql import functions as F

        return (F.length(col) >= low) & (F.length(col) <= high)

    return ok


def _enum_ok(values: list[str]) -> Callable[[Column], Column]:
    return lambda col: col.isin(values)


def _schema(code: str, failed: Callable[[], Column]) -> Rule:
    return Rule(code, ErrorCategory.SCHEMA_VIOLATION, False, failed)


def _null_rule(prefix: str, column: str) -> Rule:
    return _schema(f"{prefix}_null_or_wrong_type", _is_null(column))


def _invalid_rule(prefix: str, column: str, ok: Callable[[Column], Column]) -> Rule:
    return _schema(f"{prefix}_invalid", _present_and(column, ok))


def _field(prefix: str, column: str, ok: Callable[[Column], Column]) -> list[Rule]:
    """Required field: ``_null_or_wrong_type`` then ``_invalid``."""
    return [_null_rule(prefix, column), _invalid_rule(prefix, column, ok)]


def _decoded_key() -> Column:
    """Kafka key as text; NULL when absent or not valid UTF-8 (then the key rule cannot judge)."""
    from pyspark.sql import functions as F

    key = F.col("_kafka_key")
    text = F.decode(key, "UTF-8")
    return F.when(F.encode(text, "UTF-8") == key, text)


def _kafka_key_present() -> Column:
    from pyspark.sql import functions as F

    return _never_null(F.length(F.col("_kafka_key")) > 0)


def _kafka_key_missing() -> Column:
    return ~_kafka_key_present()


def _brand_key_mismatch() -> Column:
    from pyspark.sql import functions as F

    prefix = F.substring_index(_decoded_key(), ":", 1)
    return _never_null(
        _kafka_key_present() & F.col("brand_id").isNotNull() & (prefix != F.col("brand_id"))
    )


def _brand_header_value() -> Column:
    """Value of the first ``brand_id`` header as text; NULL when absent, NULL-valued or no headers."""
    from pyspark.sql import functions as F

    header = F.filter(F.col("_kafka_headers"), lambda h: h["key"] == F.lit(_BRAND_HEADER))
    return F.decode(header.getItem(0)["value"], "UTF-8")


def _brand_header_missing() -> Column:
    return _brand_header_value().isNull()


def _brand_header_mismatch() -> Column:
    from pyspark.sql import functions as F

    value = _brand_header_value()
    return _never_null(
        F.col("brand_id").isNotNull() & value.isNotNull() & (value != F.col("brand_id"))
    )


def _version_invalid() -> Column:
    from pyspark.sql import functions as F

    return _never_null(F.col("schema_version") != 1) | F.col("schema_version").isNull()


def _envelope_rules() -> list[Rule]:
    return [
        *_field("brand_id", "brand_id", _pattern_ok(BRAND_ID_PATTERN)),
        _schema("kafka_key_missing", _kafka_key_missing),
        _schema("brand_key_mismatch", _brand_key_mismatch),
        _schema("brand_header_missing", _brand_header_missing),
        _schema("brand_header_mismatch", _brand_header_mismatch),
        *_field("event_id", "event_id", _pattern_ok(UUID_V4_PATTERN)),
        _schema("schema_version_invalid", _version_invalid),
        _null_rule("produced_at", "produced_at"),
        _invalid_rule("produced_at", "produced_at", _aware_ts_ok),
    ]


def _signup_after_produced() -> Column:
    """signup_date > UTC date of produced_at; only when both are valid (else other rules fire)."""
    from pyspark.sql import functions as F

    produced = F.col("produced_at")
    signup = F.col("signup_date")
    both_valid = _aware_ts_ok(produced) & _date_ok(signup)
    epoch_days = F.floor(F.to_timestamp(_canonical_ts(produced)).cast("long") / F.lit(86400)).cast(
        "int"
    )
    produced_utc_date = F.date_add(F.lit("1970-01-01").cast("date"), epoch_days)
    return _never_null(both_valid & (signup.cast("date") > produced_utc_date))


def cdc_rules() -> list[Rule]:
    """Rules for ``customer_cdc.v1`` rows (Bronze ``raw_customers``), in evaluation order."""
    return [
        *_envelope_rules(),
        *_field("customer_id", "customer_id", _pattern_ok(UUID_V4_PATTERN)),
        *_field("first_name", "first_name", _length_ok(1, _NAME_MAX)),
        *_field("last_name", "last_name", _length_ok(1, _NAME_MAX)),
        *_field("email", "email", _pattern_ok(EMAIL_PATTERN, _EMAIL_MAX)),
        _invalid_rule("phone", "phone", _pattern_ok(PHONE_E164_PATTERN)),
        *_field("country", "country", _pattern_ok(COUNTRY_PATTERN)),
        *_field("loyalty_tier", "loyalty_tier", _enum_ok([m.value for m in LoyaltyTier])),
        *_field("signup_date", "signup_date", _date_ok),
        *_field("cdc_action", "_cdc_action", _enum_ok([m.value for m in CdcAction])),
        _null_rule("cdc_sequence", "_cdc_sequence"),
        _schema("cdc_sequence_invalid", _present_and("_cdc_sequence", lambda c: c >= 1)),
        *_field("cdc_ts", "_cdc_ts", _aware_ts_ok),
        Rule(
            "signup_after_produced",
            ErrorCategory.BUSINESS_RULE_VIOLATION,
            False,
            _signup_after_produced,
        ),
    ]


def clickstream_rules() -> list[Rule]:
    """Rules for ``clickstream_event.v1`` rows (Bronze ``raw_clickstream``), in order."""
    return [
        *_envelope_rules(),
        *_field("session_id", "session_id", _pattern_ok(UUID_V4_PATTERN)),
        _invalid_rule("customer_id", "customer_id", _pattern_ok(UUID_V4_PATTERN)),
        *_field("event_type", "event_type", _enum_ok([m.value for m in ClickEventType])),
        *_field("device_category", "device_category", _enum_ok([m.value for m in DeviceCategory])),
        *_field("event_timestamp", "event_timestamp", _aware_ts_ok),
    ]


RULES_BY_TABLE: dict[str, Callable[[], list[Rule]]] = {
    "raw_customers": cdc_rules,
    "raw_clickstream": clickstream_rules,
}


def apply_rules(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Add ``_rejection_codes``, ``_error_category``, ``_rejection_reason``, ``_retryable``.

    Codes are listed in rule order (empty array when the row passes); the category is that of the
    first failing rule; ``_retryable`` is TRUE iff any failing rule is retryable; the reason is the
    codes joined by ``", "`` (never values). The last three are NULL for a passing row. Rows with
    ``_parse_error`` set are passed through as passing.
    """
    from pyspark.sql import functions as F

    if not rules:
        raise ValueError("apply_rules needs at least one rule")
    parsed_ok = F.col("_parse_error").isNull()
    hits = F.filter(
        F.array(
            *[
                F.when(
                    parsed_ok & r.failed(),
                    F.struct(
                        F.lit(r.code).alias("code"),
                        F.lit(r.category.value).alias("category"),
                        F.lit(r.retryable).alias("retryable"),
                    ),
                )
                for r in rules
            ]
        ),
        lambda h: h.isNotNull(),
    )
    with_hits = df.withColumn("_hits", hits)
    failing = F.size(F.col("_hits")) > 0
    return (
        with_hits.withColumn("_rejection_codes", F.transform("_hits", lambda h: h["code"]))
        .withColumn("_error_category", F.when(failing, F.col("_hits").getItem(0)["category"]))
        .withColumn(
            "_rejection_reason", F.when(failing, F.array_join(F.col("_rejection_codes"), ", "))
        )
        .withColumn("_retryable", F.when(failing, F.exists("_hits", lambda h: h["retryable"])))
        .drop("_hits")
    )
