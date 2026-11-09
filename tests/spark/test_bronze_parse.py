"""Bronze parsing and idempotent batch writes (W2.D8.2), derived from DATA_CONTRACTS 2.0/3.1.

Runs inside the Spark image (``docker compose --profile tools run --rm spark-test``).
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import pytest


pytest.importorskip("pyspark")

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

from lakehouse.ingestion.bronze import (  # noqa: E402
    CLICKSTREAM,
    CUSTOMERS,
    PARSE_INVALID_UTF8,
    PARSE_MALFORMED_JSON,
    PARSE_NULL_VALUE,
    BronzeTarget,
    to_bronze_rows,
    write_bronze_batch,
)


if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame, Row, SparkSession


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

CDC_FIELDS = [
    "event_id",
    "customer_id",
    "brand_id",
    "schema_version",
    "produced_at",
    "first_name",
    "last_name",
    "email",
    "phone",
    "city",
    "country",
    "loyalty_tier",
    "signup_date",
    "_cdc_action",
    "_cdc_sequence",
    "_cdc_ts",
]
CLICK_FIELDS = [
    "event_id",
    "brand_id",
    "schema_version",
    "produced_at",
    "session_id",
    "customer_id",
    "event_type",
    "page_url",
    "device_category",
    "event_timestamp",
]
META_COLUMNS = {
    "_raw_value",
    "_kafka_key",
    "_kafka_headers",
    "_kafka_ts",
    "_source_ref",
    "_ingest_ts",
    "_batch_id",
    "_parse_error",
}

CDC_PAYLOAD: dict[str, Any] = {
    "event_id": "11111111-1111-4111-8111-111111111111",
    "customer_id": "22222222-2222-4222-8222-222222222222",
    "brand_id": "brand_alpha",
    "schema_version": 1,
    "produced_at": "2026-01-01T10:00:00.123Z",
    "first_name": "Ada",
    "last_name": "Lovelace",
    "email": "ada@example.com",
    "phone": None,
    "city": "Berlin",
    "country": "DE",
    "loyalty_tier": "Gold",
    "signup_date": "2025-12-01",
    "_cdc_action": "I",
    "_cdc_sequence": 7,
    "_cdc_ts": "2026-01-01T10:00:00.000Z",
}
CLICK_PAYLOAD: dict[str, Any] = {
    "event_id": "33333333-3333-4333-8333-333333333333",
    "brand_id": "brand_beta",
    "schema_version": 1,
    "produced_at": "2026-01-01T10:00:00.500Z",
    "session_id": "44444444-4444-4444-8444-444444444444",
    "customer_id": None,
    "event_type": "page_view",
    "page_url": "/home",
    "device_category": "mobile",
    "event_timestamp": "2026-01-01T09:59:59.000Z",
}
MALFORMED_TEXTS = [
    "",
    " ",
    "null",
    "123",
    '"x"',
    "[1,2]",
    '[{"a":"1"}]',
    '{"event_id":"x","brand_id":"brand_alp',
]

Headers = list[tuple[str, bytes | None]]


def _json(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _brand_header(brand: str) -> Headers:
    return [("brand_id", brand.encode("utf-8"))]


def _kafka(
    spark: SparkSession,
    values: list[bytes | None],
    *,
    headers: list[Headers | None] | None = None,
    topic: str = "t",
    kafka_ts: datetime = KAFKA_TS,
) -> DataFrame:
    rows = [
        (
            b"key-%d" % i,
            value,
            topic,
            i % 3,
            100 + i,
            kafka_ts,
            None if headers is None else headers[i],
        )
        for i, value in enumerate(values)
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def _one(
    spark: SparkSession,
    target: BronzeTarget,
    value: bytes | None,
    headers: Headers | None = None,
    **kwargs: Any,
) -> Row:
    df = _kafka(spark, [value], headers=[headers], **kwargs)
    rows = to_bronze_rows(df, target, "batch-1").collect()
    assert len(rows) == 1  # a row is never dropped
    return rows[0]


def _fields(target: BronzeTarget) -> list[str]:
    return CDC_FIELDS if target is CUSTOMERS else CLICK_FIELDS


# ---- valid ----------------------------------------------------------------------------------


def test_valid_customer_event_populates_contract_fields_and_metadata(spark: SparkSession) -> None:
    raw = _json(CDC_PAYLOAD)
    row = _one(spark, CUSTOMERS, raw, _brand_header("brand_alpha"), topic="customer_cdc")
    assert row["_parse_error"] is None
    assert row["_raw_value"] == raw.decode("utf-8")
    assert row["event_id"] == CDC_PAYLOAD["event_id"]
    assert row["customer_id"] == CDC_PAYLOAD["customer_id"]
    assert row["brand_id"] == "brand_alpha"
    assert row["schema_version"] == 1
    assert row["_cdc_action"] == "I"
    assert row["_cdc_sequence"] == 7
    assert row["email"] == "ada@example.com"
    assert row["phone"] is None
    assert row["city"] == "Berlin"
    assert row["produced_at"] == CDC_PAYLOAD["produced_at"]  # timestamps stay strings
    assert row["_cdc_ts"] == CDC_PAYLOAD["_cdc_ts"]
    assert str(row["signup_date"]) == "2025-12-01"
    assert bytes(row["_kafka_key"]) == b"key-0"
    assert row["_kafka_ts"] is not None
    assert row["_source_ref"] == "customer_cdc/0/100"
    assert row["_batch_id"] == "batch-1"
    assert row["_ingest_ts"] is not None


def test_valid_clickstream_event_populates_contract_fields(spark: SparkSession) -> None:
    row = _one(spark, CLICKSTREAM, _json(CLICK_PAYLOAD), _brand_header("brand_beta"))
    assert row["_parse_error"] is None
    for name in CLICK_FIELDS:
        assert row[name] == CLICK_PAYLOAD[name], name
    assert row["event_date"] == date(2026, 1, 1)


def test_output_columns_are_contract_fields_plus_metadata(spark: SparkSession) -> None:
    cdc = to_bronze_rows(_kafka(spark, [_json(CDC_PAYLOAD)]), CUSTOMERS, "b").columns
    click = to_bronze_rows(_kafka(spark, [_json(CLICK_PAYLOAD)]), CLICKSTREAM, "b").columns
    assert set(CDC_FIELDS) | META_COLUMNS <= set(cdc)
    assert "event_date" not in cdc
    assert set(CLICK_FIELDS) | META_COLUMNS | {"event_date"} <= set(click)


def test_headers_are_preserved_as_given(spark: SparkSession) -> None:
    headers: Headers = [("brand_id", b"brand_alpha"), ("event_type", b"x")]
    row = _one(spark, CUSTOMERS, _json(CDC_PAYLOAD), headers)
    assert [(h["key"], bytes(h["value"])) for h in row["_kafka_headers"]] == headers


def test_payload_brand_wins_over_disagreeing_header(spark: SparkSession) -> None:
    row = _one(spark, CUSTOMERS, _json(CDC_PAYLOAD), _brand_header("brand_zeta"))
    assert row["brand_id"] == "brand_alpha"
    assert row["_parse_error"] is None


# ---- parse errors ---------------------------------------------------------------------------


def test_null_value_tombstone_is_kept_with_null_value_error(spark: SparkSession) -> None:
    row = _one(spark, CUSTOMERS, None, _brand_header("brand_alpha"))
    assert PARSE_NULL_VALUE == "null_value"
    assert row["_parse_error"] == PARSE_NULL_VALUE
    assert row["_raw_value"] is None
    assert row["_source_ref"] == "t/0/100"


def test_invalid_utf8_is_kept_as_base64_raw_value(spark: SparkSession) -> None:
    row = _one(spark, CUSTOMERS, b"\xff\xfeabc", _brand_header("brand_alpha"))
    assert PARSE_INVALID_UTF8 == "invalid_utf8"
    assert row["_parse_error"] == PARSE_INVALID_UTF8
    assert row["_raw_value"] == "//5hYmM="
    assert base64.b64decode(row["_raw_value"]) == b"\xff\xfeabc"
    assert row["brand_id"] == "brand_alpha"  # header fallback


def test_genuine_replacement_character_is_not_invalid_utf8(spark: SparkSession) -> None:
    payload = {**CDC_PAYLOAD, "first_name": "A�B"}
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    row = _one(spark, CUSTOMERS, raw)
    assert row["_parse_error"] is None
    assert row["first_name"] == "A�B"
    assert row["_raw_value"] == raw.decode("utf-8")


@pytest.mark.parametrize("target", [CUSTOMERS, CLICKSTREAM], ids=["customers", "clickstream"])
@pytest.mark.parametrize("text", MALFORMED_TEXTS)
def test_malformed_json_is_flagged_and_every_contract_field_is_null(
    spark: SparkSession, target: BronzeTarget, text: str
) -> None:
    row = _one(spark, target, text.encode("utf-8"))
    assert PARSE_MALFORMED_JSON == "malformed_json"
    assert row["_parse_error"] == PARSE_MALFORMED_JSON
    assert row["_raw_value"] == text
    assert all(row[name] is None for name in _fields(target)), text


def test_invalid_utf8_takes_precedence_over_malformed_json(spark: SparkSession) -> None:
    row = _one(spark, CUSTOMERS, b"\xff")
    assert row["_parse_error"] == PARSE_INVALID_UTF8


def test_empty_object_is_parseable_with_null_brand(spark: SparkSession) -> None:
    row = _one(spark, CUSTOMERS, b"{}", _brand_header("brand_alpha"))
    assert row["_parse_error"] is None
    assert row["brand_id"] is None
    assert row["_raw_value"] == "{}"


def test_known_limit_trailing_garbage_after_json_is_not_flagged(spark: SparkSession) -> None:
    """Measured limitation (pinned): a valid JSON prefix followed by junk is NOT a parse error."""
    raw = '{"event_id":"x"} xyz'
    row = _one(spark, CUSTOMERS, raw.encode("utf-8"))
    assert row["_parse_error"] is None
    assert row["_raw_value"] == raw


def test_wrongly_typed_long_field_is_null_but_row_and_other_fields_survive(
    spark: SparkSession,
) -> None:
    row = _one(spark, CUSTOMERS, _json({**CDC_PAYLOAD, "_cdc_sequence": "abc"}))
    assert row["_parse_error"] is None  # D-045: a contract violation is the DLQ gate's business
    assert row["_cdc_sequence"] is None
    assert row["event_id"] == CDC_PAYLOAD["event_id"]
    assert row["brand_id"] == "brand_alpha"


# ---- brand_id fallback ----------------------------------------------------------------------


def test_malformed_uses_header_brand(spark: SparkSession) -> None:
    row = _one(spark, CUSTOMERS, b'{"brand_id":', _brand_header("brand_beta"))
    assert row["_parse_error"] == PARSE_MALFORMED_JSON
    assert row["brand_id"] == "brand_beta"


def test_malformed_with_several_brand_headers_uses_the_first(spark: SparkSession) -> None:
    headers: Headers = [
        ("event_type", b"x"),
        ("brand_id", b"brand_first"),
        ("brand_id", b"brand_second"),
    ]
    assert _one(spark, CUSTOMERS, b"[1,2]", headers)["brand_id"] == "brand_first"


@pytest.mark.parametrize(
    "headers",
    [
        [("brand_id", None)],
        [("brand_id", b"")],
        [],
        [("event_type", b"customer_cdc")],
        None,
    ],
    ids=["null-value", "empty-value", "no-headers", "other-header-only", "null-headers"],
)
def test_malformed_without_usable_brand_header_has_null_brand_and_row_is_kept(
    spark: SparkSession, headers: Headers | None
) -> None:
    row = _one(spark, CUSTOMERS, b"{oops", headers)
    assert row["_parse_error"] == PARSE_MALFORMED_JSON
    assert row["brand_id"] is None


def test_header_does_not_mask_a_missing_payload_brand(spark: SparkSession) -> None:
    payload = {k: v for k, v in CDC_PAYLOAD.items() if k != "brand_id"}
    row = _one(spark, CUSTOMERS, _json(payload), _brand_header("brand_alpha"))
    assert row["_parse_error"] is None
    assert row["brand_id"] is None


# ---- event_date -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("event_ts", "expected"),
    [
        ("2026-01-01T23:30:00-05:00", date(2026, 1, 2)),
        ("2026-01-01T23:30:00Z", date(2026, 1, 1)),
        ("2026-01-02T00:30:00+02:00", date(2026, 1, 1)),
    ],
)
def test_event_date_is_the_utc_date_of_event_timestamp(
    spark: SparkSession, event_ts: str, expected: date
) -> None:
    payload = {**CLICK_PAYLOAD, "event_timestamp": event_ts}
    row = _one(spark, CLICKSTREAM, _json(payload), kafka_ts=datetime(2019, 6, 1, tzinfo=UTC))
    assert row["event_date"] == expected


@pytest.mark.parametrize(
    "payload_bytes",
    [
        _json({**CLICK_PAYLOAD, "event_timestamp": "not-a-timestamp"}),
        _json({k: v for k, v in CLICK_PAYLOAD.items() if k != "event_timestamp"}),
        _json({**CLICK_PAYLOAD, "event_timestamp": None}),
        b'{"event_id":"trunc',
        None,
    ],
    ids=["garbage", "missing", "null", "parse-error", "tombstone"],
)
def test_event_date_falls_back_to_the_kafka_timestamp_date(
    spark: SparkSession, payload_bytes: bytes | None
) -> None:
    kafka_ts = datetime(2019, 6, 1, 8, 0, 0, tzinfo=UTC)  # years before any wall-clock "today"
    row = _one(spark, CLICKSTREAM, payload_bytes, kafka_ts=kafka_ts)
    assert row["event_date"] == date(2019, 6, 1)


# ---- write_bronze_batch ---------------------------------------------------------------------


def _mixed_batch(spark: SparkSession) -> DataFrame:
    values: list[bytes | None] = [_json(CDC_PAYLOAD), b"{oops", None]
    return _kafka(spark, values, headers=[_brand_header("brand_alpha")] * 3, topic="customer_cdc")


def _count(spark: SparkSession, path: Path) -> int:
    return spark.read.format("delta").load(str(path)).count()


def _write(spark: SparkSession, path: Path, app_id: str, batch_id: int) -> Any:
    return write_bronze_batch(
        _mixed_batch(spark), str(path), target=CUSTOMERS, app_id=app_id, batch_id=batch_id
    )


def test_write_reports_rows_seen_and_parse_errors_and_persists_rows(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = tmp_path / "raw_customers"
    counts = _write(spark, path, "app-a", 0)
    assert (counts.rows_seen, counts.parse_errors) == (3, 2)
    table = spark.read.format("delta").load(str(path))
    assert table.count() == 3
    assert table.filter("_parse_error IS NOT NULL").count() == 2
    null_meta = "_source_ref IS NULL OR _ingest_ts IS NULL OR _batch_id IS NULL"
    assert table.filter(null_meta).count() == 0


def test_replayed_batch_with_same_app_and_batch_id_adds_no_rows(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = tmp_path / "raw_customers"
    first = _write(spark, path, "app-a", 4)
    replay = _write(spark, path, "app-a", 4)
    assert _count(spark, path) == 3
    assert (first.rows_seen, first.parse_errors) == (3, 2)
    assert (replay.rows_seen, replay.parse_errors) == (3, 2)


def test_same_batch_id_under_a_new_app_id_is_appended_new_checkpoint_lineage(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Negative for the idempotency test above: the guard keys on (app_id, batch_id)."""
    path = tmp_path / "raw_customers"
    _write(spark, path, "app-a", 0)
    _write(spark, path, "app-b", 0)
    assert _count(spark, path) == 6


def test_higher_batch_id_with_same_app_id_is_appended(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "raw_customers"
    _write(spark, path, "app-a", 0)
    _write(spark, path, "app-a", 1)
    assert _count(spark, path) == 6


def test_clickstream_batch_is_partitioned_by_event_date(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = tmp_path / "raw_clickstream"
    second = {**CLICK_PAYLOAD, "event_timestamp": "2026-02-03T01:00:00Z"}
    df = _kafka(
        spark,
        [_json(CLICK_PAYLOAD), _json(second)],
        headers=[_brand_header("brand_beta")] * 2,
        topic="clickstream_events",
    )
    counts = write_bronze_batch(df, str(path), target=CLICKSTREAM, app_id="app-a", batch_id=0)
    assert (counts.rows_seen, counts.parse_errors) == (2, 0)
    partitions = {p.name for p in path.iterdir() if p.name.startswith("event_date=")}
    assert partitions == {"event_date=2026-01-01", "event_date=2026-02-03"}
