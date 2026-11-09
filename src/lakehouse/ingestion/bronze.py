"""Bronze transforms: one Kafka message becomes exactly one Bronze row (append-only, lossless).

Bronze keeps the message as it arrived. Nothing is dropped or "fixed" here:

* ``_raw_value`` holds the payload text (base64 of the bytes when the bytes are not valid UTF-8,
  NULL for a Kafka tombstone), so any row can be re-parsed later.
* The contract fields are parsed with ``from_json`` into top-level columns. Money and timestamps
  stay strings (D-045); typing them is a Silver step. Unknown JSON fields are ignored.
* A message that cannot be parsed still becomes a row. ``_parse_error`` names the reason and every
  parsed contract field is NULL. Contract violations (missing fields, bad patterns) are *not*
  parse errors; the quality gate (D10) sends those to the DLQ using ``_source_ref`` as the
  ``_bronze_ref``.
* ``_source_ref`` = ``topic/partition/offset`` identifies the message; ``_batch_id`` identifies the
  micro-batch (``<query id>:<batch id>``); ``_ingest_ts`` is the write time.

Parse error codes, in precedence order: ``null_value`` -> ``invalid_utf8`` -> ``malformed_json``.

Known limit (measured on Spark 3.5): ``from_json`` accepts trailing garbage after the first JSON
object (``{"a":"1"} xyz`` parses, the raw value is retained), so such a row is not flagged.

``brand_id`` comes from the payload only when the row parsed. A parseable row without a payload
``brand_id`` keeps NULL on purpose: a header must not mask a contract violation. Only for rows that
failed to parse, the ``brand_id`` header is used so the row can still be attributed to a tenant.

``write_bronze_batch`` is separate from the stream wiring so that a test can inject a failure after
the write and replay the same batch. Replays are idempotent through Delta's ``txnAppId`` /
``txnVersion``: the app id must be stable for one checkpoint lineage (the job uses the streaming
query id, which is stored in the checkpoint) and the version is the micro-batch id.

PySpark is imported lazily so this module imports on hosts without it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakehouse.common.spark import struct_type_for


if TYPE_CHECKING:
    from pyspark.sql import DataFrame


PARSE_NULL_VALUE = "null_value"
PARSE_INVALID_UTF8 = "invalid_utf8"
PARSE_MALFORMED_JSON = "malformed_json"

_BRAND_HEADER = "brand_id"


@dataclass(frozen=True)
class BronzeTarget:
    """One Kafka topic and the Bronze table it feeds."""

    topic: str
    table: str
    contract: str
    partition_by: tuple[str, ...] = ()


@dataclass(frozen=True)
class BatchCounts:
    """Rows seen in one micro-batch (not "written": a replayed batch is skipped by Delta)."""

    rows_seen: int
    parse_errors: int


CUSTOMERS = BronzeTarget("customer_cdc", "raw_customers", "customer_cdc.v1")
CLICKSTREAM = BronzeTarget(
    "clickstream_events", "raw_clickstream", "clickstream_event.v1", ("event_date",)
)


def to_bronze_rows(kafka_df: DataFrame, target: BronzeTarget, batch_id: str) -> DataFrame:
    """Turn a Kafka-source frame into Bronze rows, one row per input row (see module docstring)."""
    from pyspark.sql import functions as F

    schema = struct_type_for(target.contract)
    value = F.col("value")
    text = F.decode(value, "UTF-8")
    # `cast(binary as string)` does not replace invalid bytes in Spark 3.5, so compare against a
    # decode/encode round trip. A genuine U+FFFD payload is valid UTF-8 and round-trips unchanged.
    invalid_utf8 = F.encode(text, "UTF-8") != value
    malformed = F.from_json(text, "map<string,string>").isNull()
    error = (
        F.when(value.isNull(), F.lit(PARSE_NULL_VALUE))
        .when(invalid_utf8, F.lit(PARSE_INVALID_UTF8))
        .when(malformed, F.lit(PARSE_MALFORMED_JSON))
    )

    parsed = F.when(error.isNull(), F.from_json(text, schema)).alias("_parsed")
    first_header = F.filter(F.col("headers"), lambda h: h["key"] == F.lit(_BRAND_HEADER)).getItem(0)
    header_brand = F.decode(first_header["value"], "UTF-8")
    header_brand = F.when(F.length(header_brand) > 0, header_brand)

    base = kafka_df.select(
        parsed,
        error.alias("_parse_error"),
        F.when(value.isNull(), F.lit(None).cast("string"))
        .when(invalid_utf8, F.base64(value))
        .otherwise(text)
        .alias("_raw_value"),
        F.col("key").alias("_kafka_key"),
        F.col("headers").alias("_kafka_headers"),
        F.col("timestamp").alias("_kafka_ts"),
        F.concat(
            F.col("topic"),
            F.lit("/"),
            F.col("partition").cast("string"),
            F.lit("/"),
            F.col("offset").cast("string"),
        ).alias("_source_ref"),
        F.current_timestamp().alias("_ingest_ts"),
        F.lit(batch_id).alias("_batch_id"),
        header_brand.alias("_header_brand"),
    )

    columns = []
    for name in schema.fieldNames():
        if name == "brand_id":
            columns.append(
                F.when(F.col("_parse_error").isNull(), F.col("_parsed.brand_id"))
                .otherwise(F.col("_header_brand"))
                .alias("brand_id")
            )
        else:
            columns.append(F.col(f"_parsed.{name}").alias(name))
    if "event_date" in target.partition_by:
        # Deterministic across reloads: payload time, else the Kafka record time; never wall clock.
        columns.append(
            F.coalesce(
                F.to_date(F.to_timestamp(F.col("_parsed.event_timestamp"))),
                F.to_date(F.col("_kafka_ts")),
            ).alias("event_date")
        )
    columns += [
        F.col(name)
        for name in (
            "_raw_value",
            "_kafka_key",
            "_kafka_headers",
            "_kafka_ts",
            "_source_ref",
            "_ingest_ts",
            "_batch_id",
            "_parse_error",
        )
    ]
    return base.select(*columns)


def write_bronze_batch(
    batch_df: DataFrame, path: str, *, target: BronzeTarget, app_id: str, batch_id: int
) -> BatchCounts:
    """Append one micro-batch (a raw Kafka frame) to the Bronze Delta table at ``path``.

    ``app_id`` and ``batch_id`` form Delta's idempotent-write key: the same pair is written once.
    No ``mergeSchema``: a schema drift must fail loudly.
    """
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        rows = to_bronze_rows(batch_df, target, f"{app_id}:{batch_id}")
        writer = (
            rows.write.format("delta")
            .mode("append")
            .option("txnAppId", app_id)
            .option("txnVersion", batch_id)
        )
        if target.partition_by:
            writer = writer.partitionBy(*target.partition_by)
        writer.save(path)
        counts = rows.agg(
            F.count(F.lit(1)).alias("seen"), F.count(F.col("_parse_error")).alias("errors")
        ).first()
        if counts is None:  # an aggregate without grouping always returns one row
            raise RuntimeError("count aggregate returned no row")
        return BatchCounts(rows_seen=int(counts["seen"]), parse_errors=int(counts["errors"]))
    finally:
        batch_df.unpersist()
