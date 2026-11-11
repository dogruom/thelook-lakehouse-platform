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

That key is only safe while the checkpoint and the table agree. If the checkpoint's ``offsets/``
and ``commits/`` are lost but its ``metadata`` (query id) survives, batch ids restart at 0 and
Delta would silently skip *new* data whose (app id, batch id) is already recorded, while the offsets
move on: permanent data loss. ``write_bronze_batch`` therefore checks every skipped write (table
version unchanged by the save): the distinct ``_source_ref`` set of the incoming batch must equal
the set already stored for that ``_batch_id``. Equal means a true replay (crash between the Delta
commit and the offset commit) and passes; different raises :class:`CheckpointRewoundError`, which
fails the query before the offset commit. Normal writes cost no extra table scan. A single writer
per table is assumed.

PySpark is imported lazily so this module imports on hosts without it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakehouse.common.spark import struct_type_for


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


PARSE_NULL_VALUE = "null_value"
PARSE_INVALID_UTF8 = "invalid_utf8"
PARSE_MALFORMED_JSON = "malformed_json"

_BRAND_HEADER = "brand_id"


class CheckpointRewoundError(RuntimeError):
    """The checkpoint no longer matches the Bronze lineage: Delta skipped a batch with new data."""


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


def _table_version(spark: SparkSession, path: str) -> int | None:
    """Latest Delta version of the table at ``path``; ``None`` when there is no table yet."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return int(DeltaTable.forPath(spark, path).history(1).collect()[0]["version"])


def _check_skipped_write_is_replay(
    spark: SparkSession, rows: DataFrame, path: str, *, app_id: str, batch_id: int
) -> None:
    """Raise unless the skipped write is a true replay of the stored batch (same message set)."""
    from pyspark.sql import functions as F

    incoming = rows.select("_source_ref").distinct()
    stored = (
        spark.read.format("delta")
        .load(path)
        .where(F.col("_batch_id") == f"{app_id}:{batch_id}")
        .select("_source_ref")
        .distinct()
    )
    if incoming.exceptAll(stored).isEmpty() and stored.exceptAll(incoming).isEmpty():
        return
    raise CheckpointRewoundError(
        f"checkpoint rewound: Delta skipped batch {batch_id} of app {app_id} for {path} because "
        f"it is already recorded, but the incoming batch ({incoming.count()} distinct "
        f"_source_ref) differs from the stored one ({stored.count()} distinct _source_ref). "
        "The checkpoint no longer matches the Bronze lineage; the new rows were NOT written and "
        "the offsets were not committed. Restore the checkpoint or start a new lineage "
        "(fresh checkpoint path); do not delete only its offsets/commits."
    )


def append_idempotent(
    rows: DataFrame,
    path: str,
    *,
    app_id: str,
    batch_id: int,
    partition_by: tuple[str, ...] = (),
    merge_schema: bool = False,
) -> None:
    """Append Bronze ``rows`` once per (``app_id``, ``batch_id``) and guard a skipped write.

    ``rows`` must carry ``_source_ref`` and a ``_batch_id`` of ``<app_id>:<batch_id>``. A write that
    Delta skipped (table version unchanged) must be a true replay of the stored batch, else
    :class:`CheckpointRewoundError`. ``merge_schema`` lets an additive column evolve the table
    (file sources); the Kafka path keeps it off so that a drift fails loudly.
    """
    writer = (
        rows.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
    )
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    spark = rows.sparkSession
    version_before = _table_version(spark, path)
    writer.save(path)
    if _table_version(spark, path) == version_before:
        _check_skipped_write_is_replay(spark, rows, path, app_id=app_id, batch_id=batch_id)


def write_bronze_batch(
    batch_df: DataFrame, path: str, *, target: BronzeTarget, app_id: str, batch_id: int
) -> BatchCounts:
    """Append one micro-batch (a raw Kafka frame) to the Bronze Delta table at ``path``.

    ``app_id`` and ``batch_id`` form Delta's idempotent-write key: the same pair is written once.
    No ``mergeSchema``: a schema drift must fail loudly. A write that Delta skipped (version
    unchanged) must be a true replay of the stored batch, else :class:`CheckpointRewoundError`.
    """
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        rows = to_bronze_rows(batch_df, target, f"{app_id}:{batch_id}")
        append_idempotent(
            rows, path, app_id=app_id, batch_id=batch_id, partition_by=target.partition_by
        )
        counts = rows.agg(
            F.count(F.lit(1)).alias("seen"), F.count(F.col("_parse_error")).alias("errors")
        ).first()
        if counts is None:  # an aggregate without grouping always returns one row
            raise RuntimeError("count aggregate returned no row")
        return BatchCounts(rows_seen=int(counts["seen"]), parse_errors=int(counts["errors"]))
    finally:
        batch_df.unpersist()
