"""Bronze transforms for file-landed entities: one NDJSON line becomes exactly one Bronze row.

The landing zone holds ``landing/{brand_id}/orders/{date}/batch-{NNNNN}.ndjson`` and
``landing/{brand_id}/products/{date}/products.ndjson`` (one JSON object per line). Bronze keeps
every line as it arrived; nothing is dropped or "fixed" here:

* ``_raw_value`` is the original line. The contract fields are parsed with ``from_json`` into
  top-level columns. Money and timestamps stay strings (D-045); typing them is a Silver step.
  Unknown JSON fields are ignored by the parse, so a field outside the contract stays visible only
  in ``_raw_value``.
* A line that cannot be parsed still becomes a row: ``_parse_error`` is ``malformed_json`` (the
  stable code of D-047) and every parsed contract field is NULL. Contract violations are not parse
  errors; the quality gate sends them to the DLQ with ``_source_ref`` as ``_bronze_ref``.
  ``null_value`` cannot occur for a file line. ``invalid_utf8`` is not emitted either: the text
  source decodes the file before this code sees it, so a line with invalid bytes surfaces as
  ``malformed_json`` or as a parsed row whose ``_raw_value`` carries replacement characters.
* ``_source_ref`` = ``landing/[<namespace>/]{brand_id}/{entity}/{date}/{file}#L<n>``: the bucket-relative key
  plus the 1-based line number (DATA_CONTRACTS section 3.1). Line numbers are exact because the
  file is read whole (``wholetext``) and split here; the text source has no line numbers.
* ``brand_id`` is the **path segment**: the landing prefix is the tenant boundary. The brand the
  payload claims is kept in ``payload_brand_id`` so the DLQ gate can compare the two. (The Kafka
  tables take ``brand_id`` from the payload; here both exist because the path is trustworthy and
  the payload is data.)

A file is expected to end with ``\\n``: the empty string after the last newline is not a line. A
blank line in the middle is a line (``malformed_json``). Files are read whole, so one file must fit
in executor memory; landing files are a few MB at most.

``write_bronze_files`` appends with the shared idempotent write (Delta ``txnAppId`` = streaming
query id, ``txnVersion`` = micro-batch id) and ``mergeSchema=true``. ``sales_channel`` is not an
evolution event here: the reader projects the contract schema (``order_event.v1``), which already
contains the column, so it exists from batch 1 and is NULL until batch 10. Unknown JSON fields are
dropped from the columns and stay only in ``_raw_value``. ``mergeSchema`` matters when a new
contract version adds a column to an existing table (see the Spark evolution test).

Observed behaviour for the W3 DLQ gate (Spark 3.5, ``from_json`` PERMISSIVE with partial results,
pinned by a Spark test): valid JSON whose values have the wrong type is **not** a parse error. The
row is kept, ``_raw_value`` is intact, ``_parse_error`` is NULL and only the mistyped field is NULL
(``items`` as a string -> ``items`` NULL; ``schema_version: "x"`` -> NULL; ``quantity: "abc"`` inside
an item -> that item's ``quantity`` NULL, the rest of the item and order is parsed). The gate must
therefore detect such rows by NULL contract fields, or by re-validating ``_raw_value``. A 0-byte
``.ndjson`` file produces no row, no micro-batch and no table.

The checkpoint tracks file paths, not content: overwriting an already ingested landing key with
different bytes is not re-read. Landing keys are immutable by convention (the generator is
deterministic and writes each key once).

PySpark is imported lazily so this module imports on hosts without it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakehouse.common.spark import struct_type_for
from lakehouse.ingestion.bronze import PARSE_MALFORMED_JSON, append_idempotent


if TYPE_CHECKING:
    from collections.abc import Mapping

    from pyspark.sql import DataFrame

FILE_PATH_COLUMN = "_file_path"
PAYLOAD_BRAND_COLUMN = "payload_brand_id"
UNKNOWN_BRAND = "<unknown>"


@dataclass(frozen=True)
class BatchTarget:
    """One landing entity and the Bronze table it feeds."""

    entity: str
    table: str
    contract: str
    partition_by: tuple[str, ...] = ()


@dataclass(frozen=True)
class BrandCounts:
    """Files, lines and parse errors of one brand in one micro-batch."""

    files: int
    lines: int
    parse_errors: int


@dataclass(frozen=True)
class FileBatchCounts:
    """Rows seen in one micro-batch (not "written": a replayed batch is skipped by Delta)."""

    files: int
    lines: int
    parse_errors: int
    per_brand: Mapping[str, BrandCounts]


ORDERS = BatchTarget("orders", "raw_orders", "order_event.v1", ("brand_id",))
PRODUCTS = BatchTarget("products", "raw_products", "product.v1")


def to_bronze_rows(files_df: DataFrame, target: BatchTarget, batch_id: str) -> DataFrame:
    """Turn whole-file rows (``_file_path``, ``value``) into Bronze rows, one per line."""
    from pyspark.sql import functions as F

    schema = struct_type_for(target.contract)
    lines = F.split(F.col("value"), "\n")
    exploded = files_df.select(
        F.col(FILE_PATH_COLUMN),
        F.size(lines).alias("_n"),
        F.posexplode(lines).alias("_pos", "_line"),
    ).where(~((F.col("_pos") == F.col("_n") - 1) & (F.col("_line") == "")))

    # Optional namespace segment (``landing/<ns>/<brand>/...``, isolated test runs, D-069): the
    # brand is always the segment directly before the entity.
    key_pattern = rf"(landing/(?:[^/]+/)?[^/]+/{target.entity}/[^/]+/[^/]+\.ndjson)$"
    brand_pattern = rf"landing/(?:[^/]+/)?([^/]+)/{target.entity}/[^/]+/[^/]+\.ndjson$"
    path = F.col(FILE_PATH_COLUMN)
    key = F.regexp_extract(path, key_pattern, 1)
    brand = F.regexp_extract(path, brand_pattern, 1)
    line = F.col("_line")
    error = F.when(F.from_json(line, "map<string,string>").isNull(), F.lit(PARSE_MALFORMED_JSON))

    base = exploded.select(
        F.when(error.isNull(), F.from_json(line, schema)).alias("_parsed"),
        error.alias("_parse_error"),
        line.alias("_raw_value"),
        F.concat(
            F.coalesce(F.when(F.length(key) > 0, key), path),
            F.lit("#L"),
            (F.col("_pos") + 1).cast("string"),
        ).alias("_source_ref"),
        F.current_timestamp().alias("_ingest_ts"),
        F.lit(batch_id).alias("_batch_id"),
        F.when(F.length(brand) > 0, brand).alias("brand_id"),
    )
    columns = [
        F.col(f"_parsed.{name}").alias(PAYLOAD_BRAND_COLUMN if name == "brand_id" else name)
        for name in schema.fieldNames()
    ]
    columns += [
        F.col(name) for name in ("brand_id", "_raw_value", "_source_ref", "_ingest_ts", "_batch_id")
    ]
    columns.append(F.col("_parse_error"))
    return base.select(*columns)


def summarise(rows: DataFrame, files: int) -> FileBatchCounts:
    """Per-brand and total counters of the Bronze ``rows`` of one micro-batch."""
    from pyspark.sql import functions as F

    file_ref = F.substring_index(F.col("_source_ref"), "#L", 1)
    grouped = (
        rows.groupBy(F.col("brand_id"))
        .agg(
            F.countDistinct(file_ref).alias("files"),
            F.count(F.lit(1)).alias("lines"),
            F.count(F.col("_parse_error")).alias("errors"),
        )
        .collect()
    )
    per_brand = {
        str(r["brand_id"] if r["brand_id"] is not None else UNKNOWN_BRAND): BrandCounts(
            files=int(r["files"]), lines=int(r["lines"]), parse_errors=int(r["errors"])
        )
        for r in grouped
    }
    return FileBatchCounts(
        files=files,
        lines=sum(c.lines for c in per_brand.values()),
        parse_errors=sum(c.parse_errors for c in per_brand.values()),
        per_brand=per_brand,
    )


def write_bronze_files(
    batch_df: DataFrame, path: str, *, target: BatchTarget, app_id: str, batch_id: int
) -> FileBatchCounts:
    """Append one micro-batch of whole-file rows to the Bronze Delta table at ``path``.

    ``app_id`` and ``batch_id`` form Delta's idempotent-write key. ``mergeSchema`` is on: a column
    that a newer contract version adds is appended to the table schema. A skipped write must be a true replay of the stored batch
    (same ``_source_ref`` set), else ``CheckpointRewoundError``. An empty batch writes nothing.
    """
    from pyspark import StorageLevel

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        files = batch_df.select(FILE_PATH_COLUMN).distinct().count()
        if files == 0:
            return FileBatchCounts(files=0, lines=0, parse_errors=0, per_brand={})
        rows = to_bronze_rows(batch_df, target, f"{app_id}:{batch_id}")
        append_idempotent(
            rows,
            path,
            app_id=app_id,
            batch_id=batch_id,
            partition_by=target.partition_by,
            merge_schema=True,
        )
        return summarise(rows, files)
    finally:
        batch_df.unpersist()
