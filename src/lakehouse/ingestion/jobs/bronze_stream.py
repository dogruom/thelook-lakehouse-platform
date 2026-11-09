"""Kafka -> Bronze Delta: one ``availableNow`` streaming query per topic.

Run as ``python scripts/submit_job.py bronze_stream [--namespace <segment>]``. The job reads the
topic from the earliest offset (a new checkpoint) or from the checkpointed offset (a rerun), writes
every message as one Bronze row (see ``lakehouse.ingestion.bronze``) and stops when the topic is
drained. Airflow triggers runs; Spark owns the stream state in the checkpoint.

Why the Delta app id is the streaming query id
----------------------------------------------
Each micro-batch is written with Delta's idempotent-write key (``txnAppId``, ``txnVersion`` =
batch id). If a batch is replayed after a failure between the Delta commit and the offset commit,
Delta skips it, so Bronze does not get duplicates. The key is only safe when the app id is stable
within one checkpoint lineage and different between lineages. The query id is exactly that: Spark
stores it in the checkpoint. A fixed app id would be wrong: after a checkpoint loss the batch ids
restart at 0, Delta would find the old (app id, version) pairs and silently skip new data.

Delivery wording: offsets are committed once per checkpoint lineage; the sink is idempotent per
batch. This is not an "exactly-once" claim.

Metrics count rows *seen* in micro-batches, not rows written (a skipped replay writes nothing).
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.ingestion.bronze import CLICKSTREAM, CUSTOMERS, BronzeTarget, write_bronze_batch


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

    from lakehouse.common.jobs import JobContext
    from lakehouse.common.spark import SparkSettings

_log = get_logger(__name__)
_POLL_SECONDS = 1


def _positive_int(value: str) -> int:
    import argparse as _argparse

    try:
        number = int(value)
    except ValueError:
        raise _argparse.ArgumentTypeError(f"{value!r} is not an integer") from None
    if number < 1:
        raise _argparse.ArgumentTypeError(f"must be >= 1, got {number}")
    return number


def query_name(target: BronzeTarget) -> str:
    """Name of the streaming query of ``target`` (also how the callback finds its query id)."""
    return f"bronze_{target.table}"


def _find_query_id(spark: SparkSession, name: str) -> str:
    """Id of the one active query called ``name``; raise on none or several (never default)."""
    matches = [q for q in spark.streams.active if q.name == name]
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one active query named {name!r}, found {len(matches)}"
        )
    return str(matches[0].id)


class _Counters:
    """Per-query counters, accumulated on the driver from the foreachBatch callback."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.rows_seen = 0
        self.parse_errors = 0
        self.batches = 0

    def add(self, rows_seen: int, parse_errors: int) -> None:
        with self._lock:
            self.rows_seen += rows_seen
            self.parse_errors += parse_errors
            self.batches += 1


class BronzeStreamJob(SparkJob):
    """Drain ``customer_cdc`` and ``clickstream_events`` into the Bronze Delta tables."""

    name = "bronze_stream"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--namespace", default=None, help="optional single path segment isolating this run"
        )
        parser.add_argument("--cdc-topic", default=CUSTOMERS.topic, help="customer CDC topic")
        parser.add_argument(
            "--clickstream-topic", default=CLICKSTREAM.topic, help="clickstream topic"
        )
        parser.add_argument(
            "--max-offsets-per-trigger",
            type=_positive_int,
            default=50000,
            help="upper bound of messages per micro-batch (>= 1)",
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        namespace: str | None = args.namespace
        ns_parts = [namespace] if namespace else []
        targets = [
            BronzeTarget(args.cdc_topic, CUSTOMERS.table, CUSTOMERS.contract),
            BronzeTarget(
                args.clickstream_topic,
                CLICKSTREAM.table,
                CLICKSTREAM.contract,
                CLICKSTREAM.partition_by,
            ),
        ]
        counters: dict[str, _Counters] = {t.table: _Counters() for t in targets}
        info: dict[str, dict[str, str]] = {}
        queries: dict[str, StreamingQuery] = {}
        try:
            for target in targets:
                path = ctx.settings.layer_uri("bronze", *ns_parts, target.table)
                checkpoint = ctx.settings.layer_uri(
                    "checkpoints", "bronze_stream", *ns_parts, target.table
                )
                info[target.table] = {"path": path, "checkpoint": checkpoint}
                queries[target.table] = _start_query(
                    spark,
                    ctx.settings,
                    target,
                    path,
                    checkpoint,
                    args.max_offsets_per_trigger,
                    counters[target.table],
                )
            _await_all(queries)
        finally:
            _stop_all(queries)

        result: dict[str, Any] = {}
        for table, query in queries.items():
            c = counters[table]
            result[table] = {
                "rows_seen": c.rows_seen,
                "parse_errors": c.parse_errors,
                "batches": c.batches,
                "query_id": str(query.id),
                **info[table],
            }
            _log.info(
                "bronze_stream.query_done",
                table=table,
                rows_seen=c.rows_seen,
                parse_errors=c.parse_errors,
                batches=c.batches,
            )
        return {"queries": result}


def _start_query(
    spark: SparkSession,
    settings: SparkSettings,
    target: BronzeTarget,
    path: str,
    checkpoint: str,
    max_offsets: int,
    counters: _Counters,
) -> StreamingQuery:
    name = query_name(target)

    def on_batch(batch_df: DataFrame, batch_id: int) -> None:
        # Looked up through the outer session by name: the id is the one stored in the checkpoint.
        query_id = _find_query_id(spark, name)
        counts = write_bronze_batch(
            batch_df, path, target=target, app_id=query_id, batch_id=batch_id
        )
        counters.add(counts.rows_seen, counts.parse_errors)
        _log.info(
            "bronze_stream.batch",
            table=target.table,
            batch_id=batch_id,
            rows_seen=counts.rows_seen,
            parse_errors=counts.parse_errors,
        )

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", target.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("includeHeaders", "true")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )
    query = (
        source.writeStream.queryName(name)
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint)
        .foreachBatch(on_batch)
        .start()
    )
    _log.info("bronze_stream.query_started", table=target.table, query_id=str(query.id))
    return query


def _await_all(queries: Mapping[str, StreamingQuery]) -> None:
    """Wait until every query ended; on the first failure stop the others and raise."""
    pending = dict(queries)
    try:
        while pending:
            for table, query in list(pending.items()):
                if query.awaitTermination(_POLL_SECONDS) or not query.isActive:
                    del pending[table]
        for table, query in queries.items():
            error = query.exception()
            if error is not None:
                raise RuntimeError(f"query for {table} failed: {error}")
    except Exception:
        _stop_all(queries)
        raise


def _stop_all(queries: Mapping[str, StreamingQuery]) -> None:
    for table, query in queries.items():
        try:
            if query.isActive:
                query.stop()
        except Exception:  # must not hide the original failure
            _log.exception("bronze_stream.stop_failed", table=table)


JOB = BronzeStreamJob
