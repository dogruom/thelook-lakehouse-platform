"""Audited reset of the shared clickstream data (D-097, owner-approved rebuild for D-096).

Run as ``python scripts/submit_job.py reset_clickstream`` (dry run: reports what would be removed)
or ``... reset_clickstream --apply --confirm clickstream``. The job removes exactly the clickstream
lineage and nothing else:

* catalog entries ``bronze.raw_clickstream``, ``dead_letter.dlq_clickstream``, ``silver.clickstream``
  (external tables: dropping the entry does not touch the data);
* the Delta locations of those three tables;
* the checkpoints ``checkpoints/bronze_stream/raw_clickstream`` and
  ``checkpoints/silver_clickstream/clickstream``;
* the ``ops.pipeline_metrics`` rows whose ``source_table`` is ``raw_clickstream`` (otherwise the next
  ``reconcile`` would compare new data with old batch metrics).

Bronze is append-only; this job is the documented exception for synthetic data rebuilt with owner
approval (same class as audited erasure). It is never part of a pipeline run. The Kafka topic is
recreated separately (``kafka-topics.sh``), otherwise the next ``bronze_stream`` run would re-read the
old messages from the earliest offset. Namespaced (test) runs are refused: tests own their paths.
Logs carry paths and counts only.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lakehouse.common.jobs import SparkJob
from lakehouse.common.logging import get_logger
from lakehouse.common.tables import CATALOG_TABLES, PIPELINE_METRICS, resolve


if TYPE_CHECKING:
    import argparse
    from collections.abc import Mapping

    from pyspark.sql import SparkSession

    from lakehouse.common.jobs import JobContext

_log = get_logger(__name__)

CONFIRM_TOKEN = "clickstream"
TABLES = ("raw_clickstream", "dlq_clickstream", "clickstream")
CHECKPOINTS = (("bronze_stream", "raw_clickstream"), ("silver_clickstream", "clickstream"))
METRICS_SOURCE_TABLE = "raw_clickstream"


def _hadoop_path(spark: SparkSession, uri: str) -> tuple[Any, Any]:
    jvm = spark.sparkContext._jvm  # type: ignore[attr-defined]
    conf = spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    path = jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(conf), path


def _file_count(spark: SparkSession, uri: str) -> int:
    """Number of files below ``uri`` (0 when it does not exist)."""
    fs, path = _hadoop_path(spark, uri)
    if not fs.exists(path):
        return 0
    count = 0
    it = fs.listFiles(path, True)
    while it.hasNext():
        it.next()
        count += 1
    return count


def _delete(spark: SparkSession, uri: str) -> bool:
    fs, path = _hadoop_path(spark, uri)
    return bool(fs.exists(path) and fs.delete(path, True))


class ResetClickstreamJob(SparkJob):
    """Remove the clickstream Bronze/DLQ/Silver lineage so it can be rebuilt from Kafka."""

    name = "reset_clickstream"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--apply", action="store_true", help="delete (default: dry run)")
        parser.add_argument(
            "--confirm", default=None, help=f"must be '{CONFIRM_TOKEN}' together with --apply"
        )

    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        if getattr(args, "namespace", None):
            raise RuntimeError("reset_clickstream acts on the shared catalog only (no namespace)")
        if args.apply and args.confirm != CONFIRM_TOKEN:
            raise RuntimeError(f"--apply needs --confirm {CONFIRM_TOKEN}")
        targets: dict[str, dict[str, Any]] = {}
        for key in TABLES:
            resolved = resolve(ctx.settings, CATALOG_TABLES[key])
            targets[key] = {"name": resolved.name, "location": resolved.location}
        for job, table in CHECKPOINTS:
            uri = ctx.settings.layer_uri("checkpoints", job, table)
            targets[f"checkpoint:{job}"] = {"name": None, "location": uri}
        for item in targets.values():
            item["files"] = _file_count(spark, item["location"])

        metrics = resolve(ctx.settings, PIPELINE_METRICS).location
        metrics_rows = 0
        if _file_count(spark, metrics):
            metrics_rows = (
                spark.read.format("delta")
                .load(metrics)
                .where(f"source_table = '{METRICS_SOURCE_TABLE}'")
                .count()
            )

        if args.apply:
            for item in targets.values():
                if item["name"] is not None:
                    spark.sql(f"DROP TABLE IF EXISTS {item['name']}")
                item["deleted"] = _delete(spark, item["location"])
            if metrics_rows:
                from delta.tables import DeltaTable

                DeltaTable.forPath(spark, metrics).delete(
                    f"source_table = '{METRICS_SOURCE_TABLE}'"
                )
        for key, item in targets.items():
            _log.info("reset_clickstream.target", target=key, apply=args.apply, **item)
        _log.info("reset_clickstream.metrics", apply=args.apply, rows=metrics_rows)
        return {"apply": bool(args.apply), "targets": targets, "metrics_rows": metrics_rows}


JOB = ResetClickstreamJob
