"""Cloud variant of the run plan: the Glue jobs behind ``dags/lakehouse_glue.py`` (W5.D30, ADR-0010).

Pure data and functions, no Airflow and no boto3. The Glue job names, the job order and the
``--lakehouse_args`` argument convention are the shared W5 interface: one Glue job per pipeline
job, named ``<prefix>-<job>`` (underscores kept), started with ``--lakehouse_args`` = a JSON list
of strings that ``scripts/cloud/glue_entry.py`` hands to ``lakehouse.common.jobs.main``.

The order mirrors the cloud runner. It has no ``bronze_stream`` (Kafka-sourced Bronze is seeded
from a local Delta copy, D-101), no per-brand landing check and no dbt build (Gold runs through
dbt-athena, D-104).
"""

from __future__ import annotations

import json
import re
from typing import Final

from lakehouse.orchestration.plan import DLQ_GATE_JOB


DAG_ID: Final = "lakehouse_glue"
DEFAULT_GLUE_PREFIX: Final = "crm-lakehouse-dev"
DEFAULT_REGION: Final = "eu-central-1"
#: Glue job argument that carries the job's own CLI arguments (JSON list of strings).
LAKEHOUSE_ARGS_KEY: Final = "--lakehouse_args"
#: Replaced by the data bucket name in ``glue_script_args``.
BUCKET_PLACEHOLDER: Final = "{bucket}"
REPORT_PREFIX: Final = "ops/reports"

#: ``(job, args)`` in execution order; every entry is one existing Glue job.
CLOUD_JOBS: Final[tuple[tuple[str, tuple[str, ...]], ...]] = (
    ("register_tables", ()),
    ("bronze_batch", ()),
    ("dlq_gate", DLQ_GATE_JOB.args),
    ("silver_customers", ()),
    ("silver_customers_history", ()),
    ("silver_orders", ()),
    ("silver_products", ()),
    ("silver_clickstream", ()),
    ("reconcile", ()),
    ("metrics_report", ("--output-uri", f"s3://{BUCKET_PLACEHOLDER}/{REPORT_PREFIX}")),
)

_PREFIX_RE = re.compile(r"[a-z][a-z0-9-]{0,48}[a-z0-9]\Z")
_JOB_RE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_BUCKET_RE = re.compile(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]\Z")
_REGION_RE = re.compile(r"[a-z]{2}(-[a-z]+)+-[0-9]\Z")


def validate_glue_prefix(value: object) -> str:
    """The Glue job-name prefix (``crm-lakehouse-dev``); ``ValueError`` otherwise."""
    if not isinstance(value, str) or _PREFIX_RE.match(value) is None:
        raise ValueError("glue_prefix must be 2-50 chars of [a-z0-9-], starting with a letter")
    return value


def validate_bucket(value: object) -> str:
    """An S3 data-bucket name (3-63 chars, lowercase letters, digits, hyphens); no URI, no path."""
    if (
        not isinstance(value, str)
        or _BUCKET_RE.match(value) is None
        or "--" in value
        or re.fullmatch(r"[0-9]+(-[0-9]+){3}", value) is not None
    ):
        raise ValueError("bucket must be a bare S3 bucket name (3-63 chars of [a-z0-9-])")
    return value


def validate_region(value: object) -> str:
    """An AWS region id such as ``eu-central-1``; ``ValueError`` otherwise."""
    if not isinstance(value, str) or _REGION_RE.match(value) is None:
        raise ValueError("region must look like eu-central-1")
    return value


def glue_job_name(prefix: str, job: str) -> str:
    """Glue job name ``<prefix>-<job>``, e.g. ``crm-lakehouse-dev-silver_orders``."""
    validate_glue_prefix(prefix)
    if _JOB_RE.match(job) is None:
        raise ValueError("job must be [a-z][a-z0-9_]*")
    return f"{prefix}-{job}"


def glue_script_args(args: tuple[str, ...], bucket: str | None = None) -> dict[str, str]:
    """Glue ``Arguments`` for one run: ``{"--lakehouse_args": <JSON list>}``; ``{}`` when no args.

    ``bucket`` replaces ``{bucket}`` in the arguments. It is not validated here, so the DAG can
    pass a Jinja expression; ``validate_bucket`` runs on the resolved value in the DAG.
    """
    if not args:
        return {}
    resolved = [a.replace(BUCKET_PLACEHOLDER, bucket) if bucket else a for a in args]
    return {LAKEHOUSE_ARGS_KEY: json.dumps(resolved, separators=(",", ":"))}


def cloud_job_names() -> tuple[str, ...]:
    """Glue job short names in execution order."""
    return tuple(job for job, _ in CLOUD_JOBS)
