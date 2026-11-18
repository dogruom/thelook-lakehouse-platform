"""Cloud variant of the run plan: the Glue jobs behind ``dags/lakehouse_glue.py`` (W5.D30, ADR-0010).

Pure data and functions, no Airflow and no boto3 (functions that talk to Glue take the client). The Glue job names, the job order and the
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
from typing import TYPE_CHECKING, Any, Final

from lakehouse.common.aws import AWS_BUCKET_RE
from lakehouse.orchestration.plan import DLQ_GATE_JOB


if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


DAG_ID: Final = "lakehouse_glue"
DEFAULT_GLUE_PREFIX: Final = "crm-lakehouse-dev"
DEFAULT_REGION: Final = "eu-central-1"
#: Glue job argument that carries the job's own CLI arguments (JSON list of strings).
LAKEHOUSE_ARGS_KEY: Final = "--lakehouse_args"
#: Replaced by the data bucket name in ``glue_script_args``.
BUCKET_PLACEHOLDER: Final = "{bucket}"
REPORT_PREFIX: Final = "ops/reports"
#: Glue run states that still hold (or are about to hold) a writer on the Delta tables.
ACTIVE_RUN_STATES: Final = frozenset({"STARTING", "RUNNING", "STOPPING", "WAITING"})
#: A boto3 client built from these would talk to the local object store, not to AWS.
LOCAL_ENDPOINT_ENV_VARS: Final = (
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT_URL_GLUE",
    "AWS_ENDPOINT_URL_STS",
    "AWS_ENDPOINT_URL_S3",
    "AWS_ENDPOINT_URL_LOGS",
)
_GET_JOB_RUNS_PAGE: Final = 200

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
    """A ``crm-lakehouse-*`` data-bucket name (a valid S3 name; no URI, no path); else ``ValueError``."""
    if (
        not isinstance(value, str)
        or _BUCKET_RE.match(value) is None
        or "--" in value
        or re.fullmatch(r"[0-9]+(-[0-9]+){3}", value) is not None
        or AWS_BUCKET_RE.fullmatch(value) is None
    ):
        raise ValueError(
            "bucket must be a bare crm-lakehouse-* S3 bucket name (3-63 chars of [a-z0-9-])"
        )
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


class ActiveRunsError(RuntimeError):
    """Another Glue run of this pipeline is still active; ``runs`` = (job, run id, state)."""

    def __init__(self, runs: list[tuple[str, str, str]]) -> None:
        self.runs = runs
        listed = ", ".join(f"{job} run {run_id} ({state})" for job, run_id, state in runs)
        super().__init__(
            f"refusing to start: {len(runs)} Glue run(s) still active: {listed}. The cloud jobs "
            "share Delta tables (ops.pipeline_metrics has several writers), so only one run may "
            "be active at a time; wait for it to finish or stop it first"
        )


def validate_cloud_environment(env: Mapping[str, str]) -> None:
    """Fail when a local-endpoint override would send AWS calls to the local object store.

    The Airflow scheduler container sets ``AWS_ENDPOINT_URL`` (and local access keys) for
    SeaweedFS; boto3 honours it for every service, so a Glue call would never reach AWS.
    ``ValueError`` names the variables, never their values.
    """
    # Any service-specific override counts (boto3 reads ``AWS_ENDPOINT_URL_<SERVICE>``).
    set_vars = sorted(
        name
        for name, value in env.items()
        if name.startswith("AWS_ENDPOINT_URL") and (value or "").strip()
    )
    if set_vars:
        raise ValueError(
            f"{', '.join(set_vars)} is set (local object-store endpoint): AWS calls would not "
            "reach AWS. Run the cloud command with these variables removed and with short-lived "
            "owner credentials passed to that one command only (from an env file, never typed "
            "on the command line), e.g. `env -u AWS_ENDPOINT_URL -u AWS_ACCESS_KEY_ID "
            "-u AWS_SECRET_ACCESS_KEY <command>` inside `docker exec --env-file <file>`"
        )


def active_job_runs(
    glue_client: Any, prefix: str, jobs: Iterable[str]
) -> list[tuple[str, str, str]]:
    """``(job, run id, state)`` of every active run of ``<prefix>-<job>``; all pages are read.

    ``glue_client`` is a boto3 Glue client (or a stub with ``get_job_runs``). AWS errors
    propagate: a check that cannot be made must not read as "nothing is running".
    """
    active: list[tuple[str, str, str]] = []
    for job in dict.fromkeys(jobs):
        glue_job = glue_job_name(prefix, job)
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"JobName": glue_job, "MaxResults": _GET_JOB_RUNS_PAGE}
            if token:
                kwargs["NextToken"] = token
            page = glue_client.get_job_runs(**kwargs)
            for run in page.get("JobRuns", []):
                state = str(run.get("JobRunState", ""))
                if state in ACTIVE_RUN_STATES:
                    active.append((job, str(run.get("Id", "?")), state))
            token = page.get("NextToken")
            if not token:
                break
    return active


def assert_no_active_runs(glue_client: Any, prefix: str, jobs: Iterable[str]) -> None:
    """Raise :class:`ActiveRunsError` when any run of the given jobs is still active."""
    active = active_job_runs(glue_client, prefix, jobs)
    if active:
        raise ActiveRunsError(active)
