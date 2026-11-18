"""DAG ``lakehouse_glue``: the cloud variant of the pipeline - one AWS Glue job per step (W5.D30).

Airflow triggers, Glue runs (ADR-0010, ADR-0011): each task starts the *existing* Glue job
``<glue_prefix>-<job>`` with the same job name and arguments as the local pipeline and waits for
the run state (``SUCCEEDED`` = success; any other terminal state fails the task). The jobs, their
IAM role and their script are created by Terraform; this DAG never creates or updates a job (no
``script_location``, no IAM role, no ``create_job_kwargs`` are passed).

Manual only (``schedule=None``): a run costs money and needs an applied environment. Trigger it
with ``bucket`` (the data bucket name, required) and, if different, ``glue_prefix`` and ``region``.

Credentials are never in code: the ``aws_default`` connection comes from the owner's environment,
for example ``AIRFLOW_CONN_AWS_DEFAULT`` or short-lived ``AWS_ACCESS_KEY_ID`` /
``AWS_SECRET_ACCESS_KEY`` / ``AWS_SESSION_TOKEN`` in the scheduler's environment (default chain).

Flow::

    validate_params -> register_tables -> bronze_batch -> dlq_gate -> silver_customers
      -> silver_customers_history -> silver_orders -> silver_products -> silver_clickstream
      -> reconcile -> metrics_report

Not in the cloud chain: ``bronze_stream`` (Bronze is seeded from a local Delta copy, D-101), the
per-brand landing check and the dbt build (Gold runs through dbt-athena, D-104). The job list lives
in ``lakehouse.orchestration.cloud`` (unit-tested without Airflow).
"""

import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sdk import Param, dag, task

from lakehouse.orchestration.alert import (
    build_alert_payload,
    classify_kind,
    emit_alert,
    extract_brand_id,
    failure_text,
    is_alertable_task,
)
from lakehouse.orchestration.cloud import (
    CLOUD_JOBS,
    DAG_ID,
    DEFAULT_GLUE_PREFIX,
    DEFAULT_REGION,
    glue_script_args,
    validate_bucket,
    validate_glue_prefix,
    validate_region,
)


try:  # Airflow 3 Task SDK first; the classic module is the fallback for older 3.x layouts
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # pragma: no cover - depends on the installed Airflow
    from airflow.exceptions import AirflowFailException

log = logging.getLogger(__name__)

#: One Glue job is capped at 30 minutes (Terraform); the task timeout leaves room for start-up.
GLUE_TASK_TIMEOUT = timedelta(minutes=45)
GLUE_POLL_INTERVAL_S = 15


def alert_on_failure(context: dict[str, Any]) -> None:
    """``on_failure_callback``: same alert contract as ``lakehouse_pipeline`` (never raises)."""
    try:
        ti = context["task_instance"]
        if not is_alertable_task(ti.task_id):
            return
        reason = failure_text(
            context.get("exception"), context.get("reason"), getattr(ti, "state", None)
        )
        payload = build_alert_payload(
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            task_id=ti.task_id,
            kind=classify_kind(reason),
            reason=reason,
            try_number=int(getattr(ti, "try_number", 1) or 1),
            logical_date=context.get("logical_date"),
            brand_id=extract_brand_id(reason),
        )
        emit_alert(payload, os.environ.get("ALERT_WEBHOOK_URL") or None)
    except Exception:  # alert path only
        log.exception("alert callback failed")


@dag(
    dag_id=DAG_ID,
    description="Cloud variant: start the AWS Glue jobs of the pipeline in order (manual only)",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=6),
    default_args={"owner": "data-platform", "on_failure_callback": alert_on_failure},
    params={
        "glue_prefix": Param(DEFAULT_GLUE_PREFIX, type="string"),
        # required on purpose: no default, so no run can start against a guessed bucket
        "bucket": Param(type="string", minLength=3, maxLength=63),
        "region": Param(DEFAULT_REGION, type="string"),
    },
    tags=["lakehouse", "cloud", "aws"],
    doc_md=__doc__,
)
def lakehouse_glue() -> None:
    @task(retries=0)
    def validate_params(params: dict[str, Any] | None = None) -> None:
        """Fail fast on a malformed prefix, bucket or region (before any Glue run starts)."""
        p = params or {}
        try:
            validate_glue_prefix(p.get("glue_prefix"))
            validate_bucket(p.get("bucket"))
            validate_region(p.get("region"))
        except ValueError as exc:
            raise AirflowFailException(str(exc)) from None

    previous: Any = validate_params()
    for job, args in CLOUD_JOBS:
        current = GlueJobOperator(
            task_id=job,
            # only the job name: the operator starts the job Terraform created
            job_name=f"{{{{ params.glue_prefix }}}}-{job}",  # = glue_job_name(prefix, job)
            script_args=glue_script_args(args, "{{ params.bucket }}"),
            aws_conn_id="aws_default",
            region_name="{{ params.region }}",
            wait_for_completion=True,
            verbose=False,
            stop_job_run_on_kill=True,
            job_poll_interval=GLUE_POLL_INTERVAL_S,
            retries=0,  # a Glue run is not retried blindly; rerun the DAG after reading the log
            execution_timeout=GLUE_TASK_TIMEOUT,
        )
        previous >> current
        previous = current


lakehouse_glue()
