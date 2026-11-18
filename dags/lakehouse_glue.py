r"""DAG ``lakehouse_glue``: the cloud variant of the pipeline - one AWS Glue job per step (W5.D30).

Airflow triggers, Glue runs (ADR-0010, ADR-0011): each task starts the *existing* Glue job
``<glue_prefix>-<job>`` with the same job name and arguments as the local pipeline and waits for
the run state (``SUCCEEDED`` = success; any other terminal state fails the task). The jobs, their
IAM role and their script are created by Terraform; this DAG never creates or updates a job (no
``script_location``, no IAM role, no ``create_job_kwargs`` are passed).

Manual only (``schedule=None``): a run costs money and needs an applied environment. Trigger it
with ``bucket`` (the data bucket name, required) and, if different, ``glue_prefix`` and ``region``.

Credentials are never in code: the ``aws_default`` connection comes from the owner's environment,
for example ``AIRFLOW_CONN_AWS_DEFAULT`` or short-lived ``AWS_ACCESS_KEY_ID`` /
``AWS_SECRET_ACCESS_KEY`` / ``AWS_SESSION_TOKEN`` (default chain). The local scheduler container
sets ``AWS_ENDPOINT_URL`` and the SeaweedFS keys, which would send every Glue call to the local
object store: ``validate_params`` fails when ``AWS_ENDPOINT_URL``, ``AWS_ENDPOINT_URL_GLUE`` or
``AWS_ENDPOINT_URL_STS`` is set. Run the DAG with those variables removed and the short-lived owner
credentials passed to that one command only (not stored in the container or in a file)::

    docker compose exec airflow-scheduler env -u AWS_ENDPOINT_URL -u AWS_ACCESS_KEY_ID \
      -u AWS_SECRET_ACCESS_KEY AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
      AWS_SESSION_TOKEN=... airflow dags test lakehouse_glue --conf '{"bucket": "..."}'

One run at a time: ``check_no_active_runs`` refuses to continue while any Glue job of the pipeline
is STARTING / RUNNING / STOPPING / WAITING (several jobs write ``ops.pipeline_metrics``, and the
CLI ``scripts/cloud/run_job.py`` and the deploy workflow start the same jobs). The check is not
atomic with the starts; it stops accidental overlap, not a deliberate race.

Flow::

    validate_params -> check_no_active_runs -> register_tables -> bronze_batch -> dlq_gate
      -> silver_customers -> silver_customers_history -> silver_orders -> silver_products
      -> silver_clickstream -> reconcile -> metrics_report

Not in the cloud chain: ``bronze_stream`` (Bronze is seeded from a local Delta copy, D-101), the
per-brand landing check and the dbt build (Gold runs through dbt-athena, D-104). The job list lives
in ``lakehouse.orchestration.cloud`` (unit-tested without Airflow).
"""

import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sdk import Param, dag, get_current_context, task

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
    ActiveRunsError,
    assert_no_active_runs,
    cloud_job_names,
    glue_script_args,
    validate_bucket,
    validate_cloud_environment,
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

#: Same key as ``lakehouse_pipeline`` (D-107): ``airflow dags test`` calls the failure callback
#: without ``context["exception"]``, so the task records its reason in its own context first.
FAILURE_REASON_KEY = "lakehouse_failure_reason"


def _fail(exc: Exception) -> Exception:
    """Record ``exc``'s text for :func:`alert_on_failure` and return it: ``raise _fail(...)``."""
    try:
        get_current_context()[FAILURE_REASON_KEY] = str(exc)  # type: ignore[index]
    except Exception:  # outside a task run: no context; the raise still happens
        log.debug("no task context to record the failure reason in")
    return exc


def alert_on_failure(context: dict[str, Any]) -> None:
    """``on_failure_callback``: same alert contract as ``lakehouse_pipeline`` (never raises)."""
    try:
        ti = context["task_instance"]
        if not is_alertable_task(ti.task_id):
            return
        reason = failure_text(
            context.get("exception") or context.get(FAILURE_REASON_KEY),
            context.get("reason"),
            getattr(ti, "state", None),
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
        """Fail fast on a malformed prefix, bucket or region or a local AWS endpoint override."""
        p = params or {}
        try:
            validate_cloud_environment(os.environ)
            validate_glue_prefix(p.get("glue_prefix"))
            validate_bucket(p.get("bucket"))
            validate_region(p.get("region"))
        except ValueError as exc:
            raise _fail(AirflowFailException(str(exc))) from None

    @task(retries=0)
    def check_no_active_runs(params: dict[str, Any] | None = None) -> None:
        """Fail when another Glue run of the pipeline is still active (one writer per table)."""
        p = params or {}
        try:
            glue = AwsBaseHook(
                aws_conn_id="aws_default", client_type="glue", region_name=p.get("region")
            ).get_conn()
            assert_no_active_runs(glue, str(p.get("glue_prefix")), cloud_job_names())
        except ActiveRunsError as exc:
            raise _fail(AirflowFailException(str(exc))) from None
        except Exception as exc:  # fail closed: an unverifiable check must not let the run go on
            msg = f"could not check for active Glue runs: {type(exc).__name__}: {exc}"
            raise _fail(AirflowFailException(msg)) from None

    validated = validate_params()
    previous: Any = check_no_active_runs()
    validated >> previous
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
