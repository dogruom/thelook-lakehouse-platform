"""DAG ``lakehouse_pipeline``: one parametrised, re-runnable pass over the lakehouse (W4.D22/D23).

Airflow triggers, Spark owns state (ADR-0010): every Spark job is submitted in client mode to the
standalone cluster and talks back only through its exit code and JSON log. A job is idempotent
(``availableNow`` checkpoints, deterministic ``MERGE``), so a second run of the same interval
changes nothing. This is a batch orchestration of ``availableNow`` runs - it does not orchestrate a
running stream.

Flow::

    plan_run -> landing_check[brand] (mapped) -> summarize_landing
      -> bronze_stream -> bronze_batch -> dlq_gate
      -> silver_customers -> silver_customers_history -> silver_orders -> silver_products
      -> silver_clickstream -> reconcile
      -> dlq_rate_gate[brand] (mapped) -> dbt_build        run_verdict (one_failed) closes the run

Params: ``brands`` (default the three tenants), ``fail_brand`` (inject a landing-check failure for
one brand; ``none`` = off) and ``max_dlq_rate`` (per-brand, per-source ceiling, default 0.15).

Trigger rules (documented in ``lakehouse.orchestration.plan.TRIGGER_RULES`` and ADR-0010): mapped
brand tasks are isolated from each other; ``summarize_landing`` is ``all_done`` so it can collect
the healthy brands after a failed one; the shared chain and ``dbt_build`` keep ``all_success`` (a
failed shared job or a DLQ-rate breach halts what reads its output); ``run_verdict`` is
``one_failed`` so a degraded run ends red instead of green. Failures and breaches log one
``ALERT_PAYLOAD`` JSON line and post it to ``ALERT_WEBHOOK_URL`` when that is set.

All non-Airflow logic lives in ``lakehouse.orchestration`` (unit-tested without Airflow); this file
only wires it to the Airflow 3 Task SDK.
"""

import logging
import os
import socket
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.sdk import Param, dag, task

from lakehouse.orchestration.alert import (
    THRESHOLD_MARKER,
    build_alert_payload,
    classify_kind,
    emit_alert,
    extract_brand_id,
)
from lakehouse.orchestration.joblog import (
    breached_brands_from_log,
    failure_reason,
    job_metrics,
    parse_records,
)
from lakehouse.orchestration.landing import check_brand_landing, split_landing_results
from lakehouse.orchestration.plan import (
    BRONZE_JOBS,
    DAG_ID,
    DBT_SELECT,
    DBT_TIMEOUT_S,
    DEFAULT_BRANDS,
    DEFAULT_MAX_DLQ_RATE,
    DLQ_GATE_JOB,
    DLQ_RATE_GATE_JOB,
    DLQ_RATE_GATE_TIMEOUT_S,
    LANDING_RETRIES,
    LANDING_TIMEOUT_S,
    NO_FAILURE,
    RECONCILE_JOB,
    RETRY_DELAY_S,
    SILVER_JOBS,
    TASK_TIMEOUT_MARGIN_S,
    TRIGGER_ALL_DONE,
    TRIGGER_ONE_FAILED,
    JobSpec,
    build_run_plan,
    should_inject_failure,
    task_timeout_s,
    validate_max_dlq_rate,
)
from lakehouse.orchestration.runner import EXIT_TIMEOUT, run_streaming, tcp_reachable
from lakehouse.orchestration.submit import (
    build_submit_command,
    dbt_build_command,
    dlq_rate_gate_args,
    stage_app,
)


try:  # Airflow 3 Task SDK first; the classic module is the fallback for older 3.x layouts
    from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
except ImportError:  # pragma: no cover - depends on the installed Airflow
    from airflow.exceptions import AirflowFailException, AirflowSkipException

log = logging.getLogger(__name__)

SRC_DIR = Path(os.environ.get("LAKEHOUSE_SRC_DIR", "/opt/airflow/src"))
DIST_DIR = Path(os.environ.get("LAKEHOUSE_DIST_DIR", "/tmp/lakehouse-dist"))
DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt-venv/bin/dbt")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")


# ---- alerting ---------------------------------------------------------------------------------


def alert_on_failure(context: dict[str, Any]) -> None:
    """``on_failure_callback``: build the alert payload, log it as one line, POST it if configured.

    Never raises: a problem in the alert path must not mask the original task failure.
    """
    try:
        ti = context["task_instance"]
        reason = str(context.get("exception") or context.get("reason") or "task failed")
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


# ---- Spark and dbt helpers --------------------------------------------------------------------


def _submit(job: str, job_args: list[str], timeout_s: int) -> tuple[int, list[dict[str, Any]]]:
    """Package the code, run ``spark-submit`` in client mode; returns (exit code, JSON records)."""
    app_zip, entry = stage_app(SRC_DIR, DIST_DIR)
    cmd = build_submit_command(
        job,
        job_args,
        app_zip=str(app_zip),
        entry=str(entry),
        driver_host=os.environ.get("SPARK_DRIVER_HOST") or socket.gethostname(),
        marker=uuid.uuid4().hex,
    )
    result = run_streaming(cmd, timeout_s, on_line=log.info)
    if result.code == EXIT_TIMEOUT:
        log.error("spark job %s timed out after %s s; driver process group stopped", job, timeout_s)
    return result.code, parse_records(result.tail)


# ---- DAG --------------------------------------------------------------------------------------


@dag(
    dag_id=DAG_ID,
    description="Landing check, Bronze, DLQ gate, Silver, reconcile, DLQ-rate gate, dbt build",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=3),
    default_args={"owner": "data-platform", "on_failure_callback": alert_on_failure},
    params={
        "brands": Param(list(DEFAULT_BRANDS), type="array", items={"type": "string"}),
        "fail_brand": Param(NO_FAILURE, type="string"),
        "max_dlq_rate": Param(DEFAULT_MAX_DLQ_RATE, type="number", minimum=0, maximum=1),
    },
    tags=["lakehouse", "orchestration"],
    doc_md=__doc__,
)
def lakehouse_pipeline() -> None:  # noqa: PLR0915 (task definitions are nested by TaskFlow design)
    @task(retries=0, multiple_outputs=False)
    def plan_run(params: dict[str, Any] | None = None) -> list[str]:
        """Validate the run params and return the brands to process."""
        p = params or {}
        plan = build_run_plan(
            p.get("brands", DEFAULT_BRANDS),
            p.get("fail_brand", NO_FAILURE),
            p.get("max_dlq_rate", DEFAULT_MAX_DLQ_RATE),
        )
        return list(plan.brands)

    @task(
        retries=LANDING_RETRIES,
        retry_delay=timedelta(seconds=20),
        execution_timeout=timedelta(seconds=LANDING_TIMEOUT_S),
        multiple_outputs=False,
    )
    def landing_check(brand: str, params: dict[str, Any] | None = None) -> str:
        """One brand: every landing entity has a complete batch. Returns the brand on success."""
        if should_inject_failure(brand, str((params or {}).get("fail_brand", NO_FAILURE))):
            # AirflowFailException: no retries for a deliberate failure
            raise AirflowFailException(f"brand_id={brand} injected failure (param fail_brand)")
        import boto3
        from botocore.config import Config

        try:
            client = boto3.client(
                "s3",
                region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                config=Config(s3={"addressing_style": "path"}),
            )
            status = check_brand_landing(
                client, os.environ.get("LAKEHOUSE_BUCKET", "lakehouse"), brand
            )
        except Exception as exc:  # type name only: the message may carry endpoint details
            raise RuntimeError(
                f"brand_id={brand} landing check error: {type(exc).__name__}"
            ) from None
        if not status.ok:
            raise AirflowFailException(
                f"brand_id={brand} landing incomplete, missing: {', '.join(status.missing_entities)}"
            )
        return brand

    @task(trigger_rule=TRIGGER_ALL_DONE, retries=0, multiple_outputs=False)
    def summarize_landing(planned: list[str], checked: Any) -> list[str]:
        """Collect the brands whose landing check succeeded (runs after failed checks too)."""
        healthy, missing = split_landing_results(planned, list(checked))
        if missing:
            log.warning("landing check failed or missing for: %s", ", ".join(missing))
        if not healthy:
            raise AirflowSkipException("no brand has a complete landing zone; run skipped")
        return healthy

    @task(multiple_outputs=False)
    def spark_job(job: str, job_args: list[str], timeout_s: int) -> dict[str, Any]:
        """Submit one Spark job; fail on a non-zero exit or a missing ``job.succeeded`` line."""
        code, records = _submit(job, job_args, timeout_s)
        if code != 0:
            raise AirflowFailException(failure_reason(job, code, records))
        metrics = job_metrics(records)
        if metrics is None:
            raise AirflowFailException(f"spark job {job} ended without a job.succeeded record")
        return {"job": job, "metrics": metrics}

    @task(
        retries=0,
        max_active_tis_per_dagrun=1,  # one driver at a time: the worker has two cores
        execution_timeout=timedelta(seconds=task_timeout_s(DLQ_RATE_GATE_TIMEOUT_S)),
        multiple_outputs=False,
    )
    def dlq_rate_gate(brand: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """One brand: halt (fail) when a source's DLQ rate is above ``max_dlq_rate``."""
        max_rate = validate_max_dlq_rate((params or {}).get("max_dlq_rate", DEFAULT_MAX_DLQ_RATE))
        code, records = _submit(
            DLQ_RATE_GATE_JOB, dlq_rate_gate_args(brand, max_rate), DLQ_RATE_GATE_TIMEOUT_S
        )
        if code == 0:
            return {"brand_id": brand, "max_dlq_rate": max_rate, "metrics": job_metrics(records)}
        if brand in breached_brands_from_log(records):
            raise AirflowFailException(
                f"{THRESHOLD_MARKER} brand_id={brand}: DLQ rate above {max_rate}; downstream halted"
            )
        raise AirflowFailException(
            f"brand_id={brand} {failure_reason(DLQ_RATE_GATE_JOB, code, records)}"
        )

    @task(
        retries=0,
        execution_timeout=timedelta(seconds=DBT_TIMEOUT_S + TASK_TIMEOUT_MARGIN_S),
        multiple_outputs=False,
    )
    def dbt_build() -> dict[str, Any]:
        """``dbt build --select staging marts`` through the Thrift Server (``sql`` profile)."""
        host = os.environ.get("DBT_SPARK_HOST", "spark-thrift")
        port = int(os.environ.get("DBT_SPARK_PORT", "10000"))
        if not tcp_reachable(host, port):
            raise AirflowFailException(
                f"Spark Thrift Server {host}:{port} is unreachable; start the sql profile "
                "(docker compose --profile sql up -d)"
            )
        cmd = dbt_build_command(
            DBT_BIN, DBT_SELECT, target_path="/tmp/dbt_target", log_path="/tmp/dbt_logs"
        )
        result = run_streaming(
            cmd, DBT_TIMEOUT_S, on_line=log.info, cwd=DBT_PROJECT_DIR, env=dict(os.environ)
        )
        if result.code != 0:
            raise AirflowFailException(f"dbt build exited with code {result.code}")
        return {"exit_code": 0, "select": list(DBT_SELECT)}

    @task(trigger_rule=TRIGGER_ONE_FAILED, retries=0)
    def run_verdict() -> None:
        """Runs only when an upstream task failed; makes the DAG run red (never a green degraded run)."""
        raise AirflowFailException(
            "run degraded: at least one task failed; see the failed task instances "
            "and their ALERT_PAYLOAD log lines"
        )

    def spark(spec: JobSpec) -> Any:
        return spark_job.override(
            task_id=spec.name,
            retries=spec.retries,
            retry_delay=timedelta(seconds=RETRY_DELAY_S),
            execution_timeout=timedelta(seconds=task_timeout_s(spec.timeout_s)),
        )(job=spec.name, job_args=list(spec.args), timeout_s=spec.timeout_s)

    brands = plan_run()
    checks = landing_check.expand(brand=brands)
    healthy = summarize_landing(brands, checks)

    previous: Any = healthy
    for spec in (*BRONZE_JOBS, DLQ_GATE_JOB, *SILVER_JOBS, RECONCILE_JOB):
        current = spark(spec)
        previous >> current
        previous = current

    gates = dlq_rate_gate.expand(brand=healthy)
    dbt = dbt_build()
    previous >> gates >> dbt
    [checks, gates, dbt] >> run_verdict()


lakehouse_pipeline()
