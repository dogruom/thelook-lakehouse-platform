"""Run plan of the ``lakehouse_pipeline`` DAG: parameters, job lists, trigger-rule table (W4.D22/D23).

Everything here is plain data or a pure function; ``dags/lakehouse_pipeline.py`` only wires it to
Airflow. Failure-isolation design (W4.D23.1), per task:

* ``landing_check`` is mapped per brand (``.expand``). Mapped instances are independent: a failed
  brand does not fail, skip or retry the others.
* ``summarize_landing`` (``all_done``) runs whatever happened above, keeps the brands whose check
  succeeded and skips the rest of the run only when no brand is healthy.
* The shared chain (Bronze, ``dlq_gate``, Silver, ``reconcile``) keeps the default ``all_success``:
  one failed shared job halts everything after it, because every later job reads its output.
* ``dlq_rate_gate`` is mapped per healthy brand; ``dbt_build`` keeps ``all_success``, so one brand
  above the DLQ-rate threshold halts Gold (Gold marts are multi-tenant tables built in one run).
* Isolation scope: per-brand isolation exists in ``landing_check`` and ``dlq_rate_gate`` only; the
  Bronze, ``dlq_gate``, Silver and ``reconcile`` jobs process all brands in one run.
* ``run_verdict`` (``one_failed``) is the only leaf that can fail on purpose: it turns "a brand
  failed but the other brands completed" into a failed DAG run instead of a silently green one.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Final

from lakehouse.contracts.models import BRAND_ID_PATTERN


DAG_ID: Final = "lakehouse_pipeline"
#: Mirrors ``GEN_TENANTS`` (the three configured tenants); the DAG ``brands`` param overrides it.
DEFAULT_BRANDS: Final = ("brand_alpha", "brand_beta", "brand_gamma")
#: ``fail_brand`` value that injects no failure.
NO_FAILURE: Final = "none"
#: Default per-brand, per-source DLQ-rate ceiling (D-099): about twice the planned defect budget
#: (DATA_CONTRACTS: at most ~8 % of a source's events become DLQ rows), so planned defects pass
#: and gross corruption halts the run. A starting value, to be calibrated on measured runs.
DEFAULT_MAX_DLQ_RATE: Final = 0.15

# Airflow ``TriggerRule`` values as plain strings (the DAG file needs no enum import).
TRIGGER_ALL_SUCCESS: Final = "all_success"
TRIGGER_ALL_DONE: Final = "all_done"
TRIGGER_ONE_FAILED: Final = "one_failed"

#: ``(task id, trigger rule, why)`` - the documented rule per task (W4.D23.1, ADR-0010).
TRIGGER_RULES: Final = (
    (
        "landing_check",
        TRIGGER_ALL_SUCCESS,
        "mapped per brand; each instance depends only on plan_run, so brands are isolated",
    ),
    (
        "summarize_landing",
        TRIGGER_ALL_DONE,
        "must run after failed brand checks to collect the healthy brands; skips the run only "
        "when none is healthy",
    ),
    (
        "bronze_stream..reconcile",
        TRIGGER_ALL_SUCCESS,
        "shared chain: each job reads the previous job's tables, a failure halts the rest",
    ),
    (
        "dlq_rate_gate",
        TRIGGER_ALL_SUCCESS,
        "mapped per healthy brand; a breach fails only that brand's instance",
    ),
    (
        "dbt_build",
        TRIGGER_ALL_SUCCESS,
        "halts when any brand breached the DLQ-rate threshold (Gold is built for all brands)",
    ),
    (
        "run_verdict",
        TRIGGER_ONE_FAILED,
        "fails the run when any upstream task failed, so a degraded run is never green",
    ),
)

# Retries and timeouts (seconds). Spark jobs are idempotent (availableNow checkpoints, MERGE), so
# one retry is safe; gates are deterministic, so they are not retried.
LANDING_RETRIES: Final = 2
SPARK_RETRIES: Final = 1
RETRY_DELAY_S: Final = 60
LANDING_TIMEOUT_S: Final = 120
#: Airflow task timeout = Spark job timeout + this margin, so the wrapper stops the driver first.
TASK_TIMEOUT_MARGIN_S: Final = 120
DBT_TIMEOUT_S: Final = 1200

_BRAND_RE = re.compile(BRAND_ID_PATTERN.removesuffix("$") + r"\Z")


@dataclass(frozen=True)
class JobSpec:
    """One Spark job of the pipeline: ``lakehouse.<component>.jobs.<name>`` plus its arguments."""

    name: str
    args: tuple[str, ...] = ()
    timeout_s: int = 900
    retries: int = SPARK_RETRIES


#: Kafka and landing files -> Bronze (``availableNow``: drains what is new, then stops).
BRONZE_JOBS: Final = (JobSpec("bronze_stream"), JobSpec("bronze_batch"))
#: Contract gate for every Bronze table that has a DLQ table (D-077: file tables only when named).
DLQ_GATE_JOB: Final = JobSpec(
    "dlq_gate",
    (
        "--table",
        "raw_customers",
        "--table",
        "raw_clickstream",
        "--table",
        "raw_orders",
        "--table",
        "raw_products",
    ),
)
#: Silver in dependency order (orders check their customer FK, history follows customers).
SILVER_JOBS: Final = (
    JobSpec("silver_customers"),
    JobSpec("silver_customers_history"),
    JobSpec("silver_orders"),
    JobSpec("silver_products"),
    JobSpec("silver_clickstream"),
)
#: A mismatch is deterministic: no retry.
RECONCILE_JOB: Final = JobSpec("reconcile", timeout_s=600, retries=0)
DLQ_RATE_GATE_JOB: Final = "dlq_rate_gate"
DLQ_RATE_GATE_TIMEOUT_S: Final = 600
#: dbt selection of the Gold build (D-095): staging views, then the marts.
DBT_SELECT: Final = ("staging", "marts")


@dataclass(frozen=True)
class RunPlan:
    """Validated DAG parameters of one run."""

    brands: tuple[str, ...]
    fail_brand: str
    max_dlq_rate: float


def is_valid_brand_id(value: object) -> bool:
    """True for a string that matches the tenant-id contract (``brand_[a-z0-9_]{1,40}``)."""
    return isinstance(value, str) and _BRAND_RE.match(value) is not None


def validate_max_dlq_rate(value: object) -> float:
    """``value`` as a float in ``[0, 1]``; ``ValueError`` otherwise (also for NaN and booleans)."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"max_dlq_rate must be a number, got {type(value).__name__}")
    rate = float(value)
    if math.isnan(rate) or not 0.0 <= rate <= 1.0:
        raise ValueError(f"max_dlq_rate must be within [0, 1], got {rate}")
    return rate


def build_run_plan(
    brands: object = DEFAULT_BRANDS,
    fail_brand: object = NO_FAILURE,
    max_dlq_rate: object = DEFAULT_MAX_DLQ_RATE,
) -> RunPlan:
    """Validate the DAG params (``ValueError`` names the problem, never echoes a bad brand).

    ``brands`` must be a non-empty list of unique, contract-valid brand ids; ``fail_brand`` is
    ``"none"`` or one of ``brands``.
    """
    if not isinstance(brands, list | tuple) or not brands:
        raise ValueError("brands must be a non-empty list")
    if not all(is_valid_brand_id(b) for b in brands):
        raise ValueError("brands contains an id outside the tenant-id contract")
    if len(set(brands)) != len(brands):
        raise ValueError("brands must be unique")
    if fail_brand != NO_FAILURE and fail_brand not in brands:
        raise ValueError("fail_brand must be 'none' or one of brands")
    return RunPlan(tuple(brands), str(fail_brand), validate_max_dlq_rate(max_dlq_rate))


def should_inject_failure(brand: str, fail_brand: str) -> bool:
    """True when the landing check of ``brand`` must fail on purpose (failure-isolation test)."""
    return fail_brand != NO_FAILURE and brand == fail_brand


def task_timeout_s(spark_timeout_s: int) -> int:
    """Airflow ``execution_timeout`` (seconds) for a Spark job with ``spark_timeout_s``."""
    return spark_timeout_s + TASK_TIMEOUT_MARGIN_S


def pipeline_job_names() -> tuple[str, ...]:
    """Spark job names in execution order (excluding the per-brand ``dlq_rate_gate``)."""
    jobs = (*BRONZE_JOBS, DLQ_GATE_JOB, *SILVER_JOBS, RECONCILE_JOB)
    return tuple(j.name for j in jobs)
