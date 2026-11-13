# ADR-0010: Orchestration ↔ compute interface (Airflow triggers, Spark owns state)

- **Status:** Proposed (W4.D22, code written, not yet run: the Airflow image has not been built)
- **Date:** 2026-11-13

## Context
The platform needs one parametrised, re-runnable pipeline (landing check → Bronze → DLQ gate →
Silver → reconcile → quality gate → Gold) with failure isolation per brand, quality gates and
alerts. Airflow 3 is the scheduler; the compute is Spark (local standalone cluster, AWS Glue 5.0
on AWS, ADR-0011). ADR-0009 (rule 5) already says orchestration triggers components through their
public CLI / job interface only, and `.claude/rules/data-platform.md` says "Airflow triggers runs,
Spark owns stream state".

The W4.D22 day card in `DELIVERY_PLAN.md` still reads "EMR Serverless operator on AWS". That is
**superseded**: ADR-0011 replaced EMR Serverless with AWS Glue 5.0 (EMR is not on the owner's
account plan), so the AWS side of this interface is a Glue job operator.

## Decision
**Airflow decides when and in which order; Spark jobs own all state** (checkpoints, Delta tables,
DLQ, metrics). Airflow keeps no data state except run metadata.

**Interface = job name + arguments + exit code + structured log.** A job is `lakehouse.<component>.jobs.<name>`
(`common/jobs.py` resolves it by convention). Airflow passes the name and CLI arguments, waits for
the exit code and reads the JSON log lines (`job.succeeded` with `metrics`, `job.failed` with
`error_type`, `dlq_rate_gate.breach`). No component is imported into the DAG; the Airflow-free
logic (command builders, gate decisions, alert payloads, landing paths) lives in the independent
component `lakehouse.orchestration`, which imports only `contracts` (enforced by import-linter).

| Environment | How a job is started | Status |
|---|---|---|
| Local | `spark-submit --deploy-mode client` from the Airflow scheduler container to `spark://spark-master:7077`; code shipped as the content-addressed `lakehouse_app-<sha12>.zip` (same digest as `scripts/submit_job.py`); exit code and JSON log read from the process | code written, unverified |
| AWS (W5) | `GlueJobOperator` (Amazon provider) starts the same job name with the same arguments; Glue run state replaces the exit code, the CloudWatch log replaces stdout | planned, not built |

Only the local row is implemented in W4. The AWS row is the intended adapter behind the same
function boundary, not a claim that the DAG already runs on Glue.

**Trigger rules** (`lakehouse.orchestration.plan.TRIGGER_RULES`, the DAG imports the constants):

| Task | Rule | Why |
|---|---|---|
| `landing_check[brand]` (mapped) | `all_success` | depends only on `plan_run`; instances are independent, so one brand failing does not fail, skip or retry the others |
| `summarize_landing` | `all_done` | must run after a failed brand to collect the healthy ones; skips the rest of the run only when no brand is healthy |
| `bronze_stream` … `reconcile` | `all_success` | each job reads the previous job's tables; a failure halts the rest |
| `dlq_rate_gate[brand]` (mapped) | `all_success` | a breach fails only that brand's instance |
| `dbt_build` | `all_success` | halts when any brand breached the threshold: Gold marts are multi-tenant tables built in one run |
| `run_verdict` | `one_failed` | the only leaf that fails on purpose, so "a brand failed, the others completed" ends red, not green |

**Scope of brand isolation.** Per-brand isolation exists in two places only: the mapped
`landing_check` and the mapped `dlq_rate_gate`. `bronze_stream`, `bronze_batch`, `dlq_gate`, the
Silver jobs and `reconcile` process all brands in one run (they are not brand-scoped), so a failure
in the shared chain halts every brand. A failed brand is excluded from the DLQ-rate gate, not
from ingestion: its already landed data is still processed by the shared jobs.

**Quality gates and alerts.** The DLQ-rate gate is a Spark job (`dlq_rate_gate`) so it reads the
same Delta tables as `reconcile`; the decision itself is a pure function in `lakehouse.quality`.
Failures and breaches produce a JSON alert payload (contract in D-099) that is logged and, only if
`ALERT_WEBHOOK_URL` is set, posted to a webhook.

**Idempotency.** Every job is idempotent on its own (`availableNow` checkpoints, deterministic
`MERGE`, insert-only DLQ merge), so a second run of the same interval, or a retry, produces the same
state. This is an "idempotent outcome" of at-least-once ingestion, not exactly-once delivery.

## Consequences
- Airflow orchestrates batch runs of `availableNow` jobs. **It does not orchestrate a running
  stream**, and nothing here claims continuous streaming control.
- The DAG has no business logic and no secrets; credentials reach `spark-submit` through the
  container environment only.
- The scheduler container hosts the Spark driver JVM and the dbt subprocess (LocalExecutor), so
  its memory limit is sized for that (D-098). `core` + `orchestration` on a 7.6 GB Docker VM is a
  tight fit; `dbt build` additionally needs the `sql` profile (Thrift Server). Unmeasured.
- `run_verdict` does not alert itself: its failure only rolls up failures that already produced an
  alert (one alert per underlying failure).
- Failure semantics depend on Airflow 3 behaviour that has not been observed yet: mapped-task
  results when an instance failed, `one_failed` firing, and `airflow dags test` re-running the same
  logical date. The integration tests (`test_dag_failure_isolation.py`, `test_backfill_idempotent.py`)
  are the checks; they skip when the Airflow container is not running.
- Switching to Glue changes one adapter (how a job is started and how its result is read), not the
  DAG structure, the gates or the alert contract.

## Alternatives rejected
- `SparkSubmitOperator` / `DockerOperator`: a provider plus an Airflow connection, or the Docker
  socket inside a container, for no gain over the existing job interface.
- Airflow as stream supervisor (long-running tasks per stream): contradicts "Spark owns stream
  state" and the availableNow scheduling model.
- Gate logic inside the DAG file: untestable without Airflow, and Glue would need a copy.
