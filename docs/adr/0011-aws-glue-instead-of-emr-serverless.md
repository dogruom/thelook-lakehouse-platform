# ADR-0011: AWS Glue 5.0 instead of EMR Serverless for cloud compute

- **Status:** Accepted (owner, 2026-11-05) — supersedes the compute decision of ADR-0008
- **Date:** 2026-11-05

## Context
ADR-0008 chose EMR Serverless. New evidence the same day:
- The owner's AWS account uses the **Free account plan** (USD 100 credits unused, ~137 days left,
  i.e. until about 2027-03-22). In the new AWS sign-up experience **Amazon EMR is listed only under
  the Paid plan**, while **AWS Glue, Glue Data Catalog, Athena, S3, IAM, Budgets, Secrets Manager,
  Step Functions and CloudWatch are available on the Free plan**
  ([AWS docs](https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.html)).
- **AWS Glue 5.0** runs Spark 3.5.4, **Python 3.11**, Java 17 and Delta Lake 3.3.0
  ([AWS Big Data Blog](https://aws.amazon.com/blogs/big-data/introducing-aws-glue-5-0-for-apache-spark/)) —
  practically the local runtime of ADR-0007, without a custom image.
- **AWS IAM Identity Center is not supported** in the new sign-up experience unless advanced
  features are activated, so the planned SSO login is not available by default.
- The owner prepares the AWS Data Engineer Associate certification, where Glue is central.

## Decision
- Cloud compute = **AWS Glue 5.0 Spark jobs** running the same `lakehouse` wheel
  (`--additional-python-modules` / `--extra-py-files`, `--datalake-formats delta`), plain
  `SparkSession` code — no `GlueContext`/DynamicFrame dependency in shared logic.
- Catalog = Glue Data Catalog (unchanged); ad-hoc SQL = Athena (unchanged).
- Orchestration in the cloud: Airflow (local) triggers Glue jobs via the Amazon provider
  (`GlueJobOperator`) — ADR-0010 covers the interface; Step Functions is not added.
- Identity: GitHub OIDC role for CI (IAM, supported). Owner CLI access: short-lived credentials
  from the console sign-in (`aws login`, AWS CLI v2) if available on the account; otherwise an
  IAM user with MFA and role assumption, keys stored only in the OS credential store. Decided at
  W5.D25.1 after checking the account; no long-lived keys in files.
- The account stays on the **Free plan** for M3 (charges are impossible; credits are consumed).
  Before the plan ends (~2027-03-22) the owner decides: upgrade (credits carry over) or let it close.
- ECR and the EMR custom image are dropped.

## Consequences
- Saves ~0.5 day (no custom image), removes the Paid-plan prerequisite, aligns with certification study.
- Glue minimum billing per run (DPU-based, per-second with a 1-minute minimum); small jobs cost
  cents — measured per run and recorded (ADR-0008 cost guardrails still apply).
- Interview narrative: "same Spark/Delta code on local Spark 3.5 and Glue 5.0; catalog swapped from
  Hive metastore to Glue Data Catalog by configuration".
- Glue is a more AWS-specific runtime than EMR; the ADR records EMR (Serverless) as the
  alternative for heavier or long-running workloads.
