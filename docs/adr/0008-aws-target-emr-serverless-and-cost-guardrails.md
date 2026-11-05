# ADR-0008: AWS deployment target — EMR Serverless, Glue Data Catalog and cost guardrails

- **Status:** Accepted (owner, 2026-11-05) — **compute and identity superseded by [ADR-0011](0011-aws-glue-instead-of-emr-serverless.md)** (AWS Glue 5.0 instead of EMR Serverless; Identity Center unavailable on the account type). Region, Terraform approach, Glue catalog, Athena and all cost guardrails remain valid.
- **Date:** 2026-11-05

## Context
Milestone 3 must prove that the same Spark/Delta job code runs on AWS. The previous plan ran local
Spark against AWS S3, which demonstrates storage access but not managed compute, deployment or
operations. Constraints from the owner: total project cloud spend ≤ **USD 50** (excluding the Claude
subscription), nothing always-on, storage cost acceptable, AWS Free Tier credits should be used.
Job-market sample (DELIVERY_PLAN §6): AWS appears in more postings than other clouds; Terraform,
CI/CD, data quality and cost awareness recur; serverless/managed compute is valued.

## Decision
- **Compute:** Amazon EMR Serverless (release 7.x, Spark 3.5) running the same `lakehouse` wheel as
  the local stack, using a **custom image** with Python 3.11 (ADR-0007 runtime parity).
  Fallback if custom images are blocked: venv packaging; last resort: local Spark → S3 (old plan).
- **Catalog:** AWS Glue Data Catalog. **Ad-hoc SQL:** Amazon Athena on Glue tables (pay per query).
- **Region:** `eu-central-1`.
- **Provisioning:** Terraform only. State in an S3 bucket with versioning and native S3 lockfile
  (no DynamoDB). Modules: storage (S3, lifecycle, SSE-S3, public-access block), catalog (Glue DBs),
  compute (EMR Serverless application: auto-start, auto-stop after 15 min idle, maximum capacity
  cap), identity (EMR execution role, prefix-scoped tenant demo policy, GitHub OIDC role), budget.
- **Identity:** IAM Identity Center (SSO) for the owner; GitHub Actions via OIDC; no long-lived keys.
- **Kafka stays local** (no MSK): the cloud proof covers storage, catalog, managed Spark, IaC,
  CI/CD and operations. Bronze on AWS ingests landing files.
- **Gold on AWS:** not deployed in v1 (dbt-spark needs a Thrift endpoint); Silver/Gold tables are
  queried via Athena where supported. Stated explicitly in `docs/LOCAL_VS_CLOUD.md`.

## Cost guardrails
| Guardrail | Setting |
|---|---|
| AWS Budgets | Alerts at USD 5 / 20 / 40 (actual) and forecast > 40 |
| No idle cost | No NAT Gateway, no MSK, no EC2, no always-on endpoints; EMR auto-stop 15 min |
| Capacity cap | EMR application maximum capacity (e.g. 16 vCPU / 64 GB) |
| Storage | Lifecycle: landing and checkpoints expire after 14 days; abort incomplete multipart uploads after 1 day |
| Logs | EMR logs to S3; any CloudWatch log group retention ≤ 14 days |
| Teardown | `terraform destroy` of compute and data after each cloud session; state bucket kept |
| Evidence | Actual spend recorded from Billing/Cost Explorer per cloud day |

Expected spend (estimate, to be replaced by measured spend): EMR Serverless small runs ≈ USD 0.10–0.30
each, a few GB of S3, negligible Glue/Athena → **well under USD 15** for the whole milestone.

## Consequences
- Stronger, verifiable claim: "same job code on local Spark and EMR Serverless, deployed by Terraform
  and GitHub OIDC".
- Account readiness is an owner task at the start of M3 (W5.D25): the account plan (Free vs Paid)
  and EMR Serverless availability under the Free plan must be checked; upgrading to the Paid plan
  keeps remaining credits.
- Databricks is removed from v1 (documented next-stage work).
