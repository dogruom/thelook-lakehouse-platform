# Runbook: cloud teardown (`envs/dev`)

## When to use
End of every cloud day. `infra/terraform/envs/dev` is ephemeral (apply -> run -> destroy, D-102,
ADR-0008). Nothing here bills while idle, but the project ceiling is USD 50, so tear down the same day.
Names: prefix `crm-lakehouse-dev`, region `eu-central-1`, data bucket `crm-lakehouse-dev-<account_id>`,
state bucket `crm-lakehouse-tfstate-<account_id>`.

**`terraform destroy` and every `aws` command need explicit owner approval each time.**

## Preconditions
- Short-lived owner credentials in the shell (`aws login` or MFA role assumption). No access keys in
  files.
- Evidence is saved before the bucket goes away (destroy deletes all data; `force_destroy_data`
  defaults to true):
  - `docs/cloud_runs.csv` has a row per run (`run_job.py ... --record`), and the day's report is
    copied out:
    ```powershell
    $d = Get-Date -Format yyyy-MM-dd
    aws s3 cp --recursive "s3://$env:LAKEHOUSE_AWS_BUCKET/ops/reports/" "build\cloud-evidence\$d\"
    ```
    `--sequence cloud` runs `metrics_report` with `--output-uri s3://<bucket>/ops/reports`, so
    `ops/reports/<run_id>/report.md` and `report.json` exist after a full sequence (a single
    `metrics_report` run needs `--arg=--output-uri --arg=s3://<bucket>/ops/reports`). The repository
    has no `evidence/` or `docs/evidence/` convention; `build/` is git-ignored, so the raw copy
    never gets committed. Summarise it in the day-close report.
- Nothing is running (next section).

## Steps
1. **No Glue run is active** (all ten jobs: `bronze_batch`, `dlq_gate`, `silver_customers`,
   `silver_customers_history`, `silver_orders`, `silver_products`, `silver_clickstream`, `reconcile`,
   `register_tables`, `metrics_report`):
   ```powershell
   foreach ($j in 'bronze_batch','dlq_gate','silver_customers','silver_customers_history','silver_orders','silver_products','silver_clickstream','reconcile','register_tables','metrics_report') {
     aws glue get-job-runs --job-name "crm-lakehouse-dev-$j" --max-results 3 --region eu-central-1 --query "JobRuns[?JobRunState=='RUNNING' || JobRunState=='STARTING' || JobRunState=='STOPPING'].[JobName,Id,JobRunState]" --output text
   }
   ```
   Expected: no output. A running job: wait, or stop it deliberately
   (`aws glue batch-stop-job-run --job-name <name> --job-run-ids <id>`) and note it in the report.
2. **Review the destroy plan** (read-only):
   ```powershell
   cd infra/terraform/envs/dev
   terraform plan -destroy
   ```
   Expected: only `crm-lakehouse-dev` resources (bucket, Glue databases/jobs, Athena workgroup, IAM
   roles/policies, log groups, OIDC role). No `crm-lakehouse-tfstate-*` bucket and no budget: those live
   in `infra/terraform/bootstrap`, a separate stack.
3. **Drop what Terraform does not own** (owner-run, after approval). dbt-athena creates its own
   `gold_*` Glue databases and their tables (D-104); they are outside the Terraform state, so
   `destroy` would leave their catalog entries behind (the data under `gold/` is removed with the
   bucket by `force_destroy`). Read-only listing first:
   ```powershell
   aws glue get-databases --region eu-central-1 --query "DatabaseList[?starts_with(Name,'gold_')].Name" --output text
   # expected: gold_staging gold_marts gold_tenant (only those that were built)
   ```
   Then, per listed database (this deletes the database and every table in it):
   ```powershell
   aws glue delete-database --name gold_marts --region eu-central-1      # likewise gold_staging, gold_tenant
   ```
   `athena_silver` (the Delta registrations made by `register_athena_silver`) is a Terraform-managed
   database: `destroy` deletes it and, with it, the tables registered inside; nothing to do by hand.
4. **Destroy** (after owner approval):
   ```powershell
   terraform destroy
   ```
5. **Verify nothing is left** (region `eu-central-1`):
   ```powershell
   aws s3api head-bucket --bucket crm-lakehouse-dev-<account_id>        # expect 404 Not Found
   aws glue get-jobs --region eu-central-1 --query "Jobs[?starts_with(Name,'crm-lakehouse-dev')].Name" --output text   # expect empty
   aws glue get-databases --region eu-central-1 --query "DatabaseList[].Name" --output text                          # bronze, silver, ops, athena_silver, gold_* ... gone
   aws athena list-work-groups --region eu-central-1 --query "WorkGroups[?Name=='crm-lakehouse-dev'].Name" --output text # expect empty
   aws iam list-roles --query "Roles[?starts_with(RoleName,'crm-lakehouse-dev')].RoleName" --output text              # expect empty
   aws logs describe-log-groups --log-group-name-prefix /aws-glue/jobs --region eu-central-1 --query "logGroups[].logGroupName" --output text
   ```
6. **Verify what must survive**:
   ```powershell
   aws s3api head-bucket --bucket crm-lakehouse-tfstate-<account_id>     # expect success (state bucket kept)
   aws budgets describe-budgets --account-id <account_id> --query "Budgets[].BudgetName" --output text   # budget still listed
   ```
7. Record the day in the day-close report: runs, estimated spend from `docs/cloud_runs.csv`, "destroyed:
   yes", and the verification outputs (names only, no account data beyond what the report already uses).

## Verification (summary)
Data bucket `404`, no `crm-lakehouse-dev` Glue jobs / workgroup / roles, no running Glue run, state
bucket present, budget present. Optionally check the Billing console next day for unexpected lines.

## Rollback / do not
- Destroy is **not** reversible: data, Glue catalog and Athena results are gone. Rebuild with
  `terraform apply`, `python scripts/cloud/build_artifacts.py`, `python scripts/cloud/seed_bronze.py
  --apply` (dry run first) and the job sequence of `scripts/cloud/run_job.py --sequence cloud`.
- Never run `terraform destroy` in `infra/terraform/bootstrap` as part of a cloud day (the state bucket
  has `prevent_destroy`; the budgets are the cost alarm). Never delete the state bucket or its objects.
- Do not use `-target`, and do not empty or delete the data bucket by hand before destroy: that
  creates state drift. If destroy fails on a non-empty bucket, check that `force_destroy_data` was not
  overridden to false, then rerun `terraform destroy`.
- Never run destroy while a Glue run is active.
- After destroy the GitHub repository variables (`AWS_REGION`, `AWS_DEPLOY_ROLE_ARN`,
  `LAKEHOUSE_AWS_BUCKET`) point at deleted resources; the `deploy-and-run` workflow fails until the next
  apply. This is expected. If the next apply produces a different account or role ARN, update the
  variables from the Terraform outputs.
- Never read `terraform.tfvars` or state files into chat or logs.
