# Runbook — AWS owner setup and first cloud day (W5)

Owner-only steps: they need the AWS account, the owner's credentials or the GitHub settings
page. Run them in PowerShell from the repository root. Every step names its verification. Nothing
here creates long-lived access keys (`.claude/rules/secrets.md`, ADR-0011).

Cost frame: Free plan, credits only (ADR-0011); budgets alert at USD 5 / 20 / 40 (D-102). A full
cloud day (all Glue jobs once + a few Athena queries) is estimated at under USD 2 (target, not
measured: ~10 runs × 2 DPU × a few minutes × USD 0.44/DPU-h) — replaced by the measured figures in
`docs/cloud_runs.csv`.

## 1. Account readiness (W5.D25.1, once)

1. Root user: enable MFA (console → account menu → Security credentials → Assign MFA). Do not
   create root access keys.
2. Install the tools (one-time, user scope):
   ```powershell
   winget install --id Amazon.AWSCLI -e
   winget install --id Hashicorp.Terraform -e
   # new terminal, then:
   aws --version; terraform version      # AWS CLI >= 2.32 (has `aws login`), Terraform >= 1.10
   ```
   tflint and trivy are optional locally — CI runs them (`iac` job).
3. Short-lived CLI access with the console sign-in (no keys on disk):
   ```powershell
   aws login --profile crm --region eu-central-1
   $env:AWS_PROFILE = "crm"
   aws sts get-caller-identity          # evidence for W5.D25.1: account id + ARN (no secrets)
   ```
   If `aws login` is not available on the account: create an IAM user with MFA whose only right is
   to assume an admin role, keep its keys only in the Windows credential store, and use a
   `role_arn` + `mfa_serial` profile. Record which path was taken (D-102 follow-up).
   If Terraform does not pick up the `login` session, export it into this shell only:
   ```powershell
   aws configure export-credentials --profile crm --format powershell | Invoke-Expression
   ```

## 2. Bootstrap: state bucket + budgets (W5.D25.2/.3, once)

```powershell
cd infra/terraform/bootstrap
$env:TF_VAR_budget_email = "<your e-mail>"     # not committed; no tfvars file needed
terraform init
terraform plan -out bootstrap.plan             # review: 1 bucket (+ settings), 1 budget
terraform apply bootstrap.plan
terraform output                               # state bucket name
cd ../../..
```
Verify: AWS Budgets console shows the budget with 4 alerts; confirm the subscription e-mail.
The bootstrap state file stays local in `infra/terraform/bootstrap/` (git-ignored) — keep it.

## 3. Dev environment (W5.D27.2, every cloud day)

```powershell
cd infra/terraform/envs/dev
Copy-Item backend.hcl.example backend.hcl      # put the state bucket name in it (untracked)
terraform init -backend-config=backend.hcl
$env:TF_VAR_owner_principal_arn = (aws sts get-caller-identity --query Arn --output text)
terraform plan -out dev.plan                   # review IAM and Glue jobs; send the plan summary to Claude for review if unsure
terraform apply dev.plan
terraform output                               # data_bucket, role ARNs, glue_job_names
$env:LAKEHOUSE_AWS_BUCKET = (terraform output -raw data_bucket)
cd ../../..
```
`create_github_oidc_provider` defaults to true; set `$env:TF_VAR_create_github_oidc_provider = "false"`
if the account already has the GitHub OIDC provider. Likewise `$env:TF_VAR_manage_glue_log_groups =
"false"` if the `/aws-glue/jobs/{output,error,logs-v2}` log groups already exist (or `terraform
import` them); otherwise apply fails with "already exists".

## 4. First run (W5.D27.3 → W5.D28)

```powershell
.\.venv\Scripts\Activate.ps1
python scripts/cloud/build_artifacts.py                     # wheel + glue_entry.py → artifacts/<sha>/ and current/
# landing files to real AWS S3 (explicit opt-in): default credential chain, AWS endpoint, bucket from
# LAKEHOUSE_AWS_BUCKET (must be crm-lakehouse-*), region AWS_REGION (default eu-central-1);
# the local S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY are not used
python -m lakehouse.generator land --target aws
# copy the Kafka-sourced Bronze tables from the local stack (D-101); needs the core profile running
# and, in this shell only, the local SeaweedFS endpoint + S3_ACCESS_KEY/S3_SECRET_KEY
python scripts/cloud/seed_bronze.py                          # dry run: what would be copied
python scripts/cloud/seed_bronze.py --apply
python scripts/cloud/run_job.py register_tables --record     # first Glue run → SUCCEEDED, cost estimate
python scripts/cloud/run_job.py --sequence cloud --record    # register_tables … reconcile, metrics_report; stops at the first failure
```
Then Athena (console, workgroup `crm-lakehouse-dev`): `SELECT brand_id, count(*) FROM ...` — see
`docs/runbooks/cloud-teardown.md` for the verification queries and the dbt-athena build:
```powershell
py -3.11 -m venv .venv-dbt-athena; .\.venv-dbt-athena\Scripts\python -m pip install -r requirements/dbt-athena.txt   # once
$env:DBT_ATHENA_S3_STAGING_DIR = "s3://$env:LAKEHOUSE_AWS_BUCKET/athena-results/"
$env:DBT_ATHENA_S3_DATA_DIR = "s3://$env:LAKEHOUSE_AWS_BUCKET/gold/"
cd dbt_project
..\.venv-dbt-athena\Scripts\dbt run-operation register_athena_silver --target athena --profiles-dir profiles --vars "{athena_silver_root: 's3://$env:LAKEHOUSE_AWS_BUCKET/silver'}"
..\.venv-dbt-athena\Scripts\dbt build --target athena --select staging marts --profiles-dir profiles
cd ..
```
Tenant denial evidence (W5.D28.2):
```powershell
$creds = aws sts assume-role --role-arn (terraform -chdir=infra/terraform/envs/dev output -raw tenant_brand_alpha_role_arn) --role-session-name tenant-demo --query Credentials --output json | ConvertFrom-Json
# use them only in a child scope; expect success for brand_alpha and AccessDenied for brand_beta:
#   aws s3 ls s3://$env:LAKEHOUSE_AWS_BUCKET/landing/brand_alpha/
#   aws s3 ls s3://$env:LAKEHOUSE_AWS_BUCKET/landing/brand_beta/
```

## 5. GitHub deploy (W5.D29.2, once)

Repository → Settings → Environments → New `aws-dev`: required reviewer = you; deployment
branches = `main` only. Settings → Secrets and variables → Actions → **Variables** (not secrets):
`AWS_REGION = eu-central-1`, `AWS_DEPLOY_ROLE_ARN = <terraform output github_deploy_role_arn>`,
`LAKEHOUSE_AWS_BUCKET = <terraform output data_bucket>`. Run Actions → `deploy-and-run` → approve.

## 6. Teardown (end of every cloud day)

```powershell
cd infra/terraform/envs/dev
terraform destroy                       # data bucket is force_destroy (dev); budgets/state stay
cd ../../..
aws glue get-jobs --query "Jobs[].Name"  # expect []
aws s3 ls | Select-String crm-lakehouse  # only the tfstate bucket remains
```
Record the day's spend (Billing → Bills, or Cost Explorer) in `docs/cloud_runs.csv` notes / PROGRESS.
