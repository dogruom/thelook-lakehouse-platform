# Terraform: AWS foundation (W5)

Infrastructure for the cloud track: S3 data bucket, Glue Data Catalog, Athena workgroup, IAM,
GitHub OIDC and ten Glue 5.0 jobs. Decisions: D-102 (layout, state, budgets), D-103 (Glue job
model), D-104 (Athena over Silver), D-105 (OIDC and environment gating); ADR-0008 (cost guardrails)
and ADR-0011 (Glue instead of EMR Serverless). Status: written and statically reviewed only;
nothing has been planned or applied against AWS yet.

## Layout

```
infra/terraform/
  bootstrap/        state bucket + AWS Budgets, local state, applied once
  envs/dev/         ephemeral environment (S3 backend), apply -> run -> destroy
  modules/
    storage/        data bucket, SSE-S3, TLS-only policy, lifecycle rules
    catalog/        Glue databases (bronze, dead_letter, silver, ops, athena_silver) + Athena workgroup
    identity/       Glue job role, tenant demo role, GitHub OIDC provider + deploy role, log groups
    compute/        one aws_glue_job per pipeline job (for_each)
  .tflint.hcl       tflint config (terraform recommended preset + AWS ruleset)
```

Requires Terraform >= 1.10 (native S3 state locking, no DynamoDB) and AWS provider ~> 6.0.

## Owner sequence

Prerequisite: short-lived owner credentials in the shell (`aws login` or MFA role assumption);
never access keys in files. Every `apply` and `destroy` needs explicit owner approval.

```powershell
# 1. Once: state bucket and budgets (local state)
cd infra/terraform/bootstrap
copy terraform.tfvars.example terraform.tfvars      # set budget_email
terraform init; terraform plan; terraform apply

# 2. Per cloud day: environment
cd ../envs/dev
copy backend.hcl.example backend.hcl                # put the account id into the bucket name
copy terraform.tfvars.example terraform.tfvars      # set owner_principal_arn
terraform init -backend-config=backend.hcl
terraform plan; terraform apply

# 3. Run jobs (scripts/cloud/run_job.py), record spend, then tear down
terraform destroy
```

The bootstrap state is a local file (git-ignored, no secrets). Migrating it into the bucket it
created is optional: add a `backend "s3"` block with key `bootstrap/terraform.tfstate` and run
`terraform init -migrate-state`. The state bucket has `prevent_destroy` and is never destroyed
as part of a cloud day. After `terraform apply` set the GitHub repository variables
(not secrets) `AWS_REGION`, `AWS_DEPLOY_ROLE_ARN` and `LAKEHOUSE_AWS_BUCKET` from the outputs.

If the account already has a GitHub OIDC provider, set `create_github_oidc_provider = false`
(an account can hold only one provider per URL).

## Cost per module

Nothing in this tree bills while idle. No NAT Gateway, MSK, EC2 or always-on endpoint.

| Component | Idle cost | Cost driver |
|---|---|---|
| bootstrap: state bucket | cents (a few KB of state) | S3 storage |
| bootstrap: AWS Budgets | one budget without actions; no charge expected (verify in Billing) | none |
| storage: data bucket | storage of the current day's data; lifecycle expires landing, checkpoints, glue-temp after 14 d and athena-results after 7 d | S3 storage and requests |
| catalog: Glue databases | no charge expected at this object count (verify in Billing) | none at this scale |
| catalog: Athena workgroup | none | per query, 1 GiB scan cutoff per query |
| identity: IAM, OIDC, log groups | none | CloudWatch Logs ingestion (retention 14 d) |
| compute: Glue jobs | none | DPU-seconds per run (G.1X x 2 workers, 30 min timeout, no retries) |

Per-run spend is estimated by `scripts/cloud/run_job.py` and recorded in `docs/cloud_runs.csv`;
the measured figure comes from Billing / Cost Explorer. Estimates are not evidence.

## Checks

```powershell
terraform fmt -check -recursive infra/terraform
cd infra/terraform/envs/dev; terraform init -backend=false; terraform validate
cd ../../bootstrap;          terraform init -backend=false; terraform validate
cd ..; tflint --init; tflint --recursive --config "$PWD/.tflint.hcl"
trivy config infra/terraform --ignorefile .trivyignore
```

The `.terraform.lock.hcl` files in `bootstrap/` and `envs/dev/` are committed so provider
versions and hashes are reproducible. `backend.hcl`, `*.tfvars`, `.terraform/` and state files are
git-ignored.

## Trivy exceptions (`.trivyignore`)

Kept minimal; IAM wildcard findings are fixed, never ignored. The CI gate (`--severity
HIGH,CRITICAL`) does not need exceptions for the lower-severity S3 access-logging, S3 versioning
and log-group CMK findings; they stay documented design choices (ephemeral dev bucket, ADR-0008).

| Rule | Finding | Why accepted |
|---|---|---|
| AWS-0132 | S3 encrypted with the AWS-managed key, not a customer-managed KMS key | SSE-S3 per ADR-0008; a KMS key costs USD 1/month |

## Glue log groups

`/aws-glue/jobs/{output,error,logs-v2}` are account-wide names that Glue also creates on its own. If
they already exist (another Glue user in the account), set `manage_glue_log_groups = false` in your
git-ignored variables file or `terraform import` them; otherwise apply fails with "already exists".

## Security notes

- Glue job role: trust limited with `aws:SourceAccount`; S3 object access per prefix (read-write on
  bronze, silver, gold, dead_letter, ops, checkpoints, warehouse, glue-temp; read-only on landing
  and artifacts); `s3:ListBucket` restricted by `s3:prefix` (`StringLikeIfExists`: a listing
  without a prefix returns key names only, D-103); Glue catalog actions limited to
  the five databases and their tables; logs limited to `/aws-glue/*`.
- Deploy role: trust requires `aud = sts.amazonaws.com` and
  `sub = repo:<github_repo>:environment:<github_environment>` (StringEquals), 1 h sessions.
- Tenant demo role: read `landing/brand_alpha/*` only; prefix scoping is the demonstration, not
  a substitute for the `brand_id` key rule inside the tables.
