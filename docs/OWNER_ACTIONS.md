# Owner Actions — what only the owner can do

> **Document Version**: 1.0
> **Last Updated**: 2026-11-14
> **Purpose**: one checklist of every step that needs the owner (credentials, accounts, consoles,
> approvals, tags). Commands are PowerShell from the repository root unless stated otherwise.
> Details for the AWS steps live in [runbooks/aws-owner-setup.md](runbooks/aws-owner-setup.md)
> (on branch `feat/w5-aws-foundation` until PR #20 is merged); this file is the short path.
> Tick a box when done and paste the "Send to Claude" output into the chat — never paste secrets.

Order matters only where noted. Blocks A and B take ~15 minutes; C–E are one AWS day
(~2–3 hours, < USD 2 estimated, credits only); F is W6 preparation.

---

## A. Local machine (≈ 10 min) — unblocks long local runs

- [ ] **A1. Give Docker more memory and restart it** (three engine crashes on 2026-11-14).
  Create or edit `%UserProfile%\.wslconfig` ([Microsoft docs](https://learn.microsoft.com/windows/wsl/wsl-config)):
  ```ini
  [wsl2]
  memory=10GB
  swap=4GB
  ```
  Then:
  ```powershell
  wsl --shutdown          # stops Docker Desktop's VM; restart Docker Desktop afterwards
  docker info --format "{{.MemTotal}}"   # expect ~10 GB instead of ~7.6 GB
  ```
  Pick `memory` ≤ half of your physical RAM if 10 GB is too much. Send to Claude: the `MemTotal` number.

- [ ] **A2. Add the missing keys to `.env`** (Airflow JWT secret + generator defaults; values are
  generated randomly, nothing is printed):
  ```powershell
  .\.venv\Scripts\Activate.ps1
  python scripts/bootstrap_env.py --add-missing
  python scripts/check_env.py            # expect "all keys set"
  docker compose --profile orchestration up -d --force-recreate airflow-scheduler airflow-api-server airflow-dag-processor
  ```
  Send to Claude: the last line of `check_env.py`.

- [ ] **A3. Full e2e run including `land`** (W4.D24.1 — the host shell needs the local SeaweedFS
  credentials; this loads only the three S3 keys into this shell, without printing them):
  ```powershell
  .\.venv\Scripts\Activate.ps1
  docker compose --profile core --profile sql up -d
  Get-Content .env | Where-Object { $_ -match '^(S3_ACCESS_KEY|S3_SECRET_KEY)=' } |
    ForEach-Object { $k, $v = $_ -split '=', 2; Set-Item "env:$k" $v }
  $env:S3_ENDPOINT = "http://127.0.0.1:9000"
  python scripts/run_e2e.py              # ~30 min; expect every step "ok"
  Remove-Item env:S3_ACCESS_KEY, env:S3_SECRET_KEY
  ```
  Send to Claude: the final summary block (step names, ok/failed, seconds).

## B. GitHub (≈ 5 min)

- [ ] **B1. Review the open PRs** — [pull requests](https://github.com/dogruom/thelook-lakehouse-platform/pulls):
  [#21](https://github.com/dogruom/thelook-lakehouse-platform/pull/21) Airflow (D22/D23) →
  [#19](https://github.com/dogruom/thelook-lakehouse-platform/pull/19) e2e + contracts (D24) →
  [#20](https://github.com/dogruom/thelook-lakehouse-platform/pull/20) AWS (W5).
  Claude merges them in this order under the standing approval (D-032) once CI is green, the review
  passes and the day is closed; you only need to look if Claude reports a stop.
- [ ] **B2. Tag `v0.2.0`** — only after Claude reports "M2 review: reviewer APPROVE, auditor PASS"
  and the M2 branches are merged to `main`:
  ```powershell
  git switch main; git pull --ff-only
  git log --oneline -3                   # check the M2 merge commit is on top
  git tag -a v0.2.0 -m "M2: data correctness and analytics products"
  git push origin v0.2.0
  ```
  Check: [tags](https://github.com/dogruom/thelook-lakehouse-platform/tags).

## C. AWS day 1 — account and bootstrap (once, ≈ 45 min)

- [ ] **C1. Account readiness (W5.D25.1)**
  - Confirm the Free plan and credits: [Billing → Free tier / Credits](https://console.aws.amazon.com/billing/home#/credits).
  - Root MFA: [Security credentials](https://console.aws.amazon.com/iam/home#/security_credentials) → Assign MFA. No root access keys.
  - Tools:
    ```powershell
    winget install --id Amazon.AWSCLI -e
    winget install --id Hashicorp.Terraform -e
    # open a new terminal
    aws --version; terraform version       # AWS CLI >= 2.32, Terraform >= 1.10
    aws login --profile crm --region eu-central-1
    $env:AWS_PROFILE = "crm"
    aws sts get-caller-identity
    ```
  Send to Claude: the `get-caller-identity` JSON (account id and ARN are not secrets) and which login path you used.
  Install pages: [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) · [Terraform](https://developer.hashicorp.com/terraform/install).

- [ ] **C2. Bootstrap: state bucket + budgets (W5.D25.2/.3)**
  ```powershell
  git switch feat/w5-aws-foundation      # until PR #20 is merged
  cd infra/terraform/bootstrap
  $env:TF_VAR_budget_email = "<your e-mail>"
  terraform init
  terraform plan -out bootstrap.plan     # expect: 1 bucket (+ settings), 1 budget
  terraform apply bootstrap.plan
  terraform output
  cd ../../..
  ```
  Then confirm the budget e-mail subscription and check [Budgets](https://console.aws.amazon.com/billing/home#/budgets) (4 alerts: 5/20/40 USD).
  Keep `infra/terraform/bootstrap/terraform.tfstate` (git-ignored; never commit or paste it).
  Send to Claude: the plan summary line ("Plan: N to add…") and `terraform output` (bucket name).

## D. AWS cloud day — apply, run, verify, tear down (≈ 1.5 h)

- [ ] **D1. Apply `envs/dev` (W5.D26/W5.D27.2)** — send the plan to Claude **before** applying:
  ```powershell
  aws login --profile crm --region eu-central-1; $env:AWS_PROFILE = "crm"
  cd infra/terraform/envs/dev
  Copy-Item backend.hcl.example backend.hcl   # put the state bucket name in it (untracked)
  terraform init -backend-config=backend.hcl
  $env:TF_VAR_owner_principal_arn = (aws sts get-caller-identity --query Arn --output text)
  terraform plan -out dev.plan -no-color > dev-plan.txt   # send dev-plan.txt to Claude (no secrets in it)
  terraform apply dev.plan
  terraform output
  $env:LAKEHOUSE_AWS_BUCKET = (terraform output -raw data_bucket)
  cd ../../..
  ```
- [ ] **D2. Wheel + data + Glue runs (W5.D27.1/.3, W5.D28.1)**
  ```powershell
  .\.venv\Scripts\Activate.ps1
  python scripts/cloud/build_artifacts.py
  python -m lakehouse.generator land --target aws
  docker compose --profile core up -d    # seed_bronze copies the Kafka-sourced Bronze tables
  python scripts/cloud/seed_bronze.py
  python scripts/cloud/seed_bronze.py --apply
  python scripts/cloud/run_job.py register_tables --record
  python scripts/cloud/run_job.py --sequence cloud --record
  ```
  Watch: [Glue jobs](https://eu-central-1.console.aws.amazon.com/gluestudio/home?region=eu-central-1#/jobs).
  Send to Claude: the `run_job.py` output (job names, SUCCEEDED/FAILED, DPU-seconds).
- [ ] **D3. Athena + Gold on Athena (W5.D28.4)** — [Athena console](https://eu-central-1.console.aws.amazon.com/athena/home?region=eu-central-1#/query-editor), workgroup `crm-lakehouse-dev`; commands in [runbooks/aws-owner-setup.md §4](runbooks/aws-owner-setup.md).
  Send to Claude: `dbt build --target athena` summary line (PASS/WARN/ERROR).
- [ ] **D4. Tenant prefix denial (W5.D28.2)** — commands in [runbooks/aws-owner-setup.md §4](runbooks/aws-owner-setup.md).
  Send to Claude: the success listing for `brand_alpha/` and the `AccessDenied` line for `brand_beta/`.
- [ ] **D5. Teardown + spend (W5.D30.3)** — never skip:
  ```powershell
  terraform -chdir=infra/terraform/envs/dev destroy
  aws glue get-jobs --query "Jobs[].Name"     # expect []
  aws s3 ls | Select-String crm-lakehouse     # only the tfstate bucket remains
  ```
  Spend: [Bills](https://console.aws.amazon.com/billing/home#/bills) or [Cost Explorer](https://console.aws.amazon.com/costmanagement/home#/cost-explorer) (data lags up to 24 h).
  Send to Claude: the two check outputs and the day's spend (USD, gross and after credits).

## E. GitHub deploy environment (once, W5.D29.2 — after D1 on a later cloud day)

- [ ] [Settings → Environments](https://github.com/dogruom/thelook-lakehouse-platform/settings/environments) → New `aws-dev`: required reviewer = you; deployment branches = `main` only.
- [ ] [Settings → Actions variables](https://github.com/dogruom/thelook-lakehouse-platform/settings/variables/actions) (**Variables**, not secrets):
  `AWS_REGION = eu-central-1`, `AWS_DEPLOY_ROLE_ARN = <terraform output github_deploy_role_arn>`,
  `LAKEHOUSE_AWS_BUCKET = <terraform output data_bucket>`.
- [ ] [Actions](https://github.com/dogruom/thelook-lakehouse-platform/actions) → `deploy-and-run` → Run workflow → approve. Send to Claude: the run link. Then teardown (D5).

## F. W6 preparation — Databricks Free Edition (once, ≈ 10 min)

- [ ] Sign up: [Databricks Free Edition](https://www.databricks.com/learn/free-edition) (no credit card, no paid tier — ADR-0012).
- [ ] Install the CLI and log in (OAuth, no tokens in files):
  ```powershell
  winget install Databricks.DatabricksCLI
  databricks auth login --host https://<your-workspace>.cloud.databricks.com
  databricks current-user me
  ```
  Send to Claude: the workspace host and `current-user me` user name.

## G. Learning gates (any time)

- [ ] Tutor gates for W4 and W5: say "beni test et W4" / "beni test et W5" in a session.
