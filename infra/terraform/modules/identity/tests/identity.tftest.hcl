# Offline checks of the identity module (D-103, D-105). Mocked AWS provider, no credentials.
# The rendered JSON of aws_iam_policy_document is mocked, so the statement/condition inputs of
# the documents are asserted. It asserts plan-time configuration only and does NOT replace a
# real `terraform plan` against AWS (owner, before the first apply).

# The rendered policy JSON of aws_iam_policy_document is computed; the mock would return a random
# string, which the IAM resources reject as invalid JSON. A minimal valid document keeps the plan
# going; the assertions read the statement/condition inputs, not this JSON.
mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

# The existing-provider path (data source) gives the trust policy a known principal ARN.
variables {
  name_prefix                 = "crm-lakehouse-dev"
  account_id                  = "123456789012"
  region                      = "eu-central-1"
  data_bucket_arn             = "arn:aws:s3:::crm-lakehouse-dev-123456789012"
  glue_database_names         = ["bronze", "dead_letter", "silver", "ops", "athena_silver"]
  owner_principal_arn         = "arn:aws:iam::123456789012:user/owner"
  github_repo                 = "dogruom/thelook-lakehouse-platform"
  github_environment          = "aws-dev"
  create_github_oidc_provider = false
}

run "github_oidc_trust" {
  command = plan

  assert {
    condition     = length(data.aws_iam_policy_document.github_trust.statement) == 1
    error_message = "The deploy role trust policy must have exactly one statement."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.github_trust.statement : s.actions])) == toset(["sts:AssumeRoleWithWebIdentity"])
    error_message = "The deploy role must be assumable only through sts:AssumeRoleWithWebIdentity."
  }

  assert {
    condition     = length(flatten([for s in data.aws_iam_policy_document.github_trust.statement : s.condition])) == 2
    error_message = "The trust policy must carry exactly the aud and sub conditions."
  }

  assert {
    condition     = alltrue(flatten([for s in data.aws_iam_policy_document.github_trust.statement : [for c in s.condition : c.test == "StringEquals"]]))
    error_message = "Every OIDC condition must use StringEquals (no StringLike wildcards, D-105)."
  }

  assert {
    condition     = anytrue(flatten([for s in data.aws_iam_policy_document.github_trust.statement : [for c in s.condition : c.variable == "token.actions.githubusercontent.com:aud" && toset(c.values) == toset(["sts.amazonaws.com"])]]))
    error_message = "The aud claim must be sts.amazonaws.com."
  }

  assert {
    condition     = anytrue(flatten([for s in data.aws_iam_policy_document.github_trust.statement : [for c in s.condition : c.variable == "token.actions.githubusercontent.com:sub" && toset(c.values) == toset(["repo:dogruom/thelook-lakehouse-platform:environment:aws-dev"])]]))
    error_message = "The sub claim must be exactly repo:dogruom/thelook-lakehouse-platform:environment:aws-dev."
  }

  assert {
    condition     = aws_iam_role.github_deploy.max_session_duration == 3600
    error_message = "The deploy role session must be limited to one hour."
  }

  assert {
    condition     = aws_iam_role.github_deploy.name == "crm-lakehouse-dev-github-deploy"
    error_message = "Unexpected deploy role name."
  }
}

run "github_oidc_provider_created" {
  command = plan

  variables {
    create_github_oidc_provider = true
  }

  assert {
    condition     = length(aws_iam_openid_connect_provider.github) == 1
    error_message = "The OIDC provider must be created when create_github_oidc_provider = true."
  }

  assert {
    condition     = aws_iam_openid_connect_provider.github[0].url == "https://token.actions.githubusercontent.com"
    error_message = "The OIDC provider URL must be the GitHub Actions issuer."
  }

  assert {
    condition     = toset(aws_iam_openid_connect_provider.github[0].client_id_list) == toset(["sts.amazonaws.com"])
    error_message = "The OIDC provider audience must be sts.amazonaws.com."
  }
}

run "github_deploy_permissions" {
  command = plan

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.github_deploy.statement : s.resources if s.sid == "ReadWriteArtifacts"])) == toset(["arn:aws:s3:::crm-lakehouse-dev-123456789012/artifacts/*"])
    error_message = "The deploy role may write only under artifacts/."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.github_deploy.statement : s.actions if s.sid == "ReadWriteArtifacts"])) == toset(["s3:GetObject", "s3:PutObject"])
    error_message = "The deploy role may only get and put artifacts."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.github_deploy.statement : s.resources if s.sid == "RunGlueJobs"])) == toset(["arn:aws:glue:eu-central-1:123456789012:job/crm-lakehouse-dev-*"])
    error_message = "The deploy role may run only the project's Glue jobs."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.github_deploy.statement : s.actions if s.sid == "RunGlueJobs"])) == toset(["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"])
    error_message = "The deploy role Glue actions are limited to start/get runs."
  }

  assert {
    condition     = alltrue(flatten([for s in data.aws_iam_policy_document.github_deploy.statement : [for a in s.actions : !strcontains(a, "*")]]))
    error_message = "No wildcard actions on the deploy role."
  }
}

run "glue_job_role" {
  command = plan

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.glue_trust.statement : [for p in s.principals : p.identifiers]])) == toset(["glue.amazonaws.com"])
    error_message = "Only the Glue service may assume the job role."
  }

  assert {
    condition     = anytrue(flatten([for s in data.aws_iam_policy_document.glue_trust.statement : [for c in s.condition : c.test == "StringEquals" && c.variable == "aws:SourceAccount" && toset(c.values) == toset(["123456789012"])]]))
    error_message = "The Glue trust must be pinned to this account (aws:SourceAccount)."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.glue_job.statement : s.resources if s.sid == "ReadWriteDataPrefixes"])) == toset([for p in ["bronze", "silver", "gold", "dead_letter", "ops", "checkpoints", "warehouse", "glue-temp"] : "arn:aws:s3:::crm-lakehouse-dev-123456789012/${p}/*"])
    error_message = "Read/write object access must be limited to the eight working prefixes."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.glue_job.statement : s.resources if s.sid == "ReadOnlyPrefixes"])) == toset(["arn:aws:s3:::crm-lakehouse-dev-123456789012/landing/*", "arn:aws:s3:::crm-lakehouse-dev-123456789012/artifacts/*"])
    error_message = "Read-only object access must be limited to landing/ and artifacts/."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.glue_job.statement : s.actions if s.sid == "ReadOnlyPrefixes"])) == toset(["s3:GetObject"])
    error_message = "Read-only prefixes allow s3:GetObject only."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.glue_job.statement : s.resources if s.sid == "ListDataBucketPrefixes"])) == toset(["arn:aws:s3:::crm-lakehouse-dev-123456789012"])
    error_message = "Listing is allowed on the data bucket only."
  }

  assert {
    condition     = alltrue(flatten([for s in data.aws_iam_policy_document.glue_job.statement : [for a in s.actions : !strcontains(a, "*")]]))
    error_message = "No wildcard actions on the Glue job role (infra rules: no * actions on data buckets)."
  }

  assert {
    condition     = alltrue(flatten([for s in data.aws_iam_policy_document.glue_job.statement : [for r in s.resources : r != "*" && r != "arn:aws:s3:::crm-lakehouse-dev-123456789012/*"]]))
    error_message = "No statement may cover all resources or the whole bucket."
  }
}

run "glue_log_groups" {
  command = plan

  assert {
    condition     = toset(keys(aws_cloudwatch_log_group.glue)) == toset(["/aws-glue/jobs/output", "/aws-glue/jobs/error", "/aws-glue/jobs/logs-v2"])
    error_message = "The three Glue job log groups must be managed by default."
  }

  assert {
    condition     = alltrue([for k, g in aws_cloudwatch_log_group.glue : g.retention_in_days == 14])
    error_message = "Glue log retention must be 14 days (ADR-0008)."
  }
}

run "glue_log_groups_unmanaged" {
  command = plan

  variables {
    manage_glue_log_groups = false
  }

  assert {
    condition     = length(aws_cloudwatch_log_group.glue) == 0
    error_message = "manage_glue_log_groups = false must create no log groups."
  }
}

# Tenant isolation demo: an allow-list scoped to brand_alpha. brand_beta is denied implicitly
# (IAM default deny): the policy has no statement that mentions it.
run "tenant_brand_alpha_role" {
  command = plan

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.tenant_trust.statement : [for p in s.principals : p.identifiers]])) == toset(["arn:aws:iam::123456789012:user/owner"])
    error_message = "Only the owner principal may assume the tenant demo role."
  }

  assert {
    condition     = aws_iam_role.tenant_brand_alpha.max_session_duration == 3600
    error_message = "The tenant demo role session must be limited to one hour."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.tenant_brand_alpha.statement : s.resources if s.sid == "ReadOwnPrefix"])) == toset(["arn:aws:s3:::crm-lakehouse-dev-123456789012/landing/brand_alpha/*"])
    error_message = "The tenant role may read only landing/brand_alpha/*."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.tenant_brand_alpha.statement : s.actions if s.sid == "ReadOwnPrefix"])) == toset(["s3:GetObject"])
    error_message = "The tenant role is read-only."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.tenant_brand_alpha.statement : [for c in s.condition : c.values] if s.sid == "ListOwnPrefix"])) == toset(["landing/brand_alpha/*"])
    error_message = "Listing must be restricted to the landing/brand_alpha/ prefix."
  }

  assert {
    condition     = alltrue(flatten([for s in data.aws_iam_policy_document.tenant_brand_alpha.statement : [for r in s.resources : !strcontains(r, "brand_beta")]]))
    error_message = "The tenant policy must not allow brand_beta."
  }

  assert {
    condition     = alltrue(flatten([for s in data.aws_iam_policy_document.tenant_brand_alpha.statement : [for c in s.condition : [for v in c.values : !strcontains(v, "brand_beta")]]]))
    error_message = "The tenant policy conditions must not allow brand_beta."
  }

  assert {
    condition     = alltrue(flatten([for s in data.aws_iam_policy_document.tenant_brand_alpha.statement : [for a in s.actions : !strcontains(a, "*")]]))
    error_message = "No wildcard actions on the tenant role."
  }
}
