# Identities of the dev environment:
#   1. Glue job execution role (least privilege, prefix-scoped)      D-103
#   2. Tenant demo role (read one brand's landing prefix only)
#   3. GitHub OIDC provider + deploy role (no long-lived keys)       D-105

locals {
  job_prefixes = concat(var.read_write_prefixes, var.read_only_prefixes)

  glue_catalog_arn = "arn:aws:glue:${var.region}:${var.account_id}:catalog"
  glue_database_arns = [
    for db in var.glue_database_names : "arn:aws:glue:${var.region}:${var.account_id}:database/${db}"
  ]
  glue_table_arns = [
    for db in var.glue_database_names : "arn:aws:glue:${var.region}:${var.account_id}:table/${db}/*"
  ]

  glue_function_arns = [
    for db in var.glue_database_names : "arn:aws:glue:${var.region}:${var.account_id}:userDefinedFunction/${db}/*"
  ]

  glue_log_groups = ["/aws-glue/jobs/output", "/aws-glue/jobs/error", "/aws-glue/jobs/logs-v2"]
  logs_arn_prefix = "arn:aws:logs:${var.region}:${var.account_id}:log-group"

  github_oidc_url = "token.actions.githubusercontent.com"
  github_oidc_provider_arn = one(concat(
    aws_iam_openid_connect_provider.github[*].arn,
    data.aws_iam_openid_connect_provider.github[*].arn,
  ))
}

# ---------------------------------------------------------------------------------------------
# CloudWatch log groups written by Glue jobs (created here so retention is bounded)
# ---------------------------------------------------------------------------------------------

# Glue creates these groups itself on the first run; set manage_glue_log_groups = false when they
# already exist in the account (or `terraform import` them) to avoid an "already exists" error.
resource "aws_cloudwatch_log_group" "glue" {
  for_each = var.manage_glue_log_groups ? toset(local.glue_log_groups) : toset([])

  name              = each.key
  retention_in_days = var.log_retention_days
}

# ---------------------------------------------------------------------------------------------
# 1. Glue job role
# ---------------------------------------------------------------------------------------------

data "aws_iam_policy_document" "glue_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "${var.name_prefix}-glue-job"
  description        = "Execution role of the CRM lakehouse Glue jobs"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

data "aws_iam_policy_document" "glue_job" {
  statement {
    # D-103: Spark/Hadoop (S3A, EMRFS) and Delta also list without an s3:prefix (bucket root,
    # existence checks of missing keys), which a plain StringLike condition would deny. With
    # StringLikeIfExists a request without a prefix is allowed (key names only, no object access:
    # GetObject stays prefix-scoped below); any request that does carry a prefix must still match
    # one of the job prefixes. Accepted on a dedicated, ephemeral data bucket.
    sid       = "ListDataBucketPrefixes"
    actions   = ["s3:ListBucket"]
    resources = [var.data_bucket_arn]

    condition {
      test     = "StringLikeIfExists"
      variable = "s3:prefix"
      values   = concat(local.job_prefixes, [for p in local.job_prefixes : "${p}/*"])
    }
  }

  statement {
    sid       = "BucketLocation"
    actions   = ["s3:GetBucketLocation"]
    resources = [var.data_bucket_arn]
  }

  # s3:DeleteObject covers Delta housekeeping (expired _delta_log files, temporary objects) and
  # the Bronze/DLQ prefixes too. "Bronze is append-only" is about data rows: it is enforced by
  # code, tests and audit, not by IAM (D-103).
  statement {
    sid = "ReadWriteDataPrefixes"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
    ]
    resources = [for p in var.read_write_prefixes : "${var.data_bucket_arn}/${p}/*"]
  }

  statement {
    sid       = "ReadOnlyPrefixes"
    actions   = ["s3:GetObject"]
    resources = [for p in var.read_only_prefixes : "${var.data_bucket_arn}/${p}/*"]
  }

  statement {
    sid = "GlueCatalogTables"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:GetUserDefinedFunction",
      "glue:GetUserDefinedFunctions",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
      "glue:UpdatePartition",
    ]
    resources = concat(
      [local.glue_catalog_arn],
      local.glue_database_arns,
      local.glue_table_arns,
      local.glue_function_arns,
    )
  }

  statement {
    sid     = "GlueLogs"
    actions = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [
      "${local.logs_arn_prefix}:/aws-glue/*",
      "${local.logs_arn_prefix}:/aws-glue/*:log-stream:*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "glue-job-least-privilege"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job.json
}

# ---------------------------------------------------------------------------------------------
# 2. Tenant demo role: one brand, landing prefix, read-only
# ---------------------------------------------------------------------------------------------

data "aws_iam_policy_document" "tenant_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [var.owner_principal_arn]
    }
  }
}

resource "aws_iam_role" "tenant_brand_alpha" {
  name                 = "${var.name_prefix}-tenant-brand-alpha"
  description          = "Demo of prefix-scoped tenant access: read landing/brand_alpha/* only"
  assume_role_policy   = data.aws_iam_policy_document.tenant_trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "tenant_brand_alpha" {
  statement {
    sid       = "ListOwnPrefix"
    actions   = ["s3:ListBucket"]
    resources = [var.data_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["landing/brand_alpha/*"]
    }
  }

  statement {
    sid       = "ReadOwnPrefix"
    actions   = ["s3:GetObject"]
    resources = ["${var.data_bucket_arn}/landing/brand_alpha/*"]
  }
}

resource "aws_iam_role_policy" "tenant_brand_alpha" {
  name   = "tenant-brand-alpha-read-landing"
  role   = aws_iam_role.tenant_brand_alpha.id
  policy = data.aws_iam_policy_document.tenant_brand_alpha.json
}

# ---------------------------------------------------------------------------------------------
# 3. GitHub OIDC provider and deploy role
# ---------------------------------------------------------------------------------------------

# thumbprint_list is optional in AWS provider 6.x: IAM validates the GitHub certificate chain
# against its trusted CA store, so no thumbprint is configured.
resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 1 : 0

  url            = "https://${local.github_oidc_url}"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 0 : 1

  url = "https://${local.github_oidc_url}"
}

data "aws_iam_policy_document" "github_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.github_oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.github_oidc_url}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.github_oidc_url}:sub"
      values   = ["repo:${var.github_repo}:environment:${var.github_environment}"]
    }
  }
}

resource "aws_iam_role" "github_deploy" {
  name                 = "${var.name_prefix}-github-deploy"
  description          = "GitHub Actions deploy role (artifacts upload, start Glue jobs)"
  assume_role_policy   = data.aws_iam_policy_document.github_trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "github_deploy" {
  statement {
    sid       = "ListArtifacts"
    actions   = ["s3:ListBucket"]
    resources = [var.data_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["artifacts/*"]
    }
  }

  statement {
    sid       = "ReadWriteArtifacts"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${var.data_bucket_arn}/artifacts/*"]
  }

  statement {
    sid       = "RunGlueJobs"
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"]
    resources = ["arn:aws:glue:${var.region}:${var.account_id}:job/${var.name_prefix}-*"]
  }
}

resource "aws_iam_role_policy" "github_deploy" {
  name   = "github-deploy-artifacts-and-glue-runs"
  role   = aws_iam_role.github_deploy.id
  policy = data.aws_iam_policy_document.github_deploy.json
}
