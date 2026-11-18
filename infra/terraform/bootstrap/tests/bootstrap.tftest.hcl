# Offline checks of the bootstrap stack (D-102, ADR-0008). Mocked AWS provider, no credentials.
# It asserts plan-time configuration only and does NOT replace a real `terraform plan` against
# AWS (owner, before the first apply).

mock_provider "aws" {}

override_data {
  target = data.aws_caller_identity.current
  values = { account_id = "123456789012" }
}

override_resource {
  target = aws_s3_bucket.state
  values = { id = "crm-lakehouse-tfstate-123456789012", arn = "arn:aws:s3:::crm-lakehouse-tfstate-123456789012" }
}

variables {
  budget_email = "owner@example.com"
}

run "state_bucket" {
  command = plan

  assert {
    condition     = aws_s3_bucket.state.bucket == "crm-lakehouse-tfstate-123456789012"
    error_message = "The state bucket must be named crm-lakehouse-tfstate-<account id>."
  }

  assert {
    condition     = one(aws_s3_bucket_versioning.state.versioning_configuration).status == "Enabled"
    error_message = "State bucket versioning must be Enabled."
  }

  assert {
    condition     = toset(flatten([for r in aws_s3_bucket_server_side_encryption_configuration.state.rule : [for d in r.apply_server_side_encryption_by_default : d.sse_algorithm]])) == toset(["AES256"])
    error_message = "State bucket default encryption must be SSE-S3 (AES256)."
  }

  assert {
    condition     = aws_s3_bucket_public_access_block.state.block_public_acls == true && aws_s3_bucket_public_access_block.state.block_public_policy == true && aws_s3_bucket_public_access_block.state.ignore_public_acls == true && aws_s3_bucket_public_access_block.state.restrict_public_buckets == true
    error_message = "All four public-access-block flags must be true on the state bucket."
  }

  assert {
    condition     = one(aws_s3_bucket_ownership_controls.state.rule).object_ownership == "BucketOwnerEnforced"
    error_message = "ACLs must be disabled on the state bucket."
  }

  assert {
    condition     = alltrue([for r in aws_s3_bucket_lifecycle_configuration.state.rule : r.status == "Enabled" && one(r.noncurrent_version_expiration).noncurrent_days == 90])
    error_message = "Noncurrent state versions must expire after 90 days."
  }
}

run "state_bucket_tls_only" {
  command = plan

  assert {
    condition     = length(data.aws_iam_policy_document.state_tls_only.statement) == 1
    error_message = "The state bucket policy must contain exactly the TLS-only deny statement."
  }

  assert {
    condition     = toset([for s in data.aws_iam_policy_document.state_tls_only.statement : s.effect]) == toset(["Deny"])
    error_message = "The TLS-only statement must be a Deny."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.state_tls_only.statement : s.resources])) == toset(["arn:aws:s3:::crm-lakehouse-tfstate-123456789012", "arn:aws:s3:::crm-lakehouse-tfstate-123456789012/*"])
    error_message = "The deny must cover the state bucket and all its objects."
  }

  assert {
    condition     = anytrue(flatten([for s in data.aws_iam_policy_document.state_tls_only.statement : [for c in s.condition : c.test == "Bool" && c.variable == "aws:SecureTransport" && toset(c.values) == toset(["false"])]]))
    error_message = "The deny must trigger on aws:SecureTransport = false."
  }
}

run "budgets" {
  command = plan

  assert {
    condition     = aws_budgets_budget.monthly.budget_type == "COST" && aws_budgets_budget.monthly.time_unit == "MONTHLY"
    error_message = "The budget must be a monthly COST budget."
  }

  assert {
    condition     = aws_budgets_budget.monthly.limit_amount == "50" && aws_budgets_budget.monthly.limit_unit == "USD"
    error_message = "The monthly limit must be USD 50 (project ceiling, ADR-0008)."
  }

  assert {
    condition     = one(aws_budgets_budget.monthly.cost_types).include_credit == false
    error_message = "include_credit must be false so Free-plan credits do not hide usage (D-102)."
  }

  assert {
    condition     = toset([for n in aws_budgets_budget.monthly.notification : "${n.notification_type}:${n.threshold}"]) == toset(["ACTUAL:5", "ACTUAL:20", "ACTUAL:40", "FORECASTED:40"])
    error_message = "Notifications must be ACTUAL 5/20/40 and FORECASTED 40."
  }

  assert {
    condition     = alltrue([for n in aws_budgets_budget.monthly.notification : n.comparison_operator == "GREATER_THAN" && n.threshold_type == "ABSOLUTE_VALUE"])
    error_message = "Thresholds are absolute USD values with GREATER_THAN."
  }

  assert {
    condition     = alltrue([for n in aws_budgets_budget.monthly.notification : toset(n.subscriber_email_addresses) == toset(["owner@example.com"])])
    error_message = "Every notification must go to var.budget_email."
  }
}
