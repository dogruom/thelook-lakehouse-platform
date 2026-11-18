# Offline checks of the storage module (D-102). The AWS provider is mocked, so this runs in CI
# without credentials. It asserts plan-time configuration only and does NOT replace a real
# `terraform plan` against AWS (owner, before the first apply).

mock_provider "aws" {}

# Computed attributes that other resources and the policy document derive from.
override_resource {
  target          = aws_s3_bucket.data
  override_during = plan
  values          = { id = "crm-lakehouse-dev-123456789012", arn = "arn:aws:s3:::crm-lakehouse-dev-123456789012" }
}

variables {
  bucket_name = "crm-lakehouse-dev-123456789012"
}

run "bucket_defaults" {
  command = plan

  assert {
    condition     = aws_s3_bucket.data.bucket == "crm-lakehouse-dev-123456789012"
    error_message = "Bucket name must come from var.bucket_name."
  }

  assert {
    condition     = aws_s3_bucket.data.force_destroy == true
    error_message = "D-102/D-103: the ephemeral dev bucket must be force-destroyable."
  }

  assert {
    condition     = one(aws_s3_bucket_ownership_controls.data.rule).object_ownership == "BucketOwnerEnforced"
    error_message = "ACLs must be disabled (BucketOwnerEnforced)."
  }
}

run "force_destroy_is_wired" {
  command = plan

  variables {
    force_destroy = false
  }

  assert {
    condition     = aws_s3_bucket.data.force_destroy == false
    error_message = "var.force_destroy must reach the bucket."
  }
}

run "encryption_sse_s3" {
  command = plan

  assert {
    condition     = toset(flatten([for r in aws_s3_bucket_server_side_encryption_configuration.data.rule : [for d in r.apply_server_side_encryption_by_default : d.sse_algorithm]])) == toset(["AES256"])
    error_message = "Default encryption must be SSE-S3 (AES256); customer-managed KMS is out of scope (ADR-0008)."
  }
}

run "public_access_fully_blocked" {
  command = plan

  assert {
    condition     = aws_s3_bucket_public_access_block.data.block_public_acls == true
    error_message = "block_public_acls must be true."
  }

  assert {
    condition     = aws_s3_bucket_public_access_block.data.block_public_policy == true
    error_message = "block_public_policy must be true."
  }

  assert {
    condition     = aws_s3_bucket_public_access_block.data.ignore_public_acls == true
    error_message = "ignore_public_acls must be true."
  }

  assert {
    condition     = aws_s3_bucket_public_access_block.data.restrict_public_buckets == true
    error_message = "restrict_public_buckets must be true."
  }
}

# The rendered JSON of the policy document is mocked, so the statement inputs are asserted.
run "tls_only_policy" {
  command = plan

  assert {
    condition     = length(data.aws_iam_policy_document.tls_only.statement) == 1
    error_message = "The bucket policy must contain exactly the TLS-only deny statement."
  }

  assert {
    condition     = toset([for s in data.aws_iam_policy_document.tls_only.statement : s.effect]) == toset(["Deny"])
    error_message = "The TLS-only statement must be a Deny."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.tls_only.statement : s.actions])) == toset(["s3:*"])
    error_message = "The TLS-only statement must cover s3:*."
  }

  assert {
    condition     = toset(flatten([for s in data.aws_iam_policy_document.tls_only.statement : s.resources])) == toset(["arn:aws:s3:::crm-lakehouse-dev-123456789012", "arn:aws:s3:::crm-lakehouse-dev-123456789012/*"])
    error_message = "The deny must cover the bucket and all its objects."
  }

  assert {
    condition     = anytrue(flatten([for s in data.aws_iam_policy_document.tls_only.statement : [for c in s.condition : c.test == "Bool" && c.variable == "aws:SecureTransport" && toset(c.values) == toset(["false"])]]))
    error_message = "The deny must trigger on aws:SecureTransport = false."
  }

  assert {
    condition     = anytrue(flatten([for s in data.aws_iam_policy_document.tls_only.statement : [for p in s.principals : p.type == "*" && toset(p.identifiers) == toset(["*"])]]))
    error_message = "The deny must apply to every principal."
  }
}

run "lifecycle_rules" {
  command = plan

  assert {
    condition     = toset([for r in aws_s3_bucket_lifecycle_configuration.data.rule : r.id]) == toset(["expire-landing", "expire-checkpoints", "expire-glue-temp", "expire-athena-results", "abort-incomplete-multipart"])
    error_message = "Unexpected lifecycle rule set."
  }

  assert {
    condition     = alltrue([for r in aws_s3_bucket_lifecycle_configuration.data.rule : r.status == "Enabled"])
    error_message = "All lifecycle rules must be enabled."
  }

  assert {
    condition     = { for r in aws_s3_bucket_lifecycle_configuration.data.rule : r.id => one(r.expiration).days if length(r.expiration) > 0 }["expire-athena-results"] == 7
    error_message = "Athena results must expire after 7 days (D-104, ADR-0008)."
  }

  assert {
    condition     = alltrue([for k in ["expire-landing", "expire-checkpoints", "expire-glue-temp"] : { for r in aws_s3_bucket_lifecycle_configuration.data.rule : r.id => one(r.expiration).days if length(r.expiration) > 0 }[k] == 14])
    error_message = "Landing, checkpoints and glue-temp must expire after 14 days (ADR-0008)."
  }

  assert {
    condition     = alltrue([for r in aws_s3_bucket_lifecycle_configuration.data.rule : one(r.abort_incomplete_multipart_upload).days_after_initiation == 1 if r.id == "abort-incomplete-multipart"])
    error_message = "Incomplete multipart uploads must be aborted after 1 day."
  }
}

run "expiration_days_are_wired" {
  command = plan

  variables {
    expiration_days = { "landing" = 3 }
  }

  assert {
    condition     = toset([for r in aws_s3_bucket_lifecycle_configuration.data.rule : r.id]) == toset(["expire-landing", "abort-incomplete-multipart"])
    error_message = "var.expiration_days must drive the expire-<prefix> rules."
  }
}
