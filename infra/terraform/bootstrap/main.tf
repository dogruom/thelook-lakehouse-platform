# Bootstrap stack (D-102): the Terraform state bucket and the AWS Budgets guardrails.
#
# Applied once by the owner with LOCAL state (the state of this stack is tiny and contains no
# secrets; migrating it into the bucket it created is optional, see README). Budgets live here
# and not in envs/dev because envs/dev is destroyed after every cloud day and the cost alarms
# must survive it (ADR-0008).

data "aws_caller_identity" "current" {}

locals {
  state_bucket_name = "crm-lakehouse-tfstate-${data.aws_caller_identity.current.account_id}"

  # ADR-0008 guardrails: ACTUAL spend at 5 / 20 / 40 USD and FORECASTED spend above 40 USD.
  budget_notifications = {
    actual_5      = { type = "ACTUAL", threshold = 5 }
    actual_20     = { type = "ACTUAL", threshold = 20 }
    actual_40     = { type = "ACTUAL", threshold = 40 }
    forecasted_40 = { type = "FORECASTED", threshold = 40 }
  }
}

# ---------------------------------------------------------------------------------------------
# State bucket
# ---------------------------------------------------------------------------------------------

resource "aws_s3_bucket" "state" {
  bucket = local.state_bucket_name

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket = aws_s3_bucket.state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

data "aws_iam_policy_document" "state_tls_only" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "state" {
  bucket = aws_s3_bucket.state.id
  policy = data.aws_iam_policy_document.state_tls_only.json

  # A bucket policy and the public-access block must not be written concurrently.
  depends_on = [aws_s3_bucket_public_access_block.state]
}

resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    id     = "expire-noncurrent-state-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 90
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Lifecycle rules on a versioned bucket require versioning to be configured first.
  depends_on = [aws_s3_bucket_versioning.state]
}

# ---------------------------------------------------------------------------------------------
# AWS Budgets (ADR-0008 cost guardrails)
# ---------------------------------------------------------------------------------------------

resource "aws_budgets_budget" "monthly" {
  name         = "crm-lakehouse-monthly"
  budget_type  = "COST"
  time_unit    = "MONTHLY"
  limit_amount = tostring(var.budget_limit_usd)
  limit_unit   = "USD"

  # Credits and refunds are excluded from the measured cost on purpose: on the Free plan credits
  # absorb every charge, so a budget that nets them out would never alarm.
  cost_types {
    include_credit = false
    include_refund = false
  }

  dynamic "notification" {
    for_each = local.budget_notifications

    content {
      comparison_operator        = "GREATER_THAN"
      notification_type          = notification.value.type
      threshold                  = notification.value.threshold
      threshold_type             = "ABSOLUTE_VALUE"
      subscriber_email_addresses = [var.budget_email]
    }
  }
}
