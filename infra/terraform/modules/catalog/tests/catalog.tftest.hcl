# Offline checks of the catalog module (D-102, D-104, ADR-0008). Mocked AWS provider, no
# credentials. It asserts plan-time configuration only and does NOT replace a real
# `terraform plan` against AWS (owner, before the first apply).

mock_provider "aws" {}

variables {
  name_prefix = "crm-lakehouse-dev"
  data_bucket = "crm-lakehouse-dev-123456789012"
}

run "glue_databases" {
  command = plan

  assert {
    condition     = toset(keys(aws_glue_catalog_database.spark)) == toset(["bronze", "dead_letter", "silver", "ops"])
    error_message = "Spark-owned databases must be bronze, dead_letter, silver and ops (gold_* is created by dbt-athena, D-104)."
  }

  assert {
    condition     = alltrue([for k, db in aws_glue_catalog_database.spark : db.location_uri == "s3://crm-lakehouse-dev-123456789012/${k}"])
    error_message = "Each Spark database must point at s3://<bucket>/<name>."
  }

  assert {
    condition     = toset(keys(aws_glue_catalog_database.athena)) == toset(["athena_silver"])
    error_message = "The Athena-native Silver registration database must be athena_silver (D-104)."
  }

  # location_uri of athena_silver is not set in config; it is provider-computed, so it is unknown
  # at plan time and cannot be asserted offline.
}

run "athena_workgroup_guardrails" {
  command = plan

  assert {
    condition     = aws_athena_workgroup.main.name == "crm-lakehouse-dev"
    error_message = "The workgroup is named exactly like the name prefix."
  }

  assert {
    condition     = one(aws_athena_workgroup.main.configuration).bytes_scanned_cutoff_per_query == 1073741824
    error_message = "The per-query scan cutoff must default to 1 GiB (ADR-0008)."
  }

  assert {
    condition     = one(aws_athena_workgroup.main.configuration).enforce_workgroup_configuration == true
    error_message = "The workgroup configuration must be enforced so clients cannot override the cutoff."
  }

  assert {
    condition     = one(aws_athena_workgroup.main.configuration).publish_cloudwatch_metrics_enabled == false
    error_message = "CloudWatch metrics are off to avoid custom-metric cost."
  }

  assert {
    condition     = one(one(aws_athena_workgroup.main.configuration).result_configuration).output_location == "s3://crm-lakehouse-dev-123456789012/athena-results/"
    error_message = "Query results must go to the athena-results/ prefix (7-day lifecycle)."
  }

  assert {
    condition     = one(one(one(aws_athena_workgroup.main.configuration).result_configuration).encryption_configuration).encryption_option == "SSE_S3"
    error_message = "Query results must be encrypted with SSE_S3."
  }
}

run "cutoff_is_wired" {
  command = plan

  variables {
    bytes_scanned_cutoff_per_query = 536870912
  }

  assert {
    condition     = one(aws_athena_workgroup.main.configuration).bytes_scanned_cutoff_per_query == 536870912
    error_message = "var.bytes_scanned_cutoff_per_query must reach the workgroup."
  }
}
