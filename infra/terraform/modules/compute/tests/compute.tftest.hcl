# Offline checks of the compute module (D-103, ADR-0008 cost guardrails). Mocked AWS provider,
# no credentials. It asserts plan-time configuration only and does NOT replace a real
# `terraform plan` against AWS (owner, before the first apply).

mock_provider "aws" {}

variables {
  name_prefix = "crm-lakehouse-dev"
  glue_jobs   = ["bronze_ingest", "silver_customers"]
  role_arn    = "arn:aws:iam::123456789012:role/crm-lakehouse-dev-glue-job"
  data_bucket = "crm-lakehouse-dev-123456789012"
}

run "glue_job_guardrails" {
  command = plan

  assert {
    condition     = toset(keys(aws_glue_job.pipeline)) == toset(["bronze_ingest", "silver_customers"])
    error_message = "One Glue job per pipeline job."
  }

  assert {
    condition     = toset([for k, j in aws_glue_job.pipeline : j.name]) == toset(["crm-lakehouse-dev-bronze_ingest", "crm-lakehouse-dev-silver_customers"])
    error_message = "Glue jobs must be named <name_prefix>-<job>."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : j.glue_version == "5.0"])
    error_message = "Every job must run Glue 5.0."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : j.worker_type == "G.1X"])
    error_message = "Every job must use G.1X workers."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : j.number_of_workers == 2])
    error_message = "Every job must use 2 workers by default."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : j.timeout == 30])
    error_message = "Every job must have a 30-minute timeout."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : j.max_retries == 0])
    error_message = "Every job must have max_retries = 0."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : one(j.execution_property).max_concurrent_runs == 1])
    error_message = "Every job must have max_concurrent_runs = 1 (one writer per Delta table, D-103)."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : j.execution_class == "STANDARD"])
    error_message = "Every job must use the STANDARD execution class."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : j.role_arn == "arn:aws:iam::123456789012:role/crm-lakehouse-dev-glue-job"])
    error_message = "Every job must run with the least-privilege Glue job role."
  }
}

run "glue_job_arguments" {
  command = plan

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : j.default_arguments["--datalake-formats"] == "delta"])
    error_message = "--datalake-formats must be delta."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : j.default_arguments["--enable-glue-datacatalog"] == "true"])
    error_message = "--enable-glue-datacatalog must be set."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : strcontains(j.default_arguments["--conf"], "io.delta.sql.DeltaSparkSessionExtension") && strcontains(j.default_arguments["--conf"], "org.apache.spark.sql.delta.catalog.DeltaCatalog")])
    error_message = "--conf must enable the Delta extension and catalog."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : j.default_arguments["--lakehouse_job"] == k && j.default_arguments["--lakehouse_bucket"] == "crm-lakehouse-dev-123456789012"])
    error_message = "Each job must receive its own name and the data bucket."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : j.default_arguments["--TempDir"] == "s3://crm-lakehouse-dev-123456789012/glue-temp/"])
    error_message = "The Glue temp dir must be the glue-temp/ prefix (14-day lifecycle)."
  }

  assert {
    condition     = alltrue([for k, j in aws_glue_job.pipeline : one(j.command).name == "glueetl" && one(j.command).script_location == "s3://crm-lakehouse-dev-123456789012/artifacts/current/glue_entry.py"])
    error_message = "Jobs must run the shared glue_entry.py from artifacts/current/."
  }
}
