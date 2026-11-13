# One Glue 5.0 Spark job per pipeline job (D-103). All jobs run the same entry script and wheel
# from s3://<bucket>/artifacts/current/; scripts/cloud/run_job.py overrides the wheel per run.
# Jobs cost nothing while idle: Glue bills only DPU-seconds of a run.

locals {
  artifacts_uri = "s3://${var.data_bucket}/artifacts/current"

  # Glue accepts a single --conf key; further settings are chained inside its value.
  spark_conf = join(" --conf ", [
    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
  ])
}

resource "aws_glue_job" "pipeline" {
  for_each = var.glue_jobs

  name              = "${var.name_prefix}-${each.key}"
  description       = "CRM lakehouse job ${each.key}"
  role_arn          = var.role_arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = var.number_of_workers
  timeout           = var.timeout_minutes
  max_retries       = 0
  execution_class   = "STANDARD"

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    script_location = "${local.artifacts_uri}/glue_entry.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"              = "python"
    "--datalake-formats"          = "delta"
    "--enable-glue-datacatalog"   = "true"
    "--conf"                      = local.spark_conf
    "--TempDir"                   = "s3://${var.data_bucket}/glue-temp/"
    "--additional-python-modules" = "${local.artifacts_uri}/multitenant_crm_lakehouse-0.1.0-py3-none-any.whl"
    "--lakehouse_job"             = each.key
    "--lakehouse_bucket"          = var.data_bucket
  }
}
