# Glue Data Catalog databases and the Athena workgroup.
# dbt-athena creates its own gold_* schemas at run time (D-104); they are not managed here.

resource "aws_glue_catalog_database" "spark" {
  for_each = var.spark_databases

  name         = each.key
  description  = "Spark/Glue-owned ${each.key} layer"
  location_uri = "s3://${var.data_bucket}/${each.key}"
}

resource "aws_glue_catalog_database" "athena" {
  for_each = var.athena_databases

  name        = each.key
  description = "Athena-native Delta registrations of Silver tables for dbt-athena (D-104)"
}

resource "aws_athena_workgroup" "main" {
  name          = var.name_prefix
  description   = "CRM lakehouse dev workgroup with a per-query scan cutoff"
  force_destroy = true # dev is ephemeral; query history must not block terraform destroy

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = var.bytes_scanned_cutoff_per_query

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${var.data_bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
