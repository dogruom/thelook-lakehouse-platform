variable "name_prefix" {
  description = "Resource name prefix; the Athena workgroup is named exactly like this (crm-lakehouse-dev)."
  type        = string
}

variable "data_bucket" {
  description = "Name of the data bucket that holds table locations and Athena results."
  type        = string
}

variable "bytes_scanned_cutoff_per_query" {
  description = "Athena per-query scan limit in bytes (default 1 GiB, ADR-0008 cost guardrail)."
  type        = number
  default     = 1073741824
}

variable "spark_databases" {
  description = "Spark/Glue-owned Glue databases; the LocationUri is s3://<data bucket>/<name>."
  type        = set(string)
  default     = ["bronze", "dead_letter", "silver", "ops"]
}

variable "athena_databases" {
  description = "Athena-owned databases without a LocationUri (Delta registrations for dbt-athena, D-104)."
  type        = set(string)
  default     = ["athena_silver"]
}
