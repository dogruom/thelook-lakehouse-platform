variable "bucket_name" {
  description = "Globally unique name of the data bucket (crm-lakehouse-dev-<account_id>)."
  type        = string
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete a non-empty bucket (dev is ephemeral)."
  type        = bool
  default     = true
}

variable "expiration_days" {
  description = "Object expiration in days per top-level prefix (ADR-0008: 14 d, Athena results 7 d)."
  type        = map(number)
  default = {
    "landing"        = 14
    "checkpoints"    = 14
    "glue-temp"      = 14
    "athena-results" = 7
  }
}
