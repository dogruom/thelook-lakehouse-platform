variable "name_prefix" {
  description = "Resource name prefix (crm-lakehouse-dev)."
  type        = string
}

variable "account_id" {
  description = "AWS account ID (used in ARNs and the aws:SourceAccount trust condition)."
  type        = string
}

variable "region" {
  description = "AWS region used to build Glue and CloudWatch Logs ARNs."
  type        = string
}

variable "data_bucket_arn" {
  description = "ARN of the data bucket."
  type        = string
}

variable "glue_database_names" {
  description = "Glue databases the job role may use (the five databases of the catalog module)."
  type        = list(string)
}

variable "owner_principal_arn" {
  description = "IAM principal (owner) allowed to assume the tenant demo role."
  type        = string
}

variable "github_repo" {
  description = "GitHub repository (owner/name) trusted by the deploy role."
  type        = string
}

variable "github_environment" {
  description = "GitHub environment whose workflows may assume the deploy role (D-105)."
  type        = string
}

variable "create_github_oidc_provider" {
  description = "Create the GitHub OIDC provider; set false if the account already has one."
  type        = bool
  default     = true
}

variable "read_write_prefixes" {
  description = "Data-bucket prefixes the Glue job role may read, write and delete."
  type        = list(string)
  default = [
    "bronze", "silver", "gold", "dead_letter", "ops", "checkpoints", "warehouse", "glue-temp",
  ]
}

variable "read_only_prefixes" {
  description = "Data-bucket prefixes the Glue job role may only read."
  type        = list(string)
  default     = ["landing", "artifacts"]
}

variable "manage_glue_log_groups" {
  description = "Create the /aws-glue/jobs/* log groups; set false if they already exist."
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Retention of the Glue CloudWatch log groups (ADR-0008: at most 14 days)."
  type        = number
  default     = 14
}
