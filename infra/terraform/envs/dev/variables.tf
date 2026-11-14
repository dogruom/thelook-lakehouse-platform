variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-central-1"
}

variable "owner_principal_arn" {
  description = "IAM principal of the owner; allowed to assume the tenant demo role."
  type        = string

  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:", var.owner_principal_arn))
    error_message = "owner_principal_arn must be an IAM ARN such as arn:aws:iam::123456789012:user/name."
  }
}

variable "github_repo" {
  description = "GitHub repository (owner/name) trusted by the deploy role."
  type        = string
  default     = "dogruom/thelook-lakehouse-platform"
}

variable "github_environment" {
  description = "GitHub environment whose workflows may assume the deploy role (D-105)."
  type        = string
  default     = "aws-dev"
}

variable "create_github_oidc_provider" {
  description = "Create the GitHub OIDC provider; set false if the account already has one."
  type        = bool
  default     = true
}

variable "manage_glue_log_groups" {
  description = "Create the /aws-glue/jobs/* CloudWatch log groups; false if they already exist."
  type        = bool
  default     = true
}

variable "force_destroy_data" {
  description = "Let terraform destroy delete a non-empty data bucket (dev is ephemeral)."
  type        = bool
  default     = true
}

variable "glue_jobs" {
  description = "Pipeline job names; one Glue job crm-lakehouse-dev-<job> each."
  type        = list(string)
  default = [
    "bronze_batch",
    "dlq_gate",
    "silver_customers",
    "silver_customers_history",
    "silver_orders",
    "silver_products",
    "silver_clickstream",
    "reconcile",
    "register_tables",
    "metrics_report",
  ]
}
