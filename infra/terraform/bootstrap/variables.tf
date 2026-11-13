variable "region" {
  description = "AWS region for the state bucket and the budget (the budget itself is global)."
  type        = string
  default     = "eu-central-1"
}

variable "budget_email" {
  description = "E-mail address that receives the AWS Budgets notifications."
  type        = string

  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.budget_email))
    error_message = "budget_email must be a valid e-mail address."
  }
}

variable "budget_limit_usd" {
  description = "Monthly cost budget in USD (project ceiling, ADR-0008)."
  type        = number
  default     = 50
}
