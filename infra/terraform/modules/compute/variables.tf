variable "name_prefix" {
  description = "Job name prefix; jobs are named <prefix>-<job> (underscores kept)."
  type        = string
}

variable "glue_jobs" {
  description = "Pipeline job names, one Glue job each."
  type        = set(string)
}

variable "role_arn" {
  description = "ARN of the Glue job execution role."
  type        = string
}

variable "data_bucket" {
  description = "Name of the data bucket (artifacts, temp dir, lakehouse_bucket argument)."
  type        = string
}

variable "number_of_workers" {
  description = "Glue workers per job run (G.1X, 1 DPU each)."
  type        = number
  default     = 2
}

variable "timeout_minutes" {
  description = "Hard run timeout in minutes (ADR-0008 cost guardrail)."
  type        = number
  default     = 30
}
