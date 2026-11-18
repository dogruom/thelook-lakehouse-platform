output "state_bucket" {
  description = "Name of the Terraform state bucket (use it in envs/dev/backend.hcl)."
  value       = aws_s3_bucket.state.id
}

output "budget_name" {
  description = "Name of the monthly AWS Budget."
  value       = aws_budgets_budget.monthly.name
}
