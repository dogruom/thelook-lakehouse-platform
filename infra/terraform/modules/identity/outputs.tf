output "glue_job_role_arn" {
  description = "ARN of the Glue job execution role."
  value       = aws_iam_role.glue_job.arn
}

output "tenant_brand_alpha_role_arn" {
  description = "ARN of the tenant demo role (read landing/brand_alpha/* only)."
  value       = aws_iam_role.tenant_brand_alpha.arn
}

output "github_deploy_role_arn" {
  description = "ARN of the GitHub OIDC deploy role."
  value       = aws_iam_role.github_deploy.arn
}
