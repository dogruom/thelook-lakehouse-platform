output "data_bucket" {
  description = "Name of the data bucket (LAKEHOUSE_AWS_BUCKET)."
  value       = module.storage.bucket_name
}

output "glue_job_role_arn" {
  description = "ARN of the Glue job execution role."
  value       = module.identity.glue_job_role_arn
}

output "github_deploy_role_arn" {
  description = "ARN of the GitHub OIDC deploy role (AWS_DEPLOY_ROLE_ARN)."
  value       = module.identity.github_deploy_role_arn
}

output "tenant_brand_alpha_role_arn" {
  description = "ARN of the tenant demo role (read landing/brand_alpha/* only)."
  value       = module.identity.tenant_brand_alpha_role_arn
}

output "athena_workgroup" {
  description = "Name of the Athena workgroup."
  value       = module.catalog.athena_workgroup
}

output "glue_job_names" {
  description = "Map of pipeline job key to Glue job name."
  value       = module.compute.job_names
}
