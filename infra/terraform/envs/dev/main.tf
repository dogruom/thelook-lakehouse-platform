# Ephemeral dev environment: applied before a cloud day and destroyed afterwards (D-102).
# Nothing here bills while idle; the Budgets and the state bucket live in ../../bootstrap.

data "aws_caller_identity" "current" {}

locals {
  project     = "crm-lakehouse"
  environment = "dev"
  name_prefix = "${local.project}-${local.environment}"
  account_id  = data.aws_caller_identity.current.account_id
  bucket_name = "${local.name_prefix}-${local.account_id}"
}

module "storage" {
  source = "../../modules/storage"

  bucket_name   = local.bucket_name
  force_destroy = var.force_destroy_data
}

module "catalog" {
  source = "../../modules/catalog"

  name_prefix = local.name_prefix
  data_bucket = module.storage.bucket_name
}

module "identity" {
  source = "../../modules/identity"

  name_prefix                 = local.name_prefix
  account_id                  = local.account_id
  region                      = var.region
  data_bucket_arn             = module.storage.bucket_arn
  glue_database_names         = module.catalog.database_names
  owner_principal_arn         = var.owner_principal_arn
  github_repo                 = var.github_repo
  github_environment          = var.github_environment
  create_github_oidc_provider = var.create_github_oidc_provider
  manage_glue_log_groups      = var.manage_glue_log_groups
}

module "compute" {
  source = "../../modules/compute"

  name_prefix = local.name_prefix
  glue_jobs   = toset(var.glue_jobs)
  role_arn    = module.identity.glue_job_role_arn
  data_bucket = module.storage.bucket_name
}
