terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Partial S3 backend (D-102): the bucket name contains the account id and is passed at init time
  #   terraform init -backend-config=backend.hcl      (see backend.hcl.example)
  # Native S3 locking (Terraform >= 1.10) replaces DynamoDB.
  backend "s3" {
    key          = "dev/terraform.tfstate"
    region       = "eu-central-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project     = "crm-lakehouse"
      environment = "dev"
      managed_by  = "terraform"
    }
  }
}
