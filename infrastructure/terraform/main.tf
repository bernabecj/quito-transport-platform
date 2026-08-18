terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment after creating the state bucket manually once:
  # backend "s3" {
  #   bucket = "quito-transport-terraform-state"
  #   key    = "state/terraform.tfstate"
  #   region = "sa-east-1"
  # }
}

provider "aws" {
  region = var.aws_region
}

# quito-transport-platform was created manually (raw/, processed/, quality-reports/
# prefixes) — referenced here, not managed by Terraform.
data "aws_s3_bucket" "main" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket" "scripts" {
  bucket = "${var.project_name}-scripts"
}

# quito/openweather was created manually — referenced here, not managed by Terraform.
data "aws_secretsmanager_secret" "openweather" {
  name = "quito/openweather"
}

resource "aws_redshift_cluster" "main" {
  cluster_identifier  = "${var.project_name}-warehouse"
  database_name       = "transport"
  master_username     = var.redshift_master_username
  master_password     = var.redshift_master_password
  node_type           = "dc2.large"
  cluster_type        = "single-node"
  skip_final_snapshot = true
  iam_roles           = [aws_iam_role.redshift_role.arn]
}
