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

# Redshift is deliberately NOT managed here.
#
# The warehouse lives in a separate AWS account on its free tier, so creating a
# cluster in this account would duplicate it at roughly $180/month for a
# dc2.large. The pipeline reaches the external cluster through the Airflow
# `redshift_default` connection instead.
#
# aws_iam_role.redshift_role and its S3 read policy remain in iam.tf: they are
# what the external cluster assumes to COPY from this account's processed
# prefix, so they are still needed even with no local cluster.
