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

resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw"
}

resource "aws_s3_bucket" "processed" {
  bucket = "${var.project_name}-processed"
}

resource "aws_s3_bucket" "scripts" {
  bucket = "${var.project_name}-scripts"
}

resource "aws_s3_bucket" "quality_reports" {
  bucket = "${var.project_name}-quality-reports"
}

resource "aws_s3_bucket" "mwaa" {
  bucket = "${var.project_name}-mwaa"
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_secretsmanager_secret" "openweather" {
  name        = var.openweather_secret_name
  description = "OpenWeather API key for Quito transport ingestion"
}

resource "aws_redshift_cluster" "main" {
  cluster_identifier  = "${var.project_name}-warehouse"
  database_name       = "transport"
  master_username     = var.redshift_master_username
  master_password     = var.redshift_master_password
  node_type           = "dc2.large"
  cluster_type        = "single-node"
  skip_final_snapshot = true
  iam_roles           = [aws_iam_role.redshift_load.arn]
}
