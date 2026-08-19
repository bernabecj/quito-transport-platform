variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "sa-east-1"
}

variable "project_name" {
  description = "Project name prefix for all resource names"
  type        = string
  default     = "quito-transport"
}

variable "bucket_name" {
  description = "Name of the existing, manually-created S3 bucket holding raw/processed/quality-reports data"
  type        = string
  default     = "quito-transport-platform"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "prod"
}

# Redshift credentials are intentionally absent: the cluster lives in a separate
# AWS account and is reached through the Airflow `redshift_default` connection,
# so this configuration never needs them. See the note in main.tf.
