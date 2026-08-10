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

variable "redshift_master_password" {
  description = "Master password for Redshift cluster"
  type        = string
  sensitive   = true
}

variable "redshift_master_username" {
  description = "Master username for Redshift cluster"
  type        = string
  default     = "admin"
}
