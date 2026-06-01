variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix for all resource names"
  type        = string
  default     = "quito-transport"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "prod"
}

variable "openweather_secret_name" {
  description = "Name of the OpenWeather API key secret in Secrets Manager"
  type        = string
  default     = "quito-transport/openweather-api-key"
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
