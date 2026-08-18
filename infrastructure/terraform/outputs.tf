# S3 Bucket Outputs
output "main_bucket_name" {
  description = "Name of the existing S3 bucket holding raw/processed/quality-reports data"
  value       = data.aws_s3_bucket.main.bucket
}

output "scripts_bucket_name" {
  description = "Name of the S3 bucket for Glue and ETL scripts"
  value       = aws_s3_bucket.scripts.bucket
}

# Lambda Function Outputs
output "network_lambda_name" {
  description = "Name of the Lambda function for OSM network ingestion"
  value       = aws_lambda_function.network_ingestion.function_name
}

output "weather_lambda_name" {
  description = "Name of the Lambda function for weather data ingestion"
  value       = aws_lambda_function.weather_ingestion.function_name
}

# Glue Job Outputs
output "glue_clean_network_name" {
  description = "Name of the Glue job for cleaning network data"
  value       = aws_glue_job.clean_network.name
}

output "glue_enrich_weather_name" {
  description = "Name of the Glue job for enriching data with weather information"
  value       = aws_glue_job.enrich_weather.name
}

# Redshift Cluster Outputs
output "redshift_endpoint" {
  description = "Endpoint address of the Redshift cluster"
  value       = aws_redshift_cluster.main.endpoint
}

output "redshift_database" {
  description = "Name of the default database in the Redshift cluster"
  value       = aws_redshift_cluster.main.database_name
}

# Secrets Manager Outputs
output "openweather_secret_name" {
  description = "Name of the Secrets Manager secret storing the OpenWeather API key"
  value       = data.aws_secretsmanager_secret.openweather.name
}
