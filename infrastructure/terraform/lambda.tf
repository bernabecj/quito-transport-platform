# ──────────────────────────────────────────────
# Lambda: container image (pandas/pyarrow don't fit a plain zip deploy)
# ──────────────────────────────────────────────
# Image is built and pushed by infrastructure/scripts/deploy_lambda_image.sh —
# Terraform only creates the repo and references whatever tag was last pushed.

resource "aws_ecr_repository" "ingestion" {
  name                 = "${var.project_name}-ingestion"
  image_tag_mutability = "MUTABLE"
}

data "aws_ecr_image" "ingestion_latest" {
  repository_name = aws_ecr_repository.ingestion.name
  image_tag       = "latest"
}

# weather_loader.py reads the API key directly from this env var at runtime.
data "aws_secretsmanager_secret_version" "openweather" {
  secret_id = data.aws_secretsmanager_secret.openweather.id
}

# ──────────────────────────────────────────────
# Lambda Functions
# ──────────────────────────────────────────────

resource "aws_lambda_function" "network_ingestion" {
  function_name = "quito-network-ingestion"
  description   = "Ingests the OSM transit network into the S3 raw prefix"
  package_type  = "Image"
  image_uri = "${aws_ecr_repository.ingestion.repository_url}@${data.aws_ecr_image.ingestion_latest.image_digest}"
  role      = data.aws_iam_role.lambda_ingestion.arn

  # Overpass is a free shared service that frequently answers 429/500/504. The
  # loader retries across two endpoints with backoff, and this timeout must
  # exceed that worst case (~750s) or the retries get cut off mid-flight.
  # A healthy run finishes in well under a minute; Lambda bills actual duration.
  timeout     = 900
  memory_size = 512

  image_config {
    command = ["ingestion.network_loader.handler"]
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_lambda_function" "weather_ingestion" {
  function_name = "quito-weather-ingestion"
  description   = "Ingests weather data from OpenWeather API into the S3 raw prefix"
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.ingestion.repository_url}@${data.aws_ecr_image.ingestion_latest.image_digest}"
  role          = data.aws_iam_role.lambda_ingestion.arn
  timeout       = 300
  memory_size   = 512

  image_config {
    command = ["ingestion.weather_loader.handler"]
  }

  environment {
    variables = {
      OPENWEATHER_API_KEY = jsondecode(data.aws_secretsmanager_secret_version.openweather.secret_string)["OPENWEATHER_API_KEY"]
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# ──────────────────────────────────────────────
# CloudWatch Event Rules (daily at 05:00 UTC)
# ──────────────────────────────────────────────

resource "aws_cloudwatch_event_rule" "network_schedule" {
  name                = "${var.project_name}-network-daily"
  description         = "Trigger quito-network-ingestion Lambda daily at 05:00 UTC"
  schedule_expression = "cron(0 5 * * ? *)"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_cloudwatch_event_rule" "weather_schedule" {
  name                = "${var.project_name}-weather-daily"
  description         = "Trigger quito-weather-ingestion Lambda daily at 05:00 UTC"
  schedule_expression = "cron(0 5 * * ? *)"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# ──────────────────────────────────────────────
# CloudWatch Event Targets
# ──────────────────────────────────────────────

resource "aws_cloudwatch_event_target" "network_target" {
  rule      = aws_cloudwatch_event_rule.network_schedule.name
  target_id = "quito-network-ingestion-target"
  arn       = aws_lambda_function.network_ingestion.arn
}

resource "aws_cloudwatch_event_target" "weather_target" {
  rule      = aws_cloudwatch_event_rule.weather_schedule.name
  target_id = "quito-weather-ingestion-target"
  arn       = aws_lambda_function.weather_ingestion.arn
}

# ──────────────────────────────────────────────
# Lambda Permissions for EventBridge (CloudWatch Events)
# ──────────────────────────────────────────────

resource "aws_lambda_permission" "allow_eventbridge_network" {
  statement_id  = "AllowExecutionFromEventBridgeNetwork"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.network_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.network_schedule.arn
}

resource "aws_lambda_permission" "allow_eventbridge_weather" {
  statement_id  = "AllowExecutionFromEventBridgeWeather"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.weather_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.weather_schedule.arn
}
