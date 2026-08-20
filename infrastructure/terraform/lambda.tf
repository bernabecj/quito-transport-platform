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

# The secret's VALUE is deliberately not read here. An
# aws_secretsmanager_secret_version data source would resolve the plaintext key
# into terraform.tfstate and into every plan file — exactly how the key leaked
# once already. Only the secret's name is passed to the Lambda, which fetches
# the value itself at runtime (see ingestion/weather_loader.py).

# ──────────────────────────────────────────────
# Lambda Functions
# ──────────────────────────────────────────────

resource "aws_lambda_function" "network_ingestion" {
  function_name = "quito-network-ingestion"
  description   = "Ingests the OSM transit network into the S3 raw prefix"
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.ingestion.repository_url}@${data.aws_ecr_image.ingestion_latest.image_digest}"
  role          = data.aws_iam_role.lambda_ingestion.arn

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
      OPENWEATHER_SECRET_NAME = data.aws_secretsmanager_secret.openweather.name
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# ──────────────────────────────────────────────
# Lambda: corridor travel-time sampling
# ──────────────────────────────────────────────
# ENABLED for a fixed two-week collection window starting 2026-08-20.
# Mapbox is metered (14 corridors hourly is ~4,700 requests over 14 days,
# against a 100,000/month free tier). Once the window closes, disable by
# setting TRAFFIC_SAMPLING_ENABLED to "false" and applying — the handler then
# returns {"skipped": true} without spending a request. Leaving the function
# deployed keeps the history readable and makes re-enabling a one-word change.
#
# The token is referenced by secret NAME only. Reading its value here would put
# the plaintext token into terraform.tfstate and every plan file, which is how
# the OpenWeather key leaked. traffic_loader.py fetches it at runtime.

data "aws_secretsmanager_secret" "mapbox" {
  name = "quito/mapbox"
}

resource "aws_lambda_function" "traffic_ingestion" {
  function_name = "quito-traffic-ingestion"
  description   = "Samples BRT corridor travel time from Mapbox into the S3 raw prefix"
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.ingestion.repository_url}@${data.aws_ecr_image.ingestion_latest.image_digest}"
  role          = data.aws_iam_role.lambda_ingestion.arn
  timeout       = 300
  memory_size   = 512

  image_config {
    command = ["ingestion.traffic_loader.handler"]
  }

  environment {
    variables = {
      TRAFFIC_SAMPLING_ENABLED = "true"
      MAPBOX_SECRET_NAME       = data.aws_secretsmanager_secret.mapbox.name
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# Hourly, not daily: the whole point is resolving how travel time moves across
# the day, which a daily sample cannot show.
resource "aws_cloudwatch_event_rule" "traffic_schedule" {
  name                = "${var.project_name}-traffic-hourly"
  description         = "Trigger quito-traffic-ingestion every hour during the collection window"
  schedule_expression = "cron(0 * * * ? *)"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_cloudwatch_event_target" "traffic_target" {
  rule      = aws_cloudwatch_event_rule.traffic_schedule.name
  target_id = "quito-traffic-ingestion-target"
  arn       = aws_lambda_function.traffic_ingestion.arn
}

resource "aws_lambda_permission" "allow_eventbridge_traffic" {
  statement_id  = "AllowExecutionFromEventBridgeTraffic"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.traffic_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.traffic_schedule.arn
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
