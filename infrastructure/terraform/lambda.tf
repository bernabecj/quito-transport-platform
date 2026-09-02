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
      TRAFFIC_SAMPLING_ENABLED = "false"
      MAPBOX_SECRET_NAME       = data.aws_secretsmanager_secret.mapbox.name
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# Hourly. This ran 6-hourly from 2026-08-20 to 2026-08-24; that cadence proved
# the congestion signal is real (50%+ peak-to-overnight spread, separating
# cleanly per corridor) but it only ever samples four times of day — 01/07/13/19
# Quito time — so it can rank corridors by peak spread yet can never locate
# *when* a corridor slows down, which is one of the questions the platform is
# built to answer. More days do not fix that; only a narrower interval does.
#
# Raising the frequency costs no extra calendar time. Each analysis cell is
# (corridor x hour x weekday/weekend), so an extra sample per day fills a new
# hour cell rather than duplicating an existing one: days-to-n-per-cell is the
# same at 4/day and 24/day, for 6x the resolution over the same window. The
# 6-hourly samples already collected stay valid and comparable.
#
# Cost: 14 corridors x 24/day = ~10,100 requests/month, 10% of Mapbox's
# 100,000/month free tier (the 6-hourly rule used 1.7%).
#
# DO NOT go finer than hourly without changing the S3 key first. traffic_loader
# writes to .../day=DD/hour=HH/corridor_travel_time.parquet, which has no minute
# component, so two runs in the same hour overwrite each other and half the data
# is lost with no error. Sub-hourly sampling needs a minute in that key.
#
# TO STOP COLLECTING: set state = "DISABLED" and apply. That halts invocation
# entirely, which is cleaner than letting the rule fire into a Lambda that
# returns {"skipped": true}. TRAFFIC_SAMPLING_ENABLED on the function remains a
# second, independent guard.
resource "aws_cloudwatch_event_rule" "traffic_schedule" {
  name                = "${var.project_name}-traffic-hourly"
  description         = "Trigger quito-traffic-ingestion hourly during the collection window"
  schedule_expression = "cron(0 * * * ? *)"
  state               = "DISABLED"

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
