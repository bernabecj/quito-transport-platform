# ──────────────────────────────────────────────
# Lambda: package ingestion scripts as ZIP files
# ──────────────────────────────────────────────

data "archive_file" "gtfs_lambda" {
  type        = "zip"
  source_file = "${path.root}/../../ingestion/gtfs_loader.py"
  output_path = "/tmp/lambda_gtfs.zip"
}

data "archive_file" "weather_lambda" {
  type        = "zip"
  source_file = "${path.root}/../../ingestion/weather_loader.py"
  output_path = "/tmp/lambda_weather.zip"
}

# ──────────────────────────────────────────────
# Lambda Functions
# ──────────────────────────────────────────────

resource "aws_lambda_function" "gtfs_ingestion" {
  function_name    = "quito-gtfs-ingestion"
  description      = "Ingests GTFS transit data into the raw S3 bucket"
  filename         = data.archive_file.gtfs_lambda.output_path
  source_code_hash = data.archive_file.gtfs_lambda.output_base64sha256
  role             = aws_iam_role.lambda_exec.arn
  handler          = "gtfs_loader.handler"
  runtime          = "python3.13"
  timeout          = 300
  memory_size      = 256

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.raw.bucket
      SECRET_NAME = var.openweather_secret_name
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_lambda_function" "weather_ingestion" {
  function_name    = "quito-weather-ingestion"
  description      = "Ingests weather data from OpenWeather API into the raw S3 bucket"
  filename         = data.archive_file.weather_lambda.output_path
  source_code_hash = data.archive_file.weather_lambda.output_base64sha256
  role             = aws_iam_role.lambda_exec.arn
  handler          = "weather_loader.handler"
  runtime          = "python3.13"
  timeout          = 300
  memory_size      = 256

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.raw.bucket
      SECRET_NAME = var.openweather_secret_name
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

resource "aws_cloudwatch_event_rule" "gtfs_schedule" {
  name                = "${var.project_name}-gtfs-daily"
  description         = "Trigger quito-gtfs-ingestion Lambda daily at 05:00 UTC"
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

resource "aws_cloudwatch_event_target" "gtfs_target" {
  rule      = aws_cloudwatch_event_rule.gtfs_schedule.name
  target_id = "quito-gtfs-ingestion-target"
  arn       = aws_lambda_function.gtfs_ingestion.arn
}

resource "aws_cloudwatch_event_target" "weather_target" {
  rule      = aws_cloudwatch_event_rule.weather_schedule.name
  target_id = "quito-weather-ingestion-target"
  arn       = aws_lambda_function.weather_ingestion.arn
}

# ──────────────────────────────────────────────
# Lambda Permissions for EventBridge (CloudWatch Events)
# ──────────────────────────────────────────────

resource "aws_lambda_permission" "allow_eventbridge_gtfs" {
  statement_id  = "AllowExecutionFromEventBridgeGTFS"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.gtfs_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.gtfs_schedule.arn
}

resource "aws_lambda_permission" "allow_eventbridge_weather" {
  statement_id  = "AllowExecutionFromEventBridgeWeather"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.weather_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.weather_schedule.arn
}
