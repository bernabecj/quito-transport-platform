# -----------------------------------------------------------------------------
# Data sources
# -----------------------------------------------------------------------------

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# -----------------------------------------------------------------------------
# Lambda Execution Role: lambda-ingestion-role (created manually, referenced here)
# -----------------------------------------------------------------------------

data "aws_iam_role" "lambda_ingestion" {
  name = "lambda-ingestion-role"
}

resource "aws_iam_policy" "lambda_s3_policy" {
  name        = "${var.project_name}-lambda-s3-policy"
  description = "Allow Lambda to write Parquet files to the raw S3 prefix"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ]
        Resource = "${data.aws_s3_bucket.main.arn}/raw/*"
      },
      {
        # traffic_loader lists raw/network/ to find the most recent snapshot to
        # build corridors from. ListBucket is a bucket-level action, so it
        # cannot be scoped by object ARN — the prefix condition keeps it from
        # exposing anything outside the raw zone.
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = data.aws_s3_bucket.main.arn
        Condition = {
          StringLike = {
            "s3:prefix" = ["raw/*"]
          }
        }
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_cloudwatch_policy" {
  name        = "${var.project_name}-lambda-cloudwatch-policy"
  description = "Allow Lambda to write CloudWatch logs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_secrets_policy" {
  name        = "${var.project_name}-lambda-secrets-policy"
  description = "Allow Lambda to read the OpenWeather and Mapbox credentials at runtime"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = [
          data.aws_secretsmanager_secret.openweather.arn,
          data.aws_secretsmanager_secret.mapbox.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_secrets_attach" {
  role       = data.aws_iam_role.lambda_ingestion.name
  policy_arn = aws_iam_policy.lambda_secrets_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_s3_attach" {
  role       = data.aws_iam_role.lambda_ingestion.name
  policy_arn = aws_iam_policy.lambda_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cloudwatch_attach" {
  role       = data.aws_iam_role.lambda_ingestion.name
  policy_arn = aws_iam_policy.lambda_cloudwatch_policy.arn
}

# -----------------------------------------------------------------------------
# Glue Execution Role: glue-processing-role (created manually, referenced here)
# -----------------------------------------------------------------------------

data "aws_iam_role" "glue_processing" {
  name = "glue-processing-role"
}

resource "aws_iam_policy" "glue_s3_policy" {
  name        = "${var.project_name}-glue-s3-policy"
  description = "Allow Glue to read raw/scripts prefixes and read/write the processed prefix"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${data.aws_s3_bucket.main.arn}/raw/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ]
        Resource = "${data.aws_s3_bucket.main.arn}/processed/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.scripts.arn}/*"
      }
    ]
  })
}

resource "aws_iam_policy" "glue_datacatalog_policy" {
  name        = "${var.project_name}-glue-datacatalog-policy"
  description = "Allow Glue full access to the Glue Data Catalog"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "glue:*"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy" "glue_cloudwatch_policy" {
  name        = "${var.project_name}-glue-cloudwatch-policy"
  description = "Allow Glue full access to CloudWatch logs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "logs:*"
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = data.aws_iam_role.glue_processing.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_datacatalog_attach" {
  role       = data.aws_iam_role.glue_processing.name
  policy_arn = aws_iam_policy.glue_datacatalog_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_cloudwatch_attach" {
  role       = data.aws_iam_role.glue_processing.name
  policy_arn = aws_iam_policy.glue_cloudwatch_policy.arn
}

# -----------------------------------------------------------------------------
# Airflow/MWAA Execution Role: airflow-orchestration-role (created manually)
# -----------------------------------------------------------------------------

data "aws_iam_role" "airflow_orchestration" {
  name = "airflow-orchestration-role"
}

resource "aws_iam_policy" "airflow_lambda_policy" {
  name        = "${var.project_name}-airflow-lambda-policy"
  description = "Allow Airflow to invoke ingestion Lambda functions"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = [
          "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:quito-network-ingestion",
          "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:quito-weather-ingestion",
          "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:quito-traffic-ingestion"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "airflow_glue_policy" {
  name        = "${var.project_name}-airflow-glue-policy"
  description = "Allow Airflow to start and monitor Glue jobs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/quito-transport-clean-network",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/quito-transport-enrich-weather"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "airflow_s3_policy" {
  name        = "${var.project_name}-airflow-s3-policy"
  description = "Allow Airflow to access all project S3 data"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = [
          "${data.aws_s3_bucket.main.arn}/raw/*",
          "${data.aws_s3_bucket.main.arn}/processed/*",
          "${data.aws_s3_bucket.main.arn}/quality-reports/*",
          "${aws_s3_bucket.scripts.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = "s3:ListBucket"
        Resource = [
          data.aws_s3_bucket.main.arn,
          aws_s3_bucket.scripts.arn
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "airflow_cloudwatch_policy" {
  name        = "${var.project_name}-airflow-cloudwatch-policy"
  description = "Allow Airflow full access to CloudWatch logs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "logs:*"
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_lambda_attach" {
  role       = data.aws_iam_role.airflow_orchestration.name
  policy_arn = aws_iam_policy.airflow_lambda_policy.arn
}

resource "aws_iam_role_policy_attachment" "airflow_glue_attach" {
  role       = data.aws_iam_role.airflow_orchestration.name
  policy_arn = aws_iam_policy.airflow_glue_policy.arn
}

resource "aws_iam_role_policy_attachment" "airflow_s3_attach" {
  role       = data.aws_iam_role.airflow_orchestration.name
  policy_arn = aws_iam_policy.airflow_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "airflow_cloudwatch_attach" {
  role       = data.aws_iam_role.airflow_orchestration.name
  policy_arn = aws_iam_policy.airflow_cloudwatch_policy.arn
}

# -----------------------------------------------------------------------------
# Redshift Load Role: quito-redshift-role (not created yet — pending Redshift opt-in)
# -----------------------------------------------------------------------------

resource "aws_iam_role" "redshift_role" {
  name = "quito-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "redshift.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_policy" "redshift_s3_policy" {
  name        = "${var.project_name}-redshift-s3-policy"
  description = "Allow Redshift to read from the processed S3 prefix"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${data.aws_s3_bucket.main.arn}/processed/*"
      },
      {
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = data.aws_s3_bucket.main.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "redshift_s3_attach" {
  role       = aws_iam_role.redshift_role.name
  policy_arn = aws_iam_policy.redshift_s3_policy.arn
}