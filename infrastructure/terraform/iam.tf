# -----------------------------------------------------------------------------
# Data sources
# -----------------------------------------------------------------------------

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# -----------------------------------------------------------------------------
# Lambda Execution Role: quito-lambda-role
# -----------------------------------------------------------------------------

resource "aws_iam_role" "lambda_role" {
  name = "quito-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_policy" "lambda_s3_policy" {
  name        = "${var.project_name}-lambda-s3-policy"
  description = "Allow Lambda to read/write the raw S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ]
        Resource = "arn:aws:s3:::quito-transport-raw/*"
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_secretsmanager_policy" {
  name        = "${var.project_name}-lambda-secretsmanager-policy"
  description = "Allow Lambda to read the OpenWeather API key from Secrets Manager"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "secretsmanager:GetSecretValue"
        Resource = "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:quito-transport/openweather-api-key*"
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

resource "aws_iam_role_policy_attachment" "lambda_s3_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_secretsmanager_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_secretsmanager_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cloudwatch_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_cloudwatch_policy.arn
}

# -----------------------------------------------------------------------------
# Glue Execution Role: quito-glue-role
# -----------------------------------------------------------------------------

resource "aws_iam_role" "glue_role" {
  name = "quito-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "glue.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_policy" "glue_s3_policy" {
  name        = "${var.project_name}-glue-s3-policy"
  description = "Allow Glue to read raw/scripts buckets and read/write processed bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = "arn:aws:s3:::quito-transport-raw/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ]
        Resource = "arn:aws:s3:::quito-transport-processed/*"
      },
      {
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = "arn:aws:s3:::quito-transport-scripts/*"
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
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_datacatalog_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_datacatalog_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_cloudwatch_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_cloudwatch_policy.arn
}

# -----------------------------------------------------------------------------
# Airflow Execution Role: quito-airflow-role
# -----------------------------------------------------------------------------

resource "aws_iam_role" "airflow_role" {
  name = "quito-airflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "airflow.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project = var.project_name
  }
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
          "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:quito-gtfs-ingestion",
          "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:quito-weather-ingestion"
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
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/quito-clean-trips",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/quito-enrich-weather"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "airflow_s3_policy" {
  name        = "${var.project_name}-airflow-s3-policy"
  description = "Allow Airflow to access all project S3 buckets"

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
          "arn:aws:s3:::quito-transport-raw/*",
          "arn:aws:s3:::quito-transport-processed/*",
          "arn:aws:s3:::quito-transport-scripts/*",
          "arn:aws:s3:::quito-transport-quality-reports/*",
          "arn:aws:s3:::quito-transport-mwaa/*"
        ]
      },
      {
        Effect = "Allow"
        Action = "s3:ListBucket"
        Resource = [
          "arn:aws:s3:::quito-transport-raw",
          "arn:aws:s3:::quito-transport-processed",
          "arn:aws:s3:::quito-transport-scripts",
          "arn:aws:s3:::quito-transport-quality-reports",
          "arn:aws:s3:::quito-transport-mwaa"
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
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_lambda_policy.arn
}

resource "aws_iam_role_policy_attachment" "airflow_glue_attach" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_glue_policy.arn
}

resource "aws_iam_role_policy_attachment" "airflow_s3_attach" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "airflow_cloudwatch_attach" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_cloudwatch_policy.arn
}

# -----------------------------------------------------------------------------
# Redshift Load Role: quito-redshift-role
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
  description = "Allow Redshift to read from the processed S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = "arn:aws:s3:::quito-transport-processed/*"
      },
      {
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = "arn:aws:s3:::quito-transport-processed"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "redshift_s3_attach" {
  role       = aws_iam_role.redshift_role.name
  policy_arn = aws_iam_policy.redshift_s3_policy.arn
}
