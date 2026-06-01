# ---------------------------------------------------------------------------
# AWS Glue Data Catalog Database
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "main" {
  name = "${var.project_name}-catalog"
}

# ---------------------------------------------------------------------------
# AWS Glue Job: quito-clean-trips
# ---------------------------------------------------------------------------

resource "aws_glue_job" "clean_trips" {
  name              = "${var.project_name}-clean-trips"
  role_arn          = aws_iam_role.glue_exec.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue_jobs/clean_trips.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                   = "python"
    "--RAW_BUCKET"                     = aws_s3_bucket.raw.bucket
    "--PROCESSED_BUCKET"               = aws_s3_bucket.processed.bucket
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                 = "true"
  }
}

# ---------------------------------------------------------------------------
# AWS Glue Job: quito-enrich-weather
# ---------------------------------------------------------------------------

resource "aws_glue_job" "enrich_weather" {
  name              = "${var.project_name}-enrich-weather"
  role_arn          = aws_iam_role.glue_exec.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue_jobs/enrich_weather.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                   = "python"
    "--RAW_BUCKET"                     = aws_s3_bucket.raw.bucket
    "--PROCESSED_BUCKET"               = aws_s3_bucket.processed.bucket
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                 = "true"
  }
}
