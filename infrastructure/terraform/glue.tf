# ---------------------------------------------------------------------------
# AWS Glue Data Catalog Database
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "main" {
  name = "${var.project_name}-catalog"
}

# ---------------------------------------------------------------------------
# AWS Glue Job: quito-clean-network
# ---------------------------------------------------------------------------

resource "aws_glue_job" "clean_network" {
  name              = "${var.project_name}-clean-network"
  role_arn          = data.aws_iam_role.glue_processing.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue_jobs/clean_network.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--bucket"                           = data.aws_s3_bucket.main.bucket
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }
}

# ---------------------------------------------------------------------------
# AWS Glue Job: quito-enrich-weather
# ---------------------------------------------------------------------------

resource "aws_glue_job" "enrich_weather" {
  name              = "${var.project_name}-enrich-weather"
  role_arn          = data.aws_iam_role.glue_processing.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue_jobs/enrich_weather.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--bucket"                           = data.aws_s3_bucket.main.bucket
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }
}
