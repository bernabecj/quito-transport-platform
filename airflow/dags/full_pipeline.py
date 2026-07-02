"""
DAG: full_pipeline
End-to-end daily orchestration for the Quito Transport platform.

Task graph
──────────
  ingest_gtfs ──► validate_gtfs ──► clean_trips ──┐
                                                    ├──► enrich_weather ──► load_to_redshift ──► dbt_run ──► dbt_test
  ingest_weather ──► validate_weather ─────────────┘

Stages
  INGEST    : Lambda functions pull raw data to S3 raw zone
  VALIDATE  : Great Expectations gates — DAG halts if quality fails
  TRANSFORM : AWS Glue PySpark jobs clean and enrich data to S3 processed zone
  LOAD      : S3 → Redshift staging tables; dbt models build fact/dim layer

Schedule : daily at 07:00 UTC
Retries  : 1 × 10-min backoff per task (ingestion DAGs already ran at 05/06 UTC)
Alerting : CloudWatch TaskFailure metric on any failure
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from callbacks import alert_cloudwatch


def validate_gtfs(**context) -> None:
    """Placeholder — implement with Great Expectations in the quality step."""
    raise NotImplementedError("validate_gtfs: Great Expectations suite not yet wired up")


def validate_weather(**context) -> None:
    """Placeholder — implement with Great Expectations in the quality step."""
    raise NotImplementedError("validate_weather: Great Expectations suite not yet wired up")

# ── connection / resource identifiers ────────────────────────────────────────
AWS_CONN_ID = "aws_default"
REDSHIFT_CONN_ID = "redshift_default"

RAW_BUCKET = "quito-transport-raw"
PROCESSED_BUCKET = "quito-transport-processed"
SCRIPTS_BUCKET = "quito-transport-scripts"

GLUE_ROLE = "AWSGlueServiceRole-quito-transport"

DBT_PROJECT_DIR = "/usr/local/airflow/dbt"
DBT_PROFILES_DIR = "/usr/local/airflow/dbt"

DEFAULT_ARGS = {
    "owner": "quito-transport",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "on_failure_callback": alert_cloudwatch,
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="full_pipeline",
    description="Quito Transport: ingest → validate → transform → load (daily)",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pipeline", "gtfs", "weather"],
) as dag:

    # ── INGEST ────────────────────────────────────────────────────────────────
    ingest_gtfs = LambdaInvokeFunctionOperator(
        task_id="ingest_gtfs",
        function_name="quito-gtfs-ingestion",
        payload=json.dumps({}),
        aws_conn_id=AWS_CONN_ID,
    )

    ingest_weather = LambdaInvokeFunctionOperator(
        task_id="ingest_weather",
        function_name="quito-weather-ingestion",
        payload=json.dumps({}),
        aws_conn_id=AWS_CONN_ID,
    )

    # ── VALIDATE ──────────────────────────────────────────────────────────────
    validate_gtfs_task = PythonOperator(
        task_id="validate_gtfs",
        python_callable=validate_gtfs,
    )

    validate_weather_task = PythonOperator(
        task_id="validate_weather",
        python_callable=validate_weather,
    )

    # ── TRANSFORM ─────────────────────────────────────────────────────────────
    clean_trips = GlueJobOperator(
        task_id="clean_trips",
        job_name="quito-transport-clean-trips",
        script_location=f"s3://{SCRIPTS_BUCKET}/glue_jobs/clean_trips.py",
        aws_conn_id=AWS_CONN_ID,
        iam_role_name=GLUE_ROLE,
        create_job_kwargs={
            "GlueVersion": "4.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
        },
        script_args={
            "--execution_date": "{{ ds }}",
            "--raw_bucket": RAW_BUCKET,
            "--processed_bucket": PROCESSED_BUCKET,
        },
        wait_for_completion=True,
    )

    enrich_weather = GlueJobOperator(
        task_id="enrich_weather",
        job_name="quito-transport-enrich-weather",
        script_location=f"s3://{SCRIPTS_BUCKET}/glue_jobs/enrich_weather.py",
        aws_conn_id=AWS_CONN_ID,
        iam_role_name=GLUE_ROLE,
        create_job_kwargs={
            "GlueVersion": "4.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
        },
        script_args={
            "--execution_date": "{{ ds }}",
            "--processed_bucket": PROCESSED_BUCKET,
        },
        wait_for_completion=True,
    )

    # ── LOAD ──────────────────────────────────────────────────────────────────
    load_trips_to_redshift = S3ToRedshiftOperator(
        task_id="load_trips_to_redshift",
        schema="staging",
        table="stg_trips",
        s3_bucket=PROCESSED_BUCKET,
        s3_key="trips/{{ ds_nodash }}/",
        copy_options=["FORMAT AS PARQUET"],
        aws_conn_id=AWS_CONN_ID,
        redshift_conn_id=REDSHIFT_CONN_ID,
    )

    load_weather_to_redshift = S3ToRedshiftOperator(
        task_id="load_weather_to_redshift",
        schema="staging",
        table="stg_weather",
        s3_bucket=PROCESSED_BUCKET,
        s3_key="weather/{{ ds_nodash }}/",
        copy_options=["FORMAT AS PARQUET"],
        aws_conn_id=AWS_CONN_ID,
        redshift_conn_id=REDSHIFT_CONN_ID,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            "--vars '{\"execution_date\": \"{{ ds }}\"}'"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # ── task dependencies ─────────────────────────────────────────────────────
    ingest_gtfs >> validate_gtfs_task >> clean_trips
    ingest_weather >> validate_weather_task

    [clean_trips, validate_weather_task] >> enrich_weather

    enrich_weather >> [load_trips_to_redshift, load_weather_to_redshift]

    [load_trips_to_redshift, load_weather_to_redshift] >> dbt_run >> dbt_test
