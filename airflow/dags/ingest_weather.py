"""
DAG: ingest_weather
Triggers the Lambda that pulls current + 5-day forecast weather from
the OpenWeather API for Quito's coordinates and lands Parquet in the S3
raw zone.

Schedule : daily at 06:00 UTC (after GTFS ingestion)
Retries  : 2 × 5-min backoff
Alerting : CloudWatch TaskFailure metric on any failure
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)

from callbacks import alert_cloudwatch

AWS_CONN_ID = "aws_default"
LAMBDA_FUNCTION = "quito-weather-ingestion"

DEFAULT_ARGS = {
    "owner": "quito-transport",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": alert_cloudwatch,
}

with DAG(
    dag_id="ingest_weather",
    description="Fetch Quito weather (current + forecast) → S3 raw zone Parquet",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "weather"],
) as dag:
    LambdaInvokeFunctionOperator(
        task_id="invoke_weather_lambda",
        function_name=LAMBDA_FUNCTION,
        payload=json.dumps({}),
        aws_conn_id=AWS_CONN_ID,
    )
