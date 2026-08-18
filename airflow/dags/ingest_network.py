"""
DAG: ingest_network
Triggers the Lambda that queries the OpenStreetMap Overpass API for Quito's
transit network (routes, stops, route_stops) and lands Parquet in the S3 raw
zone as a dated snapshot.

Schedule : daily at 05:00 UTC
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
LAMBDA_FUNCTION = "quito-network-ingestion"

DEFAULT_ARGS = {
    "owner": "quito-transport",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": alert_cloudwatch,
}

with DAG(
    dag_id="ingest_network",
    description="Fetch Quito transit network from OpenStreetMap → S3 raw zone Parquet",
    schedule_interval="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "network", "osm"],
) as dag:
    LambdaInvokeFunctionOperator(
        task_id="invoke_network_lambda",
        function_name=LAMBDA_FUNCTION,
        payload=json.dumps({}),
        aws_conn_id=AWS_CONN_ID,
    )
