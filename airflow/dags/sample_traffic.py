"""
DAG: sample_traffic
Samples observed travel time on Quito's trunk BRT corridors via the Mapbox
Directions API, landing one Parquet snapshot per run in the S3 raw zone.

KEEP THIS DAG PAUSED — EventBridge owns the schedule
  Collection runs from an hourly EventBridge rule in AWS
  (quito-transport-traffic-hourly), not from here. This local Airflow only runs
  while the dev container is up, and a two-week series with overnight gaps is
  not a series. EventBridge runs regardless of whether this machine is on.

  Unpausing this DAG would therefore not add coverage — it would invoke the
  same Lambda a second time each hour, doubling Mapbox spend to write the same
  S3 key twice. The DAG exists for manual re-runs and to keep the orchestration
  layer documented.

Collection window
  Sampling was enabled on 2026-08-20 for two weeks. To stop it, set
  TRAFFIC_SAMPLING_ENABLED to "false" on the Lambda in
  infrastructure/terraform/lambda.tf and apply; the handler then returns
  {"skipped": true} without spending a request.

Why hourly
  The metrics this feeds — peak hours, worst corridors, travel-time variance —
  compare a corridor against its own history. A single snapshot cannot express
  any of them; the value is entirely in the accumulated series. Nor can it be
  backfilled: Mapbox reports current conditions only, so every unsampled hour
  is permanently lost.

Schedule : hourly (inert while paused)
Retries  : 1 x 5-min backoff
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
LAMBDA_FUNCTION = "quito-traffic-ingestion"

DEFAULT_ARGS = {
    "owner": "quito-transport",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": alert_cloudwatch,
}

with DAG(
    dag_id="sample_traffic",
    description="Sample Quito BRT corridor travel time from Mapbox → S3 raw zone",
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # Costs API quota, so it must be unpaused deliberately rather than start
    # sampling the moment it is deployed.
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "traffic", "mapbox", "disabled-by-default"],
) as dag:
    LambdaInvokeFunctionOperator(
        task_id="invoke_traffic_lambda",
        function_name=LAMBDA_FUNCTION,
        payload=json.dumps({}),
        aws_conn_id=AWS_CONN_ID,
    )
