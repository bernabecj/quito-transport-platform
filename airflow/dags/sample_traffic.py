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
  same Lambda a second time on its own schedule, spending Mapbox quota to write
  the same S3 keys again. The DAG exists for manual re-runs and to keep the orchestration
  layer documented.

Collection window
  Sampling was enabled on 2026-08-20 for two weeks. To stop it, set
  state = "DISABLED" on aws_cloudwatch_event_rule.traffic_schedule in
  infrastructure/terraform/lambda.tf and apply, which halts invocation
  entirely. TRAFFIC_SAMPLING_ENABLED on the function is a second guard.

Why hourly
  The metrics this feeds — peak hours, worst corridors, travel-time variance —
  compare a corridor against its own history. A single snapshot cannot express
  any of them; the value is entirely in the accumulated series. Nor can it be
  backfilled: Mapbox reports current conditions only, so every unsampled slot
  is permanently lost.

  This ran 6-hourly until 2026-08-24, landing at 01/07/13/19 Quito time. Those
  four points show a 50%+ peak-to-overnight spread, so they rank corridors by
  peak spread — but four fixed times of day can never locate *when* a corridor
  slows down, however many days accumulate. Hourly resolves the daily curve at
  10% of Mapbox's free tier, and because each extra sample fills a new
  (corridor, hour) cell rather than duplicating one, it needs no more calendar
  time than 6-hourly did.

  Not finer than hourly: the loader's S3 key is partitioned to the hour, so two
  runs within one hour overwrite each other silently.

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
