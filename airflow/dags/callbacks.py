"""
Shared Airflow callbacks for the Quito Transport pipeline.
"""

import logging

import boto3

logger = logging.getLogger(__name__)

CLOUDWATCH_NAMESPACE = "QuitoTransport/Airflow"


def alert_cloudwatch(context) -> None:
    """on_failure_callback that emits a TaskFailure metric to CloudWatch."""
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    try:
        boto3.client("cloudwatch", region_name="sa-east-1").put_metric_data(
            Namespace=CLOUDWATCH_NAMESPACE,
            MetricData=[
                {
                    "MetricName": "TaskFailure",
                    "Dimensions": [
                        {"Name": "dag_id", "Value": dag_id},
                        {"Name": "task_id", "Value": task_id},
                    ],
                    "Value": 1,
                    "Unit": "Count",
                }
            ],
        )
        logger.info("CloudWatch alert emitted for %s / %s", dag_id, task_id)
    except Exception:
        logger.exception("Failed to emit CloudWatch alert for %s / %s", dag_id, task_id)
