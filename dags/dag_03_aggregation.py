"""
DAG 3 – data_aggregation_dag
────────────────────────────────────────────────────────────────────────────────
Reads the processed zone and produces three aggregated datasets:

  s3a://data-pipeline/aggregated/
      daily_summary/
          year=YYYY/month=MM/day=DD/   ← revenue / volume per category per day
      category_ranking/
          year=YYYY/month=MM/          ← top-N categories per month
      hourly_trends/
          year=YYYY/month=MM/day=DD/   ← hourly order volume + avg value

Depends on DAG 02 (ExternalTaskSensor).
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET         = os.getenv("MINIO_BUCKET", "data-pipeline")

SPARK_S3A_CONF = {
    "spark.hadoop.fs.s3a.endpoint":            MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key":          MINIO_ACCESS,
    "spark.hadoop.fs.s3a.secret.key":          MINIO_SECRET,
    "spark.hadoop.fs.s3a.path.style.access":   "true",
    "spark.hadoop.fs.s3a.impl":                "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.dynamicAllocation.enabled":                  "true",
    "spark.dynamicAllocation.minExecutors":             "0",
    "spark.dynamicAllocation.maxExecutors":             "4",
    "spark.dynamicAllocation.shuffleTracking.enabled":  "true",
    "spark.sql.adaptive.enabled":              "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    # Broadcast joins for small dimension tables
    "spark.sql.autoBroadcastJoinThreshold":    "10485760",   # 10 MB
}

SPARK_JARS_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def _log_agg_start(**context):
    exec_date = context["logical_date"]
    print(
        f"[Aggregation DAG] Starting for logical_date={exec_date.date()}\n"
        f"  Source : s3a://{BUCKET}/processed/...\n"
        f"  Target : s3a://{BUCKET}/aggregated/..."
    )


def _log_agg_complete(**context):
    print("[Aggregation DAG] All aggregations written successfully.")


with DAG(
    dag_id="03_data_aggregation",
    description="Aggregate processed MinIO data – daily summary, category ranking, hourly trends",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["aggregation", "spark", "minio"],
) as dag:

    wait_for_processing = ExternalTaskSensor(
        task_id="wait_for_processing",
        external_dag_id="02_data_processing",
        external_task_id="process_raw_to_processed",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        timeout=3600,
        poke_interval=30,
        mode="poke",
    )

    log_start = PythonOperator(
        task_id="log_aggregation_start",
        python_callable=_log_agg_start,
    )

    # Task A – daily revenue & volume summary
    daily_summary = SparkSubmitOperator(
        task_id="aggregate_daily_summary",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/03_aggregate.py",
        name="AggDailySummary-{{ ds_nodash }}",
        deploy_mode="client",
        conf=SPARK_S3A_CONF,
        packages=SPARK_JARS_PACKAGES,
        application_args=[
            "--bucket",    BUCKET,
            "--date",      "{{ ds }}",
            "--agg-type",  "daily_summary",
        ],
        verbose=True,
        driver_memory="512m",
        executor_memory="1g",
        executor_cores=1,
        num_executors=2,
    )

    # Task B – category ranking (monthly)
    category_ranking = SparkSubmitOperator(
        task_id="aggregate_category_ranking",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/03_aggregate.py",
        name="AggCategoryRanking-{{ ds_nodash }}",
        deploy_mode="client",
        conf=SPARK_S3A_CONF,
        packages=SPARK_JARS_PACKAGES,
        application_args=[
            "--bucket",    BUCKET,
            "--date",      "{{ ds }}",
            "--agg-type",  "category_ranking",
        ],
        verbose=True,
        driver_memory="512m",
        executor_memory="1g",
        executor_cores=1,
        num_executors=2,
    )

    # Task C – hourly trend analysis
    hourly_trends = SparkSubmitOperator(
        task_id="aggregate_hourly_trends",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/03_aggregate.py",
        name="AggHourlyTrends-{{ ds_nodash }}",
        deploy_mode="client",
        conf=SPARK_S3A_CONF,
        packages=SPARK_JARS_PACKAGES,
        application_args=[
            "--bucket",    BUCKET,
            "--date",      "{{ ds }}",
            "--agg-type",  "hourly_trends",
        ],
        verbose=True,
        driver_memory="512m",
        executor_memory="1g",
        executor_cores=1,
        num_executors=2,
    )

    log_complete = PythonOperator(
        task_id="log_aggregation_complete",
        python_callable=_log_agg_complete,
    )

    # Pipeline graph:
    #   wait → log_start → daily_summary  ──┐
    #                    → category_ranking  ├── log_complete
    #                    → hourly_trends   ──┘
    wait_for_processing >> log_start >> [daily_summary, category_ranking, hourly_trends] >> log_complete
