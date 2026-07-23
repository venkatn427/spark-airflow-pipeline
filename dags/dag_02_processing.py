"""
DAG 2 – data_processing_dag
────────────────────────────────────────────────────────────────────────────────
Reads raw Parquet files from MinIO raw zone, applies transformations (cleansing,
enrichment, type casting, partitioning), and writes the results to the processed
zone:

  s3a://data-pipeline/processed/
      category=<category>/
          year=YYYY/
              month=MM/
                  day=DD/
                      part-*.parquet

Depends on DAG 01 having run for the same logical date (ExternalTaskSensor).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

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
    # Adaptive query execution
    "spark.sql.adaptive.enabled":              "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}

SPARK_JARS_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def _log_processing_start(**context):
    exec_date = context["logical_date"]
    print(
        f"[Processing DAG] Starting run for logical_date={exec_date.date()}\n"
        f"  Source : s3a://{BUCKET}/raw/year={exec_date.year}/...\n"
        f"  Target : s3a://{BUCKET}/processed/category=<*>/..."
    )


with DAG(
    dag_id="02_data_processing",
    description="Read raw MinIO data, transform with Spark, write to processed zone",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["processing", "spark", "minio"],
) as dag:

    # Wait for the ingestion DAG to complete first
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="01_data_ingestion",
        external_task_id="generate_and_ingest_to_minio",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        timeout=3600,
        poke_interval=30,
        mode="poke",
    )

    log_start = PythonOperator(
        task_id="log_processing_start",
        python_callable=_log_processing_start,
    )

    process_data = SparkSubmitOperator(
        task_id="process_raw_to_processed",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/02_process_data.py",
        name="DataProcessing-{{ ds_nodash }}",
        deploy_mode="client",
        conf=SPARK_S3A_CONF,
        packages=SPARK_JARS_PACKAGES,
        application_args=[
            "--bucket",  BUCKET,
            "--date",    "{{ ds }}",
        ],
        verbose=True,
        driver_memory="512m",
        executor_memory="1g",
        executor_cores=1,
        num_executors=2,
    )

    wait_for_ingestion >> log_start >> process_data
