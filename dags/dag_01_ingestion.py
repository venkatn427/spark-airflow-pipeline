"""
DAG 1 – data_ingestion_dag
────────────────────────────────────────────────────────────────────────────────
Generates synthetic sales / event data and uploads it to MinIO under:

  s3a://data-pipeline/raw/
      year=YYYY/
          month=MM/
              day=DD/
                  sales_YYYYMMDD_<run_id>.parquet

The actual heavy lifting (data generation + Parquet write) is done by a Spark
job submitted to the Spark cluster in CLIENT mode.  The Airflow scheduler merely
calls spark-submit via SparkSubmitOperator.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Default args ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET         = os.getenv("MINIO_BUCKET", "data-pipeline")

# Spark S3A config passed as --conf flags so the driver / executors can
# talk to MinIO.
SPARK_S3A_CONF = {
    "spark.hadoop.fs.s3a.endpoint":            MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key":          MINIO_ACCESS,
    "spark.hadoop.fs.s3a.secret.key":          MINIO_SECRET,
    "spark.hadoop.fs.s3a.path.style.access":   "true",
    "spark.hadoop.fs.s3a.impl":                "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    # Dynamic allocation – scale up for the job, idle executors released after
    "spark.dynamicAllocation.enabled":                  "true",
    "spark.dynamicAllocation.minExecutors":             "0",
    "spark.dynamicAllocation.maxExecutors":             "4",
    "spark.dynamicAllocation.shuffleTracking.enabled":  "true",
}

SPARK_JARS_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def _log_ingestion_start(**context):
    exec_date = context["logical_date"]
    print(
        f"[Ingestion DAG] Starting run for logical_date={exec_date.date()}\n"
        f"  Target : s3a://{BUCKET}/raw/"
        f"year={exec_date.year}/month={exec_date.month:02d}/day={exec_date.day:02d}/"
    )


with DAG(
    dag_id="01_data_ingestion",
    description="Generate random sales data with Spark and land it in MinIO raw zone",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",          # runs once per day; trigger manually to test
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "spark", "minio"],
) as dag:

    log_start = PythonOperator(
        task_id="log_ingestion_start",
        python_callable=_log_ingestion_start,
    )

    # ── SparkSubmitOperator spins up a Spark driver + executors, runs the job,
    #    then the cluster resources are released automatically once the driver exits.
    generate_and_ingest = SparkSubmitOperator(
        task_id="generate_and_ingest_to_minio",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/01_generate_ingest.py",
        name="DataIngestion-{{ ds_nodash }}",
        deploy_mode="client",          # driver runs inside the Airflow container
        conf=SPARK_S3A_CONF,
        packages=SPARK_JARS_PACKAGES,
        application_args=[
            "--bucket",     BUCKET,
            "--date",       "{{ ds }}",          # YYYY-MM-DD
            "--num-records", "100000",
        ],
        verbose=True,
        # Spark resources for this job
        driver_memory="512m",
        executor_memory="1g",
        executor_cores=1,
        num_executors=2,
    )

    log_start >> generate_and_ingest
