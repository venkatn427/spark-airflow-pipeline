#!/usr/bin/env bash
# airflow_connections.sh
# Creates all required Airflow connections at startup.

set -e

echo ">>> Setting up Airflow connections..."

# Spark connection (used by SparkSubmitOperator)
airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark://spark-master \
  --conn-port 7077 \
  --conn-extra '{"queue": "root.default", "deploy_mode": "client", "spark_home": "/opt/bitnami/spark", "spark_binary": "spark-submit"}' \
  || airflow connections delete spark_default && \
  airflow connections add spark_default \
    --conn-type spark \
    --conn-host spark://spark-master \
    --conn-port 7077 \
    --conn-extra '{"queue": "root.default", "deploy_mode": "client", "spark_home": "/opt/bitnami/spark", "spark_binary": "spark-submit"}'

# MinIO / S3 connection
airflow connections add minio_default \
  --conn-type aws \
  --conn-login minioadmin \
  --conn-password minioadmin123 \
  --conn-extra '{"endpoint_url": "http://minio:9000", "region_name": "us-east-1"}' \
  || true

echo ">>> Connections configured."
