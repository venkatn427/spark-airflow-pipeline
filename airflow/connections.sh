#!/usr/bin/env bash
# infra/airflow_connections.sh – bootstraps Airflow connections at init time.
set -e

echo ">>> Configuring Airflow connections…"

upsert_conn() {
  airflow connections delete "$1" 2>/dev/null || true
  airflow connections add "$@"
}

# Spark – spark_home points to the volume-mounted Spark binaries inside Airflow containers (/opt/spark)
upsert_conn spark_default \
  --conn-type spark \
  --conn-host spark://spark-master \
  --conn-port 7077 \
  --conn-extra '{"queue":"root.default","deploy_mode":"client","spark_home":"/opt/spark","spark_binary":"spark-submit"}'

# MinIO / S3
upsert_conn minio_default \
  --conn-type aws \
  --conn-login minioadmin \
  --conn-password minioadmin123 \
  --conn-extra '{"endpoint_url":"http://minio:9000","region_name":"us-east-1"}'

echo ">>> Connections configured."
