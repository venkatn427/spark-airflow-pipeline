#!/usr/bin/env bash
# scripts/start.sh – one-command local startup for the full Airflow 3 + Spark 4 + MinIO stack
set -e

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${CYAN}[pipeline]${NC} $*"; }
ok()   { echo -e "${GREEN}[ok]${NC} $*"; }

command -v docker >/dev/null 2>&1 || { echo "Docker not found"; exit 1; }
DC="docker compose"

# Ensure runtime dirs exist
mkdir -p logs/airflow logs/spark airflow/plugins
chmod -R 777 logs/airflow logs/spark

# Build custom Airflow image (uses layer cache when unchanged)
log "Building Airflow image…"
$DC build --quiet

# Pull external images
log "Pulling images…"
$DC pull --quiet postgres minio minio-init spark-master spark-worker-1 spark-worker-2

# Start infrastructure
log "Starting Postgres, MinIO, Spark…"
$DC up -d postgres minio spark-master spark-worker-1 spark-worker-2

log "Waiting for MinIO…"
until $DC exec -T minio curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; do sleep 3; done
ok "MinIO ready"

log "Creating MinIO buckets…"
$DC run --rm minio-init

log "Waiting for Spark master…"
until $DC exec -T spark-master curl -sf http://localhost:8082 >/dev/null 2>&1; do sleep 3; done
ok "Spark master ready"

log "Initialising Airflow DB and admin user…"
$DC up -d airflow-init
$DC wait airflow-init
ok "Airflow DB ready"

log "Starting Airflow services (api-server, scheduler, dag-processor, triggerer)…"
$DC up -d airflow-webserver airflow-scheduler airflow-dag-processor airflow-triggerer

log "Waiting for Airflow api-server…"
until $DC exec -T airflow-webserver curl -sf http://localhost:8080/public/health >/dev/null 2>&1; do sleep 5; done
ok "Airflow api-server ready"

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║     Airflow 3 + Spark 4 + MinIO  –  ALL SYSTEMS GO      ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  Airflow UI   →  ${CYAN}http://localhost:8080${NC}  (admin / admin)"
echo -e "  Spark UI     →  ${CYAN}http://localhost:8082${NC}"
echo -e "  MinIO UI     →  ${CYAN}http://localhost:9001${NC}  (minioadmin / minioadmin123)"
echo ""
echo -e "  Logs         →  ${YELLOW}logs/airflow/${NC}  &  ${YELLOW}logs/spark/${NC}"
echo -e "  Stop:           ${YELLOW}make down${NC}   |   Full cleanup: ${YELLOW}make clean${NC}"
echo ""
