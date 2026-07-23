# Spark + Airflow + MinIO Data Pipeline

A production-style local data pipeline using:
- **Apache Airflow 2.10.4** — orchestration & scheduling
- **Apache Spark 3.5** (Bitnami image) — distributed data processing
- **MinIO** (latest) — S3-compatible local object storage
- **Docker Compose** — single-command spin-up

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Network                           │
│                                                                 │
│   ┌──────────────┐     ┌──────────────────────────────────┐    │
│   │   Airflow    │────▶│         Spark Cluster            │    │
│   │  (Scheduler  │     │  ┌─────────────┐                 │    │
│   │  + Webserver)│     │  │ Spark Master│                 │    │
│   └──────────────┘     │  │  :7077/:8082│                 │    │
│          │             │  └──────┬──────┘                 │    │
│          │ SparkSubmit │         │ manages                 │    │
│          └────────────▶│  ┌──────┴──────┐ ┌────────────┐  │    │
│                        │  │  Worker 1   │ │  Worker 2  │  │    │
│                        │  │ (2G / 2CPU) │ │(2G / 2CPU) │  │    │
│                        │  └─────────────┘ └────────────┘  │    │
│                        └──────────────────────────────────┘    │
│                                    │ S3A                        │
│                        ┌───────────▼──────────────────────┐    │
│                        │             MinIO                 │    │
│                        │  :9000 (API)  :9001 (Console)    │    │
│                        │  data-pipeline/                   │    │
│                        │    raw/       processed/          │    │
│                        │    aggregated/                    │    │
│                        └──────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

```bash
# 1. Clone / unzip the project
cd spark-airflow-pipeline

# 2. Make scripts executable
chmod +x scripts/start.sh scripts/stop.sh

# 3. Start everything (first run downloads ~4 GB of images)
./scripts/start.sh
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| Spark Master UI | http://localhost:8082 | — |
| Spark Worker 1 | http://localhost:8083 | — |
| Spark Worker 2 | http://localhost:8084 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |

---

## DAGs

### DAG 01 — `01_data_ingestion`
- **Schedule**: `@daily`
- **What it does**: Submits `spark_jobs/01_generate_ingest.py` to the Spark cluster.
  - Generates **100,000 synthetic sales records** using PySpark parallelism.
  - Writes Parquet to `s3a://data-pipeline/raw/year=YYYY/month=MM/day=DD/`.
- **Trigger manually**: Airflow UI → enable DAG → click ▶.

### DAG 02 — `02_data_processing`
- **Schedule**: `@daily`
- **Dependencies**: Waits for DAG 01 via `ExternalTaskSensor`.
- **What it does**: Submits `spark_jobs/02_process_data.py`.
  - Cleans, enriches, and transforms raw data.
  - Writes to `s3a://data-pipeline/processed/category=*/year=*/month=*/day=*/`.

### DAG 03 — `03_data_aggregation`
- **Schedule**: `@daily`
- **Dependencies**: Waits for DAG 02 via `ExternalTaskSensor`.
- **What it does**: Runs three parallel Spark jobs via `spark_jobs/03_aggregate.py`:
  - `daily_summary` → revenue + volume per category/region/channel.
  - `category_ranking` → top categories by month-to-date revenue.
  - `hourly_trends` → hourly order volume + cumulative revenue.

---

## MinIO Folder & Partition Layout

```
data-pipeline/
├── raw/
│   └── year=2025/
│       └── month=01/
│           └── day=15/
│               └── part-*.snappy.parquet
├── processed/
│   └── category=Electronics/
│       └── year=2025/
│           └── month=01/
│               └── day=15/
│                   └── part-*.snappy.parquet
└── aggregated/
    ├── daily_summary/
    │   └── year=2025/month=01/day=15/
    ├── category_ranking/
    │   └── year=2025/month=01/
    └── hourly_trends/
        └── year=2025/month=01/day=15/
```

---

## Spark Cluster Mode & Dynamic Allocation

When a DAG triggers `SparkSubmitOperator`:

1. Airflow calls `spark-submit` pointing at `spark://spark-master:7077`.
2. The **driver** runs inside the Airflow container (deploy-mode = client).
3. The Spark master assigns **executor slots** on Worker 1 and Worker 2.
4. With **Dynamic Allocation** enabled:
   - Executors spin up as tasks are ready (min=0, max=4).
   - Idle executors are released after **60 seconds**.
   - After the job finishes, all executor resources are returned to the pool.

To watch this in real time: open the **Spark Master UI** at http://localhost:8082 while a DAG is running.

---

## Data Schema (Raw Zone)

| Column | Type | Description |
|--------|------|-------------|
| order_id | string | UUID primary key |
| customer_id | string | UUID |
| product_id | string | UUID |
| category | string | Electronics \| Clothing \| Food \| Books \| Sports |
| sub_category | string | Fine-grained category |
| quantity | integer | 1–20 |
| unit_price | double | 1.00–999.99 |
| discount_pct | double | 0.00–0.50 |
| revenue | double | qty × price × (1 − discount) |
| region | string | North \| South \| East \| West \| Central |
| country | string | US \| UK \| DE \| FR \| JP \| IN \| CA \| AU |
| channel | string | Online \| In-Store \| Mobile |
| event_ts | timestamp | Random time within the run date |
| year / month / day | int | Hive partition columns |

---

## Troubleshooting

**Airflow can't find spark-submit**
```bash
docker exec -it pipeline-airflow-scheduler which spark-submit
# If missing, the spark-home volume may not have mounted yet; restart:
docker compose restart airflow-scheduler
```

**S3A / MinIO connection errors in Spark logs**
- Confirm MinIO is healthy: `docker compose ps minio`
- Check the Spark conf: `config/spark-defaults.conf` → `fs.s3a.endpoint`

**ExternalTaskSensor times out**
- Ensure DAG 01 completed successfully before DAG 02 starts.
- Sensors poll every 30 s and timeout after 1 hour.

**Reset everything**
```bash
./scripts/stop.sh
docker compose down -v   # removes all volumes / data
./scripts/start.sh
```

---

## Stopping

```bash
./scripts/stop.sh
# or to wipe all data volumes:
docker compose down -v
```
