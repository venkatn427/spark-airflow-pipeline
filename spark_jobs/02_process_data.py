"""
spark_jobs/02_process_data.py
────────────────────────────────────────────────────────────────────────────────
Reads raw Parquet for a given date from MinIO, applies business-rule
transformations, and writes cleaned / enriched data to the PROCESSED zone.

Transformations applied
───────────────────────
1.  Drop rows with null order_id or revenue <= 0 (data quality gate)
2.  Cast event_ts timezone to UTC and extract hour / weekday
3.  Compute price_tier   : Budget | Mid-Range | Premium | Luxury
4.  Compute revenue_band : Low | Medium | High | Very High
5.  Add is_discounted flag
6.  Normalise country codes → country_name (lookup join)
7.  Add processing_ts (pipeline watermark)

Partition layout (processed zone)
───────────────────────────────────
s3a://<bucket>/processed/
    category=<category>/
        year=YYYY/
            month=MM/
                day=DD/
                    part-*.snappy.parquet
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── CLI args ──────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--bucket", default="data-pipeline")
parser.add_argument("--date",   required=True, help="YYYY-MM-DD")
args = parser.parse_args()

run_date = datetime.strptime(args.date, "%Y-%m-%d")
YEAR, MONTH, DAY = run_date.year, run_date.month, run_date.day
BUCKET = args.bucket

RAW_PATH       = f"s3a://{BUCKET}/raw/year={YEAR}/month={MONTH:02d}/day={DAY:02d}/"
PROCESSED_PATH = f"s3a://{BUCKET}/processed/"

print(f"[02_process_data] date={args.date}")
print(f"  source : {RAW_PATH}")
print(f"  target : {PROCESSED_PATH}")

# ── Spark session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName(f"DataProcessing-{args.date}")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Country code → name lookup ────────────────────────────────────────────────
country_map = {
    "US": "United States", "UK": "United Kingdom",
    "DE": "Germany",       "FR": "France",
    "JP": "Japan",         "IN": "India",
    "CA": "Canada",        "AU": "Australia",
}
country_lookup = spark.createDataFrame(
    [(k, v) for k, v in country_map.items()],
    ["country", "country_name"],
)

# ── Read raw data ─────────────────────────────────────────────────────────────
raw_df = spark.read.parquet(RAW_PATH)
print(f"  raw row count = {raw_df.count():,}")

# ── Data quality filter ───────────────────────────────────────────────────────
clean_df = (
    raw_df
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("revenue") > 0)
    .filter(F.col("quantity") > 0)
)

# ── Transformations ───────────────────────────────────────────────────────────
enriched_df = (
    clean_df

    # Timestamp enrichment
    .withColumn("event_ts_utc", F.to_utc_timestamp("event_ts", "UTC"))
    .withColumn("event_hour",   F.hour("event_ts_utc"))
    .withColumn("event_weekday", F.dayofweek("event_ts_utc"))   # 1=Sun … 7=Sat
    .withColumn("is_weekend",   F.col("event_weekday").isin([1, 7]))

    # Price tier
    .withColumn(
        "price_tier",
        F.when(F.col("unit_price") < 25,   "Budget")
         .when(F.col("unit_price") < 100,  "Mid-Range")
         .when(F.col("unit_price") < 500,  "Premium")
         .otherwise("Luxury"),
    )

    # Revenue band (quartile-like buckets)
    .withColumn(
        "revenue_band",
        F.when(F.col("revenue") < 50,    "Low")
         .when(F.col("revenue") < 250,   "Medium")
         .when(F.col("revenue") < 1000,  "High")
         .otherwise("Very High"),
    )

    # Discount flag
    .withColumn("is_discounted", F.col("discount_pct") > 0)

    # Gross margin proxy (simplified; real margin would need cost data)
    .withColumn("gross_margin_est",
        F.round(F.col("revenue") * F.lit(0.35), 2))

    # Pipeline watermark
    .withColumn("processing_ts", F.current_timestamp())

    # Keep partition columns explicit
    .withColumn("year",  F.lit(YEAR))
    .withColumn("month", F.lit(MONTH))
    .withColumn("day",   F.lit(DAY))
)

# ── Join country name ──────────────────────────────────────────────────────────
final_df = (
    enriched_df
    .join(country_lookup, on="country", how="left")
    .withColumnRenamed("country", "country_code")
)

# ── Write to processed zone – partition by category / year / month / day ──────
(
    final_df
    .repartition(4, "category")           # one file-group per category
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("category", "year", "month", "day")
    .parquet(PROCESSED_PATH)
)

written_count = spark.read.parquet(PROCESSED_PATH).count()
print(f"[02_process_data] SUCCESS – {written_count:,} rows in processed zone")

spark.stop()
