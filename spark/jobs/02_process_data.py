"""
spark_jobs/02_process_data.py
────────────────────────────────────────────────────────────────────────────────
Three-phase processing pipeline invoked once per phase by Airflow:

  Phase clean  : Read raw Parquet → filter bad rows → write to staging/clean/
  Phase enrich : Read staging/clean/ → add derived columns → write to staging/enriched/
  Phase write  : Read staging/enriched/ → write to processed/ (final zone)

Country lookup join is performed during enrich so enriched data is self-contained.
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── CLI args ──────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--bucket", default="data-pipeline")
parser.add_argument("--date",   required=True, help="YYYY-MM-DD")
parser.add_argument("--phase",  required=True, choices=["clean", "enrich", "write"])
args = parser.parse_args()

run_date = datetime.strptime(args.date, "%Y-%m-%d")
YEAR, MONTH, DAY = run_date.year, run_date.month, run_date.day
BUCKET = args.bucket
DATE_SUFFIX = f"year={YEAR}/month={MONTH:02d}/day={DAY:02d}"

PATHS = {
    "raw":       f"s3a://{BUCKET}/raw/{DATE_SUFFIX}/",
    "clean":     f"s3a://{BUCKET}/staging/clean/{DATE_SUFFIX}/",
    "enriched":  f"s3a://{BUCKET}/staging/enriched/{DATE_SUFFIX}/",
    "processed": f"s3a://{BUCKET}/processed/",
}

print(f"[02_process_data] phase={args.phase}  date={args.date}")

# ── Spark session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName(f"DataProcessing-{args.phase}-{args.date}")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ── Phase: clean ──────────────────────────────────────────────────────────────
if args.phase == "clean":
    raw_df = spark.read.parquet(PATHS["raw"])
    print(f"  raw rows = {raw_df.count():,}")

    clean_df = (
        raw_df
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("revenue") > 0)
        .filter(F.col("quantity") > 0)
    )
    dropped = raw_df.count() - clean_df.count()
    print(f"  dropped {dropped:,} rows (nulls / non-positive values)")

    (
        clean_df
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(PATHS["clean"])
    )
    print(f"  wrote {clean_df.count():,} clean rows → {PATHS['clean']}")


# ── Phase: enrich ─────────────────────────────────────────────────────────────
elif args.phase == "enrich":
    country_map = {
        "US": "United States", "UK": "United Kingdom",
        "DE": "Germany",       "FR": "France",
        "JP": "Japan",         "IN": "India",
        "CA": "Canada",        "AU": "Australia",
    }
    country_lookup = spark.createDataFrame(
        [(k, v) for k, v in country_map.items()],
        ["country_code", "country_name"],
    )

    clean_df = spark.read.parquet(PATHS["clean"])
    print(f"  clean rows = {clean_df.count():,}")

    ts_utc = F.to_utc_timestamp("event_ts", "UTC")
    enriched_df = (
        clean_df
        .join(F.broadcast(country_lookup), clean_df["country"] == country_lookup["country_code"], how="left")
        .select(
            *[F.col(c) for c in clean_df.columns if c != "country"],
            clean_df["country"].alias("country_code"),
            F.col("country_name"),
            ts_utc.alias("event_ts_utc"),
            F.hour(ts_utc).alias("event_hour"),
            F.dayofweek(ts_utc).alias("event_weekday"),
            F.dayofweek(ts_utc).isin([1, 7]).alias("is_weekend"),
            F.when(F.col("unit_price") < 25,  "Budget")
             .when(F.col("unit_price") < 100, "Mid-Range")
             .when(F.col("unit_price") < 500, "Premium")
             .otherwise("Luxury").alias("price_tier"),
            F.when(F.col("revenue") < 50,   "Low")
             .when(F.col("revenue") < 250,  "Medium")
             .when(F.col("revenue") < 1000, "High")
             .otherwise("Very High").alias("revenue_band"),
            (F.col("discount_pct") > 0).alias("is_discounted"),
            F.round(F.col("revenue") * F.lit(0.35), 2).alias("gross_margin_est"),
            F.current_timestamp().alias("processing_ts"),
            F.lit(YEAR).alias("year"),
            F.lit(MONTH).alias("month"),
            F.lit(DAY).alias("day"),
        )
    )

    (
        enriched_df
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(PATHS["enriched"])
    )
    print(f"  wrote enriched rows → {PATHS['enriched']}")


# ── Phase: write ──────────────────────────────────────────────────────────────
elif args.phase == "write":
    enriched_df = spark.read.parquet(PATHS["enriched"])
    row_count = enriched_df.count()
    print(f"  enriched rows = {row_count:,}")

    (
        enriched_df
        .repartition(4, "category")
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("category", "year", "month", "day")
        .parquet(PATHS["processed"])
    )
    written = spark.read.parquet(PATHS["processed"]).count()
    print(f"[02_process_data] SUCCESS – {written:,} rows in processed zone → {PATHS['processed']}")

spark.stop()
