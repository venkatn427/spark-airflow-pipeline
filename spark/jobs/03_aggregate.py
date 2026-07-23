"""
spark_jobs/03_aggregate.py
────────────────────────────────────────────────────────────────────────────────
Aggregation Spark application – called three times by DAG 03, once per
--agg-type value:

  daily_summary     – revenue, order volume, avg basket per category × region × channel
  category_ranking  – top categories by revenue for the run month
  hourly_trends     – hourly order volume + avg revenue per hour

Output partition layouts
────────────────────────
daily_summary/
    year=YYYY/month=MM/day=DD/
        part-*.snappy.parquet

category_ranking/
    year=YYYY/month=MM/
        part-*.snappy.parquet

hourly_trends/
    year=YYYY/month=MM/day=DD/
        part-*.snappy.parquet
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ── CLI ───────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--bucket",   default="data-pipeline")
parser.add_argument("--date",     required=True, help="YYYY-MM-DD")
parser.add_argument("--agg-type", required=True,
                    choices=["daily_summary", "category_ranking", "hourly_trends"])
args = parser.parse_args()

run_date = datetime.strptime(args.date, "%Y-%m-%d")
YEAR, MONTH, DAY = run_date.year, run_date.month, run_date.day
BUCKET   = args.bucket
AGG_TYPE = args.agg_type

PROCESSED_PATH = f"s3a://{BUCKET}/processed/"
AGG_BASE       = f"s3a://{BUCKET}/aggregated/{AGG_TYPE}/"

print(f"[03_aggregate] type={AGG_TYPE}  date={args.date}")
print(f"  source : {PROCESSED_PATH}")
print(f"  target : {AGG_BASE}")

# ── Spark session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName(f"Aggregation-{AGG_TYPE}-{args.date}")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Read processed data filtered to the run date ──────────────────────────────
processed_df = (
    spark.read
    .parquet(PROCESSED_PATH)
    .filter(
        (F.col("year")  == YEAR) &
        (F.col("month") == MONTH) &
        (F.col("day")   == DAY)
    )
    .cache()
)
print(f"  processed rows for date = {processed_df.count():,}")

# ─────────────────────────────────────────────────────────────────────────────
# AGG 1 – Daily summary
# ─────────────────────────────────────────────────────────────────────────────
if AGG_TYPE == "daily_summary":
    agg_df = (
        processed_df
        .groupBy("category", "region", "channel", "price_tier", "year", "month", "day")
        .agg(
            F.count("order_id")             .alias("order_count"),
            F.sum("revenue")                .alias("total_revenue"),
            F.avg("revenue")                .alias("avg_order_value"),
            F.sum("quantity")               .alias("total_units_sold"),
            F.avg("discount_pct")           .alias("avg_discount_pct"),
            F.sum("gross_margin_est")       .alias("total_gross_margin"),
            F.countDistinct("customer_id")  .alias("unique_customers"),
            F.countDistinct("product_id")   .alias("unique_products"),
            F.sum(F.col("is_discounted").cast("int")).alias("discounted_orders"),
        )
        .withColumn("discount_rate",
            F.round(F.col("discounted_orders") / F.col("order_count"), 4))
        .withColumn("revenue_per_unit",
            F.round(F.col("total_revenue") / F.col("total_units_sold"), 2))
        .withColumn("agg_ts", F.current_timestamp())
    )

    out_path = f"{AGG_BASE}year={YEAR}/month={MONTH:02d}/day={DAY:02d}/"
    (
        agg_df
        .drop("year", "month", "day")    # already in path
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_path)
    )

# ─────────────────────────────────────────────────────────────────────────────
# AGG 2 – Category ranking (month-to-date)
# ─────────────────────────────────────────────────────────────────────────────
elif AGG_TYPE == "category_ranking":
    # Read the full month of processed data, not just today
    month_df = (
        spark.read
        .parquet(PROCESSED_PATH)
        .filter((F.col("year") == YEAR) & (F.col("month") == MONTH))
        .cache()
    )
    print(f"  month-to-date rows = {month_df.count():,}")

    window_rank = Window.orderBy(F.col("total_revenue").desc())

    agg_df = (
        month_df
        .groupBy("category", "sub_category")
        .agg(
            F.count("order_id")             .alias("order_count"),
            F.sum("revenue")                .alias("total_revenue"),
            F.avg("unit_price")             .alias("avg_unit_price"),
            F.sum("quantity")               .alias("total_quantity"),
            F.countDistinct("customer_id")  .alias("unique_customers"),
            F.avg("discount_pct")           .alias("avg_discount_pct"),
        )
        .withColumn("revenue_rank", F.rank().over(window_rank))
        .withColumn("revenue_pct",
            F.round(
                F.col("total_revenue") /
                F.sum("total_revenue").over(Window.partitionBy()),
                4,
            )
        )
        .withColumn("year",  F.lit(YEAR))
        .withColumn("month", F.lit(MONTH))
        .withColumn("agg_ts", F.current_timestamp())
    )

    out_path = f"{AGG_BASE}year={YEAR}/month={MONTH:02d}/"
    (
        agg_df
        .drop("year", "month")
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_path)
    )

# ─────────────────────────────────────────────────────────────────────────────
# AGG 3 – Hourly trends
# ─────────────────────────────────────────────────────────────────────────────
elif AGG_TYPE == "hourly_trends":
    agg_df = (
        processed_df
        .groupBy("event_hour", "channel", "is_weekend", "year", "month", "day")
        .agg(
            F.count("order_id")      .alias("order_count"),
            F.sum("revenue")         .alias("total_revenue"),
            F.avg("revenue")         .alias("avg_order_value"),
            F.sum("quantity")        .alias("total_units"),
            F.avg("discount_pct")    .alias("avg_discount_pct"),
        )
        .withColumn("orders_per_minute",
            F.round(F.col("order_count") / F.lit(60), 2))
        .withColumn("agg_ts", F.current_timestamp())
        # Running total by hour within the day
        .withColumn(
            "cumulative_revenue",
            F.sum("total_revenue").over(
                Window.partitionBy("channel")
                      .orderBy("event_hour")
                      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )

    out_path = f"{AGG_BASE}year={YEAR}/month={MONTH:02d}/day={DAY:02d}/"
    (
        agg_df
        .drop("year", "month", "day")
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_path)
    )

print(f"[03_aggregate] SUCCESS – {AGG_TYPE} written to {AGG_BASE}")
spark.stop()
