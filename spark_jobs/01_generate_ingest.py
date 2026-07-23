"""
spark_jobs/01_generate_ingest.py
────────────────────────────────────────────────────────────────────────────────
Spark application: generate synthetic sales events and write them to the MinIO
RAW zone as Parquet, partitioned by year / month / day.

Partition layout
────────────────
s3a://<bucket>/raw/
    year=YYYY/
        month=MM/
            day=DD/
                part-<n>-<uuid>.snappy.parquet

Data schema
───────────
order_id          string      UUID-based unique order ID
customer_id       string      UUID
product_id        string      UUID
category          string      Electronics | Clothing | Food | Books | Sports
sub_category      string      fine-grained category
quantity          integer     1 – 20
unit_price        double      1.00 – 999.99
discount_pct      double      0.00 – 0.50
revenue           double      quantity * unit_price * (1 - discount_pct)
region            string      North | South | East | West | Central
country           string      US | UK | DE | FR | JP | IN | CA | AU
channel           string      Online | In-Store | Mobile
event_ts          timestamp   random timestamp within the run date
year              integer     partition column
month             integer     partition column
day               integer     partition column
"""

import argparse
import uuid
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType,
)

# ── CLI args ──────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--bucket",       default="data-pipeline")
parser.add_argument("--date",         required=True,  help="YYYY-MM-DD")
parser.add_argument("--num-records",  type=int, default=100_000)
args = parser.parse_args()

run_date   = datetime.strptime(args.date, "%Y-%m-%d")
YEAR       = run_date.year
MONTH      = run_date.month
DAY        = run_date.day
BUCKET     = args.bucket
NUM_REC    = args.num_records
RAW_PATH   = f"s3a://{BUCKET}/raw/year={YEAR}/month={MONTH:02d}/day={DAY:02d}/"

print(f"[01_generate_ingest] date={args.date}  records={NUM_REC}  -> {RAW_PATH}")

# ── Spark session (inherits cluster config from spark-defaults.conf) ───────────
spark = (
    SparkSession.builder
    .appName(f"DataIngestion-{args.date}")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Reference data ────────────────────────────────────────────────────────────
CATEGORIES = {
    "Electronics": ["Smartphones", "Laptops", "Tablets", "Cameras", "Audio"],
    "Clothing":    ["Men's", "Women's", "Kids'", "Sportswear", "Accessories"],
    "Food":        ["Beverages", "Snacks", "Dairy", "Bakery", "Organic"],
    "Books":       ["Fiction", "Non-Fiction", "Children's", "Academic", "Comics"],
    "Sports":      ["Fitness", "Outdoor", "Team Sports", "Water Sports", "Winter"],
}
REGIONS   = ["North", "South", "East", "West", "Central"]
COUNTRIES = ["US", "UK", "DE", "FR", "JP", "IN", "CA", "AU"]
CHANNELS  = ["Online", "In-Store", "Mobile"]

def _gen_row(_):
    """Generate one synthetic order row."""
    cat        = random.choice(list(CATEGORIES))
    sub_cat    = random.choice(CATEGORIES[cat])
    quantity   = random.randint(1, 20)
    unit_price = round(random.uniform(1.0, 999.99), 2)
    discount   = round(random.uniform(0.0, 0.50), 2)
    revenue    = round(quantity * unit_price * (1 - discount), 2)
    # Random timestamp within the run date
    secs_offset = random.randint(0, 86399)
    event_ts    = run_date + timedelta(seconds=secs_offset)
    return (
        str(uuid.uuid4()),          # order_id
        str(uuid.uuid4()),          # customer_id
        str(uuid.uuid4()),          # product_id
        cat,                        # category
        sub_cat,                    # sub_category
        quantity,                   # quantity
        unit_price,                 # unit_price
        discount,                   # discount_pct
        revenue,                    # revenue
        random.choice(REGIONS),     # region
        random.choice(COUNTRIES),   # country
        random.choice(CHANNELS),    # channel
        event_ts,                   # event_ts
        YEAR, MONTH, DAY,           # partition cols
    )

# ── Generate data via Spark parallelism ──────────────────────────────────────
schema = StructType([
    StructField("order_id",     StringType(),    False),
    StructField("customer_id",  StringType(),    False),
    StructField("product_id",   StringType(),    False),
    StructField("category",     StringType(),    False),
    StructField("sub_category", StringType(),    False),
    StructField("quantity",     IntegerType(),   False),
    StructField("unit_price",   DoubleType(),    False),
    StructField("discount_pct", DoubleType(),    False),
    StructField("revenue",      DoubleType(),    False),
    StructField("region",       StringType(),    False),
    StructField("country",      StringType(),    False),
    StructField("channel",      StringType(),    False),
    StructField("event_ts",     TimestampType(), False),
    StructField("year",         IntegerType(),   False),
    StructField("month",        IntegerType(),   False),
    StructField("day",          IntegerType(),   False),
])

rdd = spark.sparkContext.parallelize(range(NUM_REC), numSlices=8).map(_gen_row)
df  = spark.createDataFrame(rdd, schema=schema)

# ── Write partitioned Parquet to MinIO ───────────────────────────────────────
(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    # year/month/day are already baked into the path; don't double-partition
    .parquet(RAW_PATH)
)

count = spark.read.parquet(RAW_PATH).count()
print(f"[01_generate_ingest] SUCCESS – wrote {count:,} rows to {RAW_PATH}")

spark.stop()
