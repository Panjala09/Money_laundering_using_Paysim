import os
import sys
import glob

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ----------------------------------------------------
# 1. Locate the CSV file
# ----------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)

raw_dir = os.path.join(PROJECT_DIR, "data", "raw")
csv_files = glob.glob(os.path.join(raw_dir, "*.csv"))

if not csv_files:
    print(f"ERROR: No CSV files found in {raw_dir}")
    sys.exit(1)

csv_path = csv_files[0]
print(f"Using CSV file: {csv_path}")

# ----------------------------------------------------
# 2. Create Spark session
# ----------------------------------------------------
spark = (
    SparkSession.builder
    .appName("AML_PaySim_Spark_Cleaning")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------------------
# 3. Read CSV into Spark DataFrame
# ----------------------------------------------------
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(csv_path)
)

print("✅ CSV loaded into Spark.")
print("Schema:")
df.printSchema()

# ----------------------------------------------------
# 4. Basic column checks / renames
# ----------------------------------------------------
expected_cols = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
]

missing = [c for c in expected_cols if c not in df.columns]
if missing:
    print("ERROR: Missing expected columns:", missing)
    spark.stop()
    sys.exit(1)

df = (
    df
    .withColumnRenamed("nameOrig", "name_orig")
    .withColumnRenamed("oldbalanceOrg", "old_balance_orig")
    .withColumnRenamed("newbalanceOrig", "new_balance_orig")
    .withColumnRenamed("nameDest", "name_dest")
    .withColumnRenamed("oldbalanceDest", "old_balance_dest")
    .withColumnRenamed("newbalanceDest", "new_balance_dest")
    .withColumnRenamed("isFraud", "is_fraud")
    .withColumnRenamed("isFlaggedFraud", "is_flagged_fraud")
)

# Add transaction_id similar to SQLite (monotonically increasing)
df = df.withColumn("transaction_id", F.monotonically_increasing_id() + 1)

# ----------------------------------------------------
# 5. Time-based features (event_time, hour_of_day, day_of_week)
# ----------------------------------------------------
# 'step' is in HOURS from a base timestamp. We'll compute:
# event_time = base_time + step * 3600 seconds

base_time_str = "2018-01-01 00:00:00"

base_unix = F.unix_timestamp(F.lit(base_time_str), "yyyy-MM-dd HH:mm:ss")

df = df.withColumn(
    "event_time",
    F.from_unixtime(base_unix + F.col("step") * F.lit(3600)).cast("timestamp")
)

df = df.withColumn("hour_of_day", F.hour("event_time"))

# day_of_week: Monday=0, Sunday=6
df = df.withColumn(
    "day_of_week",
    (F.dayofweek("event_time") + 5) % 7  # Spark: Sunday=1 → we map to 6, etc.
)

# ----------------------------------------------------
# 6. Balance change features
# ----------------------------------------------------
df = df.withColumn(
    "src_balance_change",
    F.col("new_balance_orig") - F.col("old_balance_orig")
)

df = df.withColumn(
    "dst_balance_change",
    F.col("new_balance_dest") - F.col("old_balance_dest")
)

# ----------------------------------------------------
# 7. Rename some for clarity (matching clean_transactions style)
# ----------------------------------------------------
df = (
    df
    .withColumnRenamed("name_orig", "src_account_id")
    .withColumnRenamed("name_dest", "dst_account_id")
    .withColumnRenamed("type", "transaction_type")
    .withColumnRenamed("amount", "transaction_amount")
)

# Reorder columns for readability
cols_order = [
    "transaction_id",
    "event_time",
    "step",
    "hour_of_day",
    "day_of_week",
    "transaction_type",
    "transaction_amount",
    "src_account_id",
    "old_balance_orig",
    "new_balance_orig",
    "src_balance_change",
    "dst_account_id",
    "old_balance_dest",
    "new_balance_dest",
    "dst_balance_change",
    "is_fraud",
    "is_flagged_fraud",
]

cols_order = [c for c in cols_order if c in df.columns]
df = df.select(cols_order)

print("✅ Spark clean DataFrame prepared.")
df.printSchema()

# ----------------------------------------------------
# 8. Write output as Parquet
# ----------------------------------------------------
output_dir = os.path.join(PROJECT_DIR, "data", "spark", "clean_transactions")

print(f"Writing Spark clean dataset to: {output_dir}")

(
    df.write
    .mode("overwrite")
    .parquet(output_dir)
)

print("🎉 Spark clean_transactions written successfully.")

spark.stop()

