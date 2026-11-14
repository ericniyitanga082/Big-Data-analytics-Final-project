
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Project directories
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"

# Start Spark
spark = (
    SparkSession.builder
    .appName("AUCA_Ecommerce_Data_Cleaning")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # Read the source JSON files
    users = spark.read.option("multiLine", "true").json(
        str(DATA_DIR / "users.json")
    )

    products = spark.read.option("multiLine", "true").json(
        str(DATA_DIR / "products.json")
    )

    categories = spark.read.option("multiLine", "true").json(
        str(DATA_DIR / "categories.json")
    )

    transactions = spark.read.option("multiLine", "true").json(
        str(DATA_DIR / "transactions_*.json")
    )

    sessions = spark.read.option("multiLine", "true").json(
        str(DATA_DIR / "sessions_*.json")
    )

    # Normalize timestamp fields
    transactions = transactions.withColumn(
        "transaction_timestamp",
        F.to_timestamp("timestamp")
    )

    sessions = sessions.withColumn(
        "session_start_timestamp",
        F.to_timestamp("start_time")
    )

    # Remove records missing essential identifiers
    users = users.filter(F.col("user_id").isNotNull())
    products = products.filter(F.col("product_id").isNotNull())
    transactions = transactions.filter(
        F.col("transaction_id").isNotNull()
    )
    sessions = sessions.filter(
        F.col("session_id").isNotNull()
    )

    # Report record counts
    print("\n========== DATA QUALITY SUMMARY ==========")

    for name, df in [
        ("Users", users),
        ("Products", products),
        ("Categories", categories),
        ("Transactions", transactions),
        ("Sessions", sessions),
    ]:
        print(f"{name}: {df.count():,} records")

    print("\nTransaction schema:")
    transactions.printSchema()

    print("\nSession schema:")
    sessions.printSchema()

    print("\nData loading and initial cleaning completed.")

finally:
    spark.stop()
