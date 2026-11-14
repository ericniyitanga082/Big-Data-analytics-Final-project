
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Project directories
root = Path(__file__).resolve().parent.parent
files = sorted((root / "data").glob("transactions_*.json"))

output = root / "outputs" / "spark_sales"
output.mkdir(parents=True, exist_ok=True)

if not files:
    raise FileNotFoundError("No transaction files found")

print(f"Found {len(files)} transaction files", flush=True)

# 2. Start Spark with the same working configuration
spark = (
    SparkSession.builder
    .appName("AUCA_Sales_Analytics")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # 3. Load transaction data
    df = (
        spark.read
        .option("multiLine", "true")
        .json([str(f) for f in files])
        .select("transaction_id", "timestamp", "total", "status")
    )

    # 4. Filter completed and delivered transactions
    sales = df.filter(
        F.col("status").isin("completed", "delivered")
        & F.col("total").isNotNull()
    )

    # 5. Overall sales analytics
    result = sales.agg(
        F.count("*").alias("transactions"),
        F.round(F.sum("total"), 2).alias("revenue"),
        F.round(F.avg("total"), 2).alias("average_order_value")
    )

    print("\nOVERALL SALES PERFORMANCE", flush=True)
    result.show(truncate=False)

    # 6. Export overall results
    result.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "sales_summary"))

    print("Sales summary saved.", flush=True)

    # 7. Daily revenue analysis
    daily = (
        sales
        .withColumn(
            "date",
            F.to_date(F.to_timestamp("timestamp"))
        )
        .filter(F.col("date").isNotNull())
        .groupBy("date")
        .agg(
            F.count("*").alias("transactions"),
            F.round(F.sum("total"), 2).alias("revenue")
        )
        .orderBy("date")
    )

    print("\nDAILY SALES PERFORMANCE", flush=True)
    daily.show(10, truncate=False)

    # 8. Export daily revenue
    daily.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "daily_revenue"))

    print("\nSPARK SALES ANALYTICS COMPLETED SUCCESSFULLY")
    print(f"Results saved to: {output}")

finally:
    spark.stop()
