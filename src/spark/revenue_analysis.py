
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
OUTPUT = ROOT / "outputs"

OUTPUT.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("AUCA_Revenue_Analysis")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    transactions = (
        spark.read.option("multiLine", "true")
        .json(str(DATA / "transactions_*.json"))
        .select(
            "transaction_id", "user_id", "timestamp",
            "status", "total", "discount", "items"
        )
    )

    # Revenue recognized only for completed/delivered sales.
    sales = (
        transactions
        .filter(F.col("status").isin("completed", "delivered"))
        .withColumn("sale_date", F.to_date("timestamp"))
    )

    sales.createOrReplaceTempView("sales")

    print("\n========== REVENUE SUMMARY ==========")

    summary = spark.sql("""
        SELECT
            COUNT(*) AS transactions,
            COUNT(DISTINCT user_id) AS paying_customers,
            ROUND(SUM(total), 2) AS total_revenue,
            ROUND(AVG(total), 2) AS average_order_value
        FROM sales
    """)

    summary.show(truncate=False)

    print("\n========== DAILY REVENUE ==========")

    daily_revenue = spark.sql("""
        SELECT
            sale_date,
            COUNT(*) AS orders,
            ROUND(SUM(total), 2) AS revenue
        FROM sales
        GROUP BY sale_date
        ORDER BY sale_date
    """)

    daily_revenue.show(15, truncate=False)

    print("\n========== TOP REVENUE DAYS ==========")

    daily_revenue.orderBy(
        F.desc("revenue")
    ).show(10, truncate=False)

    # Small daily summary, suitable for plotting.
    daily_revenue.coalesce(1).write.mode(
        "overwrite"
    ).option("header", True).csv(
        str(OUTPUT / "daily_revenue")
    )

    print("\nRevenue analysis completed.")
    print("Results saved to outputs/daily_revenue")

finally:
    spark.stop()
