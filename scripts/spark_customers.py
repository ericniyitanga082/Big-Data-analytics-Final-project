
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
import matplotlib.pyplot as plt

# 1. Project directories
root = Path(__file__).resolve().parent.parent
files = sorted((root / "data").glob("transactions_*.json"))

output = root / "outputs" / "spark_customers"
charts = root / "outputs" / "charts"

output.mkdir(parents=True, exist_ok=True)
charts.mkdir(parents=True, exist_ok=True)

if not files:
    raise FileNotFoundError("No transaction files found")

# 2. Explicit transaction schema
schema = StructType([
    StructField("user_id", StringType()),
    StructField("status", StringType()),
    StructField("total", DoubleType())
])

# 3. Start Spark with low RAM usage
spark = (
    SparkSession.builder
    .appName("AUCA_Customer_Segmentation")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    partial = output / "partial"
    partial.mkdir(exist_ok=True)

    # 4. Process each transaction file
    for i, file in enumerate(files, 1):
        df = (
            spark.read.schema(schema)
            .option("multiLine", "true")
            .json(str(file))
        )

        sales = df.filter(
            F.col("status").isin("completed", "delivered")
            & F.col("user_id").isNotNull()
            & F.col("total").isNotNull()
        )

        batch = (
            sales.groupBy("user_id")
            .agg(
                F.count("*").alias("orders"),
                F.sum("total").alias("spending")
            )
        )

        batch.coalesce(1).write.mode("overwrite") \
            .parquet(str(partial / f"batch_{i:03d}"))

        print(f"Processed file {i}/{len(files)}", flush=True)

    # 5. Combine customer-level results
    combined = spark.read.parquet(
        str(partial / "batch_*")
    )

    customers = (
        combined.groupBy("user_id")
        .agg(
            F.sum("orders").alias("orders"),
            F.round(F.sum("spending"), 2).alias("spending")
        )
        .withColumn(
            "average_order_value",
            F.round(F.col("spending") / F.col("orders"), 2)
        )
    )

    # 6. Segment customers by spending
    # These are illustrative spending thresholds
    customers = customers.withColumn(
        "segment",
        F.when(F.col("spending") >= 50000, "High Value")
         .when(F.col("spending") >= 20000, "Medium Value")
         .otherwise("Low Value")
    )

    # 7. Customer segment summary
    segments = (
        customers.groupBy("segment")
        .agg(
            F.count("*").alias("customers"),
            F.sum("orders").alias("orders"),
            F.round(F.sum("spending"), 2).alias("revenue")
        )
    )

    print("\nCUSTOMER SEGMENTATION")
    segments.show(truncate=False)

    # 8. Save Spark results
    customers.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "customer_performance"))

    segments.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "customer_segments"))

    # 9. Collect only three segment summary rows
    rows = segments.collect()

    names = [r["segment"] for r in rows]
    counts = [int(r["customers"]) for r in rows]
    revenues = [float(r["revenue"]) for r in rows]

    # Chart 1: Customers by segment
    plt.figure(figsize=(9, 5))
    plt.bar(names, counts)
    plt.title("Customer Segmentation by Spending")
    plt.xlabel("Customer Segment")
    plt.ylabel("Number of Customers")
    plt.tight_layout()
    plt.savefig(
        charts / "customer_segments.png",
        dpi=200
    )
    plt.close()

    # Chart 2: Revenue by customer segment
    plt.figure(figsize=(9, 5))
    plt.bar(names, revenues)
    plt.title("Revenue by Customer Segment")
    plt.xlabel("Customer Segment")
    plt.ylabel("Revenue (dataset currency units)")
    plt.tight_layout()
    plt.savefig(
        charts / "customer_segment_revenue.png",
        dpi=200
    )
    plt.close()

    print("\nCUSTOMER SEGMENTATION COMPLETED")
    print("Saved customer_segments.png")
    print("Saved customer_segment_revenue.png")

finally:
    spark.stop()
