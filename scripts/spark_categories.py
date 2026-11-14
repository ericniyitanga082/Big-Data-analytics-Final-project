
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt

# Project directories
root = Path(__file__).resolve().parent.parent
data = root / "data"
output = root / "outputs"
charts = output / "charts"

charts.mkdir(parents=True, exist_ok=True)

# Start Spark with low RAM usage
spark = (
    SparkSession.builder
    .appName("AUCA_Category_Analytics")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # 1. Load saved product performance
    performance = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(
            output / "spark_products" / "product_performance"
        ))
    )

    # 2. Load product information
    products = (
        spark.read
        .option("multiLine", "true")
        .json(str(data / "products.json"))
        .select(
            "product_id",
            F.col("name").alias("product_name"),
            "category_id"
        )
    )

    # 3. Load category information
    categories = (
        spark.read
        .option("multiLine", "true")
        .json(str(data / "categories.json"))
        .select(
            "category_id",
            F.col("name").alias("category_name")
        )
    )

    # 4. Join product sales with product information
    enriched = (
        performance
        .join(products, "product_id", "left")
        .join(categories, "category_id", "left")
    )

    # 5. Calculate category performance
    category_sales = (
        enriched
        .groupBy("category_id", "category_name")
        .agg(
            F.sum("units_sold").alias("units_sold"),
            F.round(F.sum("revenue"), 2).alias("revenue")
        )
        .orderBy(F.desc("revenue"))
    )

    print("\nCATEGORY SALES PERFORMANCE")
    category_sales.show(25, truncate=False)

    # 6. Save category analytics
    category_sales.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "spark_products" / "category_performance"))

    # 7. Save enriched product information
    enriched.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "spark_products" / "enriched_products"))

    # 8. Generate category revenue chart
    rows = category_sales.limit(10).collect()

    names = [
        row["category_name"] or row["category_id"]
        for row in rows
    ]
    values = [float(row["revenue"]) for row in rows]

    plt.figure(figsize=(12, 6))
    plt.barh(names[::-1], values[::-1])
    plt.title("Top 10 Categories by Revenue")
    plt.xlabel("Item Revenue Before Order Discounts")
    plt.ylabel("Category")
    plt.tight_layout()

    plt.savefig(
        charts / "category_revenue.png",
        dpi=200,
        bbox_inches="tight"
    )
    plt.close()

    print("\nCATEGORY ANALYTICS COMPLETED SUCCESSFULLY")
    print("Chart saved:", charts / "category_revenue.png")

finally:
    spark.stop()
