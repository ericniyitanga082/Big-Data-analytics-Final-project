
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
OUTPUT = ROOT / "outputs"
OUTPUT.mkdir(exist_ok=True)

spark = (
    SparkSession.builder
    .appName("AUCA_Product_Performance")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    transactions = (
        spark.read.option("multiLine", "true")
        .json(str(DATA / "transactions_*.json"))
        .select("transaction_id", "status", "subtotal",
                "discount", "total", "items")
        .filter(F.col("status").isin("completed", "delivered"))
    )

    # Expand each transaction's items into separate rows.
    items = (
        transactions
        .withColumn("item", F.explode("items"))
        .select(
            "transaction_id",
            "subtotal",
            "total",
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.subtotal").alias("item_subtotal")
        )
    )

    # Allocate transaction-level discounts proportionally
    # to each product's share of the order subtotal.
    items = items.withColumn(
        "allocated_revenue",
        F.when(
            F.col("subtotal") > 0,
            F.col("item_subtotal") * F.col("total") / F.col("subtotal")
        ).otherwise(F.lit(0.0))
    )

    products = (
        spark.read.option("multiLine", "true")
        .json(str(DATA / "products.json"))
        .select("product_id", "name", "category_id")
    )

    performance = (
        items.groupBy("product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("transaction_id").alias("orders"),
            F.round(F.sum("allocated_revenue"), 2)
             .alias("revenue")
        )
        .join(F.broadcast(products), "product_id", "left")
    )

    performance.createOrReplaceTempView("product_performance")

    print("\n========== TOP PRODUCTS BY REVENUE ==========")
    spark.sql("""
        SELECT product_id, name, units_sold, revenue
        FROM product_performance
        ORDER BY revenue DESC
        LIMIT 10
    """).show(truncate=False)

    print("\n========== TOP PRODUCTS BY QUANTITY ==========")
    spark.sql("""
        SELECT product_id, name, units_sold, revenue
        FROM product_performance
        ORDER BY units_sold DESC
        LIMIT 10
    """).show(truncate=False)

    performance.coalesce(1).write.mode(
        "overwrite"
    ).option("header", True).csv(
        str(OUTPUT / "product_performance")
    )

    print("\nProduct analysis completed.")

finally:
    spark.stop()
