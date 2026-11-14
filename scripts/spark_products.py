
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, ArrayType
)

root = Path(__file__).resolve().parent.parent
files = sorted((root / "data").glob("transactions_*.json"))
output = root / "outputs" / "spark_products"
output.mkdir(parents=True, exist_ok=True)

if not files:
    raise FileNotFoundError("No transaction files found")

# Explicit schema avoids unnecessary inference
item_schema = StructType([
    StructField("product_id", StringType()),
    StructField("quantity", LongType()),
    StructField("subtotal", DoubleType()),
    StructField("unit_price", DoubleType())
])

schema = StructType([
    StructField("status", StringType()),
    StructField("items", ArrayType(item_schema))
])

spark = (
    SparkSession.builder
    .appName("AUCA_Product_Analytics")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # Save partial results per file
    partial_dir = output / "partial"
    partial_dir.mkdir(exist_ok=True)

    for i, file in enumerate(files, 1):
        df = (
            spark.read.schema(schema)
            .option("multiLine", "true")
            .json(str(file))
        )

        items = (
            df.filter(F.col("status").isin(
                "completed", "delivered"
            ))
            .select(F.explode("items").alias("item"))
            .select(
                F.col("item.product_id").alias("product_id"),
                F.col("item.quantity").alias("quantity"),
                F.col("item.subtotal").alias("revenue")
            )
            .filter(F.col("product_id").isNotNull())
        )

        aggregated = (
            items.groupBy("product_id")
            .agg(
                F.sum("quantity").alias("units_sold"),
                F.round(F.sum("revenue"), 2).alias("revenue")
            )
        )

        aggregated.coalesce(1).write.mode("overwrite") \
            .parquet(str(partial_dir / f"batch_{i:03d}"))

        print(f"Processed file {i}/{len(files)}", flush=True)

    # Combine compact partial aggregates
    combined = spark.read.parquet(
        str(partial_dir / "batch_*")
    )

    products = (
        combined.groupBy("product_id")
        .agg(
            F.sum("units_sold").alias("units_sold"),
            F.round(F.sum("revenue"), 2).alias("revenue")
        )
    )

    top_products = products.orderBy(
        F.desc("revenue")
    ).limit(10)

    print("\nTOP 10 PRODUCTS BY REVENUE")
    top_products.show(10, truncate=False)

    # Save complete product performance
    products.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "product_performance"))

    # Save top 10
    top_products.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "top_10_products"))

    print("\nPRODUCT ANALYTICS COMPLETED SUCCESSFULLY")
    print(f"Results saved in: {output}")

finally:
    spark.stop()
