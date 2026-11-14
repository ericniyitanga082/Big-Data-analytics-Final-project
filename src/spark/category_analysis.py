
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
OUTPUT = ROOT / "outputs"

spark = (
    SparkSession.builder
    .appName("AUCA_Category_Analysis")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    products = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(OUTPUT / "product_performance"))
    )

    categories = (
        spark.read.option("multiLine", "true")
        .json(str(DATA / "categories.json"))
        .select(
            "category_id",
            F.col("name").alias("category_name")
        )
    )

    category_performance = (
        products.join(
            F.broadcast(categories),
            "category_id",
            "left"
        )
        .groupBy("category_id", "category_name")
        .agg(
            F.sum("units_sold").alias("units_sold"),
            F.round(F.sum("revenue"), 2).alias("revenue"),
            F.countDistinct("product_id").alias("products_sold")
        )
    )

    category_performance.createOrReplaceTempView(
        "category_performance"
    )

    print("\n========== REVENUE BY CATEGORY ==========")

    spark.sql("""
        SELECT
            category_id,
            category_name,
            units_sold,
            products_sold,
            revenue
        FROM category_performance
        ORDER BY revenue DESC
    """).show(25, truncate=False)

    print("\n========== CATEGORY REVENUE CONTRIBUTION ==========")

    spark.sql("""
        SELECT
            category_name,
            revenue,
            ROUND(
                100.0 * revenue / SUM(revenue) OVER (),
                2
            ) AS revenue_share_pct
        FROM category_performance
        ORDER BY revenue DESC
    """).show(25, truncate=False)

    category_performance.coalesce(1).write.mode(
        "overwrite"
    ).option("header", True).csv(
        str(OUTPUT / "category_performance")
    )

    print("\nCategory analysis completed.")

finally:
    spark.stop()
