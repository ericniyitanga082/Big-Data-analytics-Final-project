
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. PROJECT PATHS
# ============================================================

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"

OUTPUT = ROOT / "outputs" / "customer_segments"

OUTPUT.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. START SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("AUCA_Customer_Segmentation")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


try:

    # ========================================================
    # 3. LOAD TRANSACTIONS
    # ========================================================

    transactions = (
        spark.read
        .option("multiLine", "true")
        .json(str(DATA / "transactions_*.json"))
    )

    # Keep transactions that qualify for revenue analysis.

    purchases = (
        transactions
        .filter(
            F.col("status").isin(
                "completed",
                "delivered",
                "shipped"
            )
        )
        .filter(
            F.col("user_id").isNotNull()
            & F.col("transaction_id").isNotNull()
            & F.col("total").isNotNull()
        )
        .withColumn(
            "purchase_date",
            F.to_date(F.col("timestamp"))
        )
        .filter(
            F.col("purchase_date").isNotNull()
        )
        .dropDuplicates(["transaction_id"])
    )

    # ========================================================
    # 4. REFERENCE DATE
    # ========================================================

    # One day after the most recent transaction.
    # This makes the analysis reproducible.

    reference_date = (
        purchases
        .agg(
            F.date_add(
                F.max("purchase_date"), 1
            ).alias("reference_date")
        )
        .first()["reference_date"]
    )

    if reference_date is None:
        raise ValueError(
            "No qualifying purchases found."
        )

    # ========================================================
    # 5. CALCULATE RFM METRICS
    # ========================================================

    rfm = (
        purchases
        .groupBy("user_id")
        .agg(
            F.max("purchase_date").alias(
                "last_purchase"
            ),

            F.countDistinct(
                "transaction_id"
            ).alias("frequency"),

            F.round(
                F.sum("total"), 2
            ).alias("monetary")
        )
        .withColumn(
            "recency",
            F.datediff(
                F.lit(reference_date),
                F.col("last_purchase")
            )
        )
    )

    # ========================================================
    # 6. PERCENTILE-BASED RFM SCORING
    # ========================================================

    # Scores range from 1 to 5.
    #
    # Recency:
    # More recent purchases receive higher scores.
    #
    # Frequency:
    # More purchases receive higher scores.
    #
    # Monetary:
    # Higher spending receives higher scores.
    #
    # user_id provides reproducible tie-breaking.

    recency_window = Window.orderBy(
        F.col("recency").asc(),
        F.col("user_id").asc()
    )

    frequency_window = Window.orderBy(
        F.col("frequency").asc(),
        F.col("user_id").asc()
    )

    monetary_window = Window.orderBy(
        F.col("monetary").asc(),
        F.col("user_id").asc()
    )

    scored = (
        rfm
        .withColumn(
            "r_score",
            6 - F.ntile(5).over(recency_window)
        )
        .withColumn(
            "f_score",
            F.ntile(5).over(frequency_window)
        )
        .withColumn(
            "m_score",
            F.ntile(5).over(monetary_window)
        )
    )

    # ========================================================
    # 7. CUSTOMER SEGMENTATION
    # ========================================================

    segmented = (
        scored
        .withColumn(
            "segment",

            F.when(
                (F.col("r_score") >= 4)
                & (F.col("f_score") >= 4)
                & (F.col("m_score") >= 4),
                "Champions"
            )

            .when(
                (F.col("f_score") >= 4)
                & (F.col("m_score") >= 4),
                "High-Value Customers"
            )

            .when(
                (F.col("r_score") >= 4)
                & (F.col("f_score") <= 2),
                "New or Occasional"
            )

            .when(
                F.col("r_score") <= 2,
                "At Risk"
            )

            .otherwise(
                "Regular Customers"
            )
        )
    )

    # ========================================================
    # 8. SEGMENT SUMMARY
    # ========================================================

    summary = (
        segmented
        .groupBy("segment")
        .agg(
            F.count("*").alias("customers"),

            F.round(
                F.avg("monetary"), 2
            ).alias("avg_customer_spend"),

            F.round(
                F.avg("frequency"), 2
            ).alias("avg_purchases"),

            F.round(
                F.sum("monetary"), 2
            ).alias("segment_revenue")
        )
        .orderBy(
            F.desc("segment_revenue")
        )
    )

    # ========================================================
    # 9. DISPLAY RESULTS
    # ========================================================

    print(
        "\n========== CUSTOMER SEGMENTATION =========="
    )

    print(
        "Reference date:", reference_date
    )

    print(
        "Customers analyzed:", segmented.count()
    )

    print(
        "\n========== SEGMENT SUMMARY =========="
    )

    summary.show(
        truncate=False
    )

    print(
        "\n========== RFM SCORE DISTRIBUTION =========="
    )

    segmented.groupBy(
        "r_score"
    ).count().orderBy(
        "r_score"
    ).show()

    # ========================================================
    # 10. SAVE RESULTS
    # ========================================================

    summary.coalesce(1).write.mode(
        "overwrite"
    ).option(
        "header", True
    ).csv(
        str(OUTPUT)
    )

    # Save individual customer scores separately.
    # These are useful for additional business analysis.

    customer_output = (
        ROOT / "outputs" / "customer_rfm_details"
    )

    segmented.select(
        "user_id",
        "last_purchase",
        "recency",
        "frequency",
        "monetary",
        "r_score",
        "f_score",
        "m_score",
        "segment"
    ).write.mode(
        "overwrite"
    ).option(
        "header", True
    ).csv(
        str(customer_output)
    )

    print(
        "\nCustomer segmentation completed."
    )

    print(
        "Summary saved to:", OUTPUT
    )

    print(
        "Customer details saved to:",
        customer_output
    )

finally:

    spark.stop()
