
from pathlib import Path
import csv
import json

import happybase
from pymongo import MongoClient
from pyspark.sql import SparkSession, functions as F


ROOT = Path(__file__).resolve().parents[2]
STAGING = ROOT / "outputs" / "integration_staging"
OUTPUT = ROOT / "outputs" / "conversion_funnel"

STAGING.mkdir(parents=True, exist_ok=True)

SESSION_FILE = STAGING / "hbase_session_stages.csv"
PURCHASE_FILE = STAGING / "mongo_purchases.csv"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "ecommerce_analytics_full"

QUALIFYING_STATUSES = [
    "completed", "delivered", "shipped"
]


def extract_hbase_sessions():
    """Read actual browsing sessions from HBase."""

    print("\nExtracting HBase browsing sessions...")

    connection = happybase.Connection(
        "localhost",
        port=9091,
        timeout=120000,
        autoconnect=False
    )

    connection.open()

    try:
        table = connection.table("user_activity")
        total = 0

        with SESSION_FILE.open(
            "w", newline="", encoding="utf-8"
        ) as file:

            writer = csv.writer(file)
            writer.writerow([
                "session_id",
                "user_id",
                "device_type",
                "referrer",
                "product_view",
                "has_cart",
                "checkout"
            ])

            for row_key, data in table.scan(
                columns=[
                    b"session:session_id",
                    b"session:user_id",
                    b"session:referrer",
                    b"device:type",
                    b"activity:page_views",
                    b"activity:cart_contents"
                ],
                batch_size=1000
            ):

                def get_value(column):
                    return data.get(column, b"").decode(
                        "utf-8"
                    )

                session_id = get_value(
                    b"session:session_id"
                )

                if not session_id:
                    continue

                raw_views = get_value(
                    b"activity:page_views"
                )

                raw_cart = get_value(
                    b"activity:cart_contents"
                )

                views = (
                    json.loads(raw_views)
                    if raw_views else []
                )

                cart = (
                    json.loads(raw_cart)
                    if raw_cart else {}
                )

                page_types = {
                    item.get("page_type")
                    for item in views
                    if isinstance(item, dict)
                }

                writer.writerow([
                    session_id,
                    get_value(b"session:user_id"),
                    get_value(b"device:type"),
                    get_value(b"session:referrer"),
                    int("product_detail" in page_types),
                    int(bool(cart)),
                    int("checkout" in page_types)
                ])

                total += 1

                if total % 100000 == 0:
                    print(
                        f"HBase sessions extracted: {total:,}",
                        flush=True
                    )

        print(f"Total HBase sessions: {total:,}")

    finally:
        connection.close()


def extract_mongo_purchases():
    """Read qualifying purchases from MongoDB."""

    print("\nExtracting MongoDB purchases...")

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000
    )

    try:
        collection = client[MONGO_DB]["transactions"]

        cursor = collection.find(
            {
                "status": {
                    "$in": QUALIFYING_STATUSES
                },
                "session_id": {
                    "$nin": [None, ""]
                }
            },
            {
                "_id": 0,
                "session_id": 1,
                "transaction_id": 1
            },
            batch_size=5000
        )

        total = 0

        with PURCHASE_FILE.open(
            "w", newline="", encoding="utf-8"
        ) as file:

            writer = csv.writer(file)
            writer.writerow([
                "session_id",
                "transaction_id"
            ])

            for transaction in cursor:
                session_id = transaction.get("session_id")
                transaction_id = transaction.get(
                    "transaction_id"
                )

                if not session_id or not transaction_id:
                    continue

                writer.writerow([
                    session_id,
                    transaction_id
                ])

                total += 1

        print(
            f"Linked qualifying purchases: {total:,}"
        )

    finally:
        client.close()


def calculate_funnel():
    """Join cross-system data using Apache Spark."""

    spark = (
        SparkSession.builder
        .appName("AUCA_Cross_System_Conversion_Funnel")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        sessions = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(SESSION_FILE))
        )

        purchases = (
            spark.read
            .option("header", True)
            .csv(str(PURCHASE_FILE))
            .select("session_id")
            .distinct()
            .withColumn("purchased", F.lit(1))
        )

        # Join HBase browsing with MongoDB purchases.
        joined = sessions.join(
            purchases,
            on="session_id",
            how="left"
        ).fillna({"purchased": 0})

        # Build sequential funnel stages.
        funnel = (
            joined
            .withColumn(
                "stage_view",
                F.col("product_view") == 1
            )
            .withColumn(
                "stage_cart",
                F.col("stage_view")
                & (F.col("has_cart") == 1)
            )
            .withColumn(
                "stage_checkout",
                F.col("stage_cart")
                & (F.col("checkout") == 1)
            )
            .withColumn(
                "stage_purchase",
                F.col("stage_checkout")
                & (F.col("purchased") == 1)
            )
        )

        totals = funnel.agg(
            F.count("*").alias("sessions"),
            F.sum(
                F.col("stage_view").cast("int")
            ).alias("views"),
            F.sum(
                F.col("stage_cart").cast("int")
            ).alias("carts"),
            F.sum(
                F.col("stage_checkout").cast("int")
            ).alias("checkouts"),
            F.sum(
                F.col("stage_purchase").cast("int")
            ).alias("purchases")
        ).first()

        stages = [
            ("All Sessions", totals["sessions"]),
            ("Product Views", totals["views"]),
            ("Shopping Carts", totals["carts"]),
            ("Checkout", totals["checkouts"]),
            ("Completed Purchases", totals["purchases"])
        ]

        rows = []
        previous = None

        for stage, count in stages:
            rate = (
                100.0 * count / previous
                if previous else 100.0
            )

            rows.append((
                stage,
                int(count),
                round(rate, 2)
            ))

            previous = count

        result = spark.createDataFrame(
            rows,
            [
                "stage",
                "sessions",
                "step_conversion_pct"
            ]
        )

        print("\n========== CONVERSION FUNNEL ==========")
        result.show(truncate=False)

        result.coalesce(1).write.mode(
            "overwrite"
        ).option(
            "header", True
        ).csv(str(OUTPUT))

        print("\nSaved funnel results to:", OUTPUT)

    finally:
        spark.stop()


if __name__ == "__main__":
    # Reuse completed extracts on subsequent runs.
    # Delete the staging CSV files if source data changes.
    if not SESSION_FILE.exists():
        extract_hbase_sessions()
    else:
        print("Reusing existing HBase extract.")

    if not PURCHASE_FILE.exists():
        extract_mongo_purchases()
    else:
        print("Reusing existing MongoDB extract.")

    calculate_funnel()
