
"""
AUCA BIG DATA ANALYTICS FINAL PROJECT
Distributed Multi-Model Analytics for E-commerce Data

File: spark_analytics.py

Run:
    python spark_analytics.py

Optional:
    python spark_analytics.py --data-dir data --output-dir outputs/spark
    python spark_analytics.py --data-dir data/dev --output-dir outputs/dev

Requirements:
    pip install pyspark

This script reads the generator's JSON files, processes them with
Apache Spark, and exports analytical results for visualization.

NOTE:
- The dataset is synthetic.
- Revenue is defined here using completed/delivered transactions.
- Funnel purchases require completed transactions linked to sessions.
- The script does not claim to read directly from MongoDB or HBase.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


# ============================================================
# 1. CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("AUCA_Ecommerce")

ROOT = Path(__file__).resolve().parent.parent

REVENUE_STATUSES = ["completed", "delivered"]
PURCHASE_STATUS = "completed"


def parse_args():
    parser = argparse.ArgumentParser(
        description="AUCA E-commerce Spark Analytics"
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Directory containing generated JSON files",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/spark",
        help="Directory for analytical CSV results",
    )
    parser.add_argument(
        "--master",
        default="local[2]",
        help="Spark execution master",
    )
    return parser.parse_args()


def resolve_data_dir(value):
    if value:
        path = Path(value)
        if not path.is_absolute():
            path = ROOT / path
        return path.resolve()

    candidates = [
        ROOT / "data",
        ROOT / "data" / "full",
        ROOT / "data" / "production",
        ROOT / "data" / "dev",
    ]

    required = [
        "users.json",
        "products.json",
        "categories.json",
        "transactions.json",
    ]

    for path in candidates:
        if (
            all((path / name).is_file() for name in required)
            and list(path.glob("sessions_*.json"))
        ):
            return path.resolve()

    raise FileNotFoundError(
        "No complete dataset found. "
        "Specify --data-dir explicitly."
    )


# ============================================================
# 2. EXPLICIT SPARK SCHEMAS
# ============================================================

# These schemas prevent Spark from treating every product ID
# inside cart_contents as a separate STRUCT column.

geo_schema = T.StructType([
    T.StructField("city", T.StringType(), True),
    T.StructField("state", T.StringType(), True),
    T.StructField("country", T.StringType(), True),
])

device_schema = T.StructType([
    T.StructField("type", T.StringType(), True),
    T.StructField("os", T.StringType(), True),
    T.StructField("browser", T.StringType(), True),
])

page_view_schema = T.StructType([
    T.StructField("timestamp", T.StringType(), True),
    T.StructField("page_type", T.StringType(), True),
    T.StructField("product_id", T.StringType(), True),
    T.StructField("category_id", T.StringType(), True),
    T.StructField("view_duration", T.LongType(), True),
])

cart_item_schema = T.StructType([
    T.StructField("price", T.DoubleType(), True),
    T.StructField("quantity", T.LongType(), True),
])

session_schema = T.StructType([
    T.StructField("session_id", T.StringType(), True),
    T.StructField("user_id", T.StringType(), True),
    T.StructField("start_time", T.StringType(), True),
    T.StructField("end_time", T.StringType(), True),
    T.StructField("duration_seconds", T.LongType(), True),
    T.StructField("geo_data", geo_schema, True),
    T.StructField("device_profile", device_schema, True),
    T.StructField("referrer", T.StringType(), True),
    T.StructField("conversion_status", T.StringType(), True),
    T.StructField(
        "page_views",
        T.ArrayType(page_view_schema),
        True,
    ),
    T.StructField(
        "viewed_products",
        T.ArrayType(T.StringType()),
        True,
    ),
    T.StructField(
        "cart_contents",
        T.MapType(
            T.StringType(),
            cart_item_schema,
            True,
        ),
        True,
    ),
])

transaction_item_schema = T.StructType([
    T.StructField("product_id", T.StringType(), True),
    T.StructField("quantity", T.LongType(), True),
    T.StructField("unit_price", T.DoubleType(), True),
    T.StructField("subtotal", T.DoubleType(), True),
])

transaction_schema = T.StructType([
    T.StructField("transaction_id", T.StringType(), True),
    T.StructField("session_id", T.StringType(), True),
    T.StructField("user_id", T.StringType(), True),
    T.StructField("timestamp", T.StringType(), True),
    T.StructField(
        "items",
        T.ArrayType(transaction_item_schema),
        True,
    ),
    T.StructField("subtotal", T.DoubleType(), True),
    T.StructField("discount", T.DoubleType(), True),
    T.StructField("total", T.DoubleType(), True),
    T.StructField("payment_method", T.StringType(), True),
    T.StructField("status", T.StringType(), True),
])

product_schema = T.StructType([
    T.StructField("product_id", T.StringType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("category_id", T.StringType(), True),
    T.StructField("base_price", T.DoubleType(), True),
    T.StructField("current_stock", T.LongType(), True),
    T.StructField("is_active", T.BooleanType(), True),
    T.StructField(
        "price_history",
        T.ArrayType(
            T.StructType([
                T.StructField("date", T.StringType(), True),
                T.StructField("price", T.DoubleType(), True),
            ])
        ),
        True,
    ),
    T.StructField("creation_date", T.StringType(), True),
])

user_schema = T.StructType([
    T.StructField("user_id", T.StringType(), True),
    T.StructField("geo_data", geo_schema, True),
    T.StructField("registration_date", T.StringType(), True),
    T.StructField("last_active", T.StringType(), True),
])

subcategory_schema = T.StructType([
    T.StructField("subcategory_id", T.StringType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("profit_margin", T.DoubleType(), True),
])

category_schema = T.StructType([
    T.StructField("category_id", T.StringType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField(
        "subcategories",
        T.ArrayType(subcategory_schema),
        True,
    ),
])


# ============================================================
# 3. SPARK SESSION
# ============================================================

def create_spark(master):
    spark = (
        SparkSession.builder
        .appName("AUCA_Ecommerce_Analytics")
        .master(master)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.files.maxPartitionBytes", "32m")
        .config("spark.driver.maxResultSize", "256m")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ============================================================
# 4. DATA LOADING
# ============================================================

def load_json(spark, path, schema):
    if not path.is_file():
        raise FileNotFoundError(f"Missing file: {path}")

    return (
        spark.read
        .schema(schema)
        .option("multiLine", "true")
        .json(str(path))
    )


def load_data(spark, data_dir):
    logger.info("Reading dataset from %s", data_dir)

    users = load_json(
        spark,
        data_dir / "users.json",
        user_schema,
    )

    products = load_json(
        spark,
        data_dir / "products.json",
        product_schema,
    )

    categories = load_json(
        spark,
        data_dir / "categories.json",
        category_schema,
    )

    # Load transaction files in chunks
    transaction_files = sorted(data_dir.glob("transactions_*.json"))

    if transaction_files:
        logger.info("Found %d transaction files", len(transaction_files))

        transactions = (
        spark.read
        .schema(transaction_schema)
        .option("multiLine", "true")
        .json([str(f) for f in transaction_files])
    )

    elif (data_dir / "transactions.json").exists():
        transactions = load_json(
            spark,
            data_dir / "transactions.json",
            transaction_schema,
        )

    else:
        raise FileNotFoundError(
            f"No transaction JSON files found in {data_dir}"
        )

    session_files = sorted(
        data_dir.glob("sessions_*.json"),
        key=lambda p: (
            int(p.stem.split("_")[-1])
            if p.stem.split("_")[-1].isdigit()
            else 999999
        ),
    )

    if not session_files:
        raise FileNotFoundError(
            f"No sessions_*.json files in {data_dir}"
        )

    logger.info(
        "Found %s session files",
        len(session_files),
    )

    # All session files have the same explicitly defined schema.
    # Reading together avoids a long unionByName chain.
    sessions = (
        spark.read
        .schema(session_schema)
        .option("multiLine", "true")
        .json([str(p) for p in session_files])
    )

    return users, products, categories, transactions, sessions


# ============================================================
# 5. CLEANING AND NORMALIZATION
# ============================================================

def clean_data(users, products, categories, transactions, sessions):

    users = (
        users
        .filter(F.col("user_id").isNotNull())
        .dropDuplicates(["user_id"])
        .withColumn(
            "registration_ts",
            F.to_timestamp("registration_date"),
        )
    )

    products = (
        products
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
        .withColumn(
            "base_price",
            F.col("base_price").cast("double"),
        )
    )

    categories = (
        categories
        .filter(F.col("category_id").isNotNull())
        .dropDuplicates(["category_id"])
        .select(
            "category_id",
            F.col("name").alias("category_name"),
            "subcategories",
        )
    )

    transactions = (
        transactions
        .filter(F.col("transaction_id").isNotNull())
        .dropDuplicates(["transaction_id"])
        .withColumn(
            "transaction_ts",
            F.to_timestamp("timestamp"),
        )
        .withColumn(
            "transaction_date",
            F.to_date("transaction_ts"),
        )
        .withColumn(
            "total",
            F.col("total").cast("double"),
        )
    )

    sessions = (
        sessions
        .filter(F.col("session_id").isNotNull())
        .dropDuplicates(["session_id"])
        .withColumn(
            "session_ts",
            F.to_timestamp("start_time"),
        )
        .withColumn(
            "session_date",
            F.to_date("session_ts"),
        )
        .withColumn(
            "device_type",
            F.coalesce(
                F.col("device_profile.type"),
                F.lit("unknown"),
            ),
        )
        .withColumn(
            "referrer",
            F.coalesce(
                F.col("referrer"),
                F.lit("unknown"),
            ),
        )
    )

    return users, products, categories, transactions, sessions


# ============================================================
# 6. OUTPUT HELPER
# ============================================================

def export_csv(df, output_dir, name):
    destination = output_dir / name

    logger.info("Exporting %s", name)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(destination))
    )


def show_result(title, df, rows=10):
    print("\n" + "=" * 65)
    print(title)
    print("=" * 65)
    df.show(rows, truncate=False)


# ============================================================
# 7. DATA QUALITY VALIDATION
# ============================================================

def validate_data(
    users,
    products,
    categories,
    transactions,
    sessions,
):
    print("\nDATA QUALITY VALIDATION")

    session_metrics = sessions.agg(
        F.count("*").alias("total_sessions"),
        F.sum(
            F.when(F.col("session_ts").isNull(), 1).otherwise(0)
        ).alias("missing_session_dates")
    ).first()

    counts = {
        "users": users.count(),
        "products": products.count(),
        "categories": categories.count(),
        "transactions": transactions.count(),
        "sessions": session_metrics["total_sessions"],
    }

    for name, count in counts.items():
        print(f"{name}: {count:,}")

    missing_transaction_dates = transactions.filter(
        F.col("transaction_ts").isNull()
    ).count()

    missing_session_dates = session_metrics["missing_session_dates"]

    negative_transaction_totals = transactions.filter(
        F.col("total") < 0
    ).count()
    missing_transaction_users = (
        transactions
        .select("user_id")
        .where(F.col("user_id").isNotNull())
        .distinct()
        .join(
            users.select("user_id"),
            "user_id",
            "left_anti",
        )
        .count()
    )

    missing_product_categories = (
        products
        .select("category_id")
        .where(F.col("category_id").isNotNull())
        .distinct()
        .join(
            categories.select("category_id"),
            "category_id",
            "left_anti",
        )
        .count()
    )

    linked_transactions = transactions.filter(
        F.col("session_id").isNotNull()
    ).count()

    unlinked_transactions = (
        counts["transactions"] - linked_transactions
    )

    quality = {
        "missing_transaction_dates": missing_transaction_dates,
        "missing_session_dates": missing_session_dates,
        "negative_transaction_totals": negative_transaction_totals,
        "missing_transaction_user_ids": missing_transaction_users,
        "missing_product_category_ids": missing_product_categories,
        "linked_transactions": linked_transactions,
        "unlinked_transactions": unlinked_transactions,
    }

    for name, value in quality.items():
        print(f"{name}: {value:,}")

    return counts, quality


# ============================================================
# 8. TRANSACTION LINE ITEMS
# ============================================================

def create_transaction_items(transactions):

    valid_transactions = transactions.filter(
        F.col("status").isin(*REVENUE_STATUSES)
    )

    items = (
        valid_transactions
        .select(
            "transaction_id",
            "user_id",
            "session_id",
            "transaction_date",
            "status",
            "total",
            "subtotal",
            "discount",
            F.explode_outer("items").alias("item"),
        )
        .filter(F.col("item").isNotNull())
        .select(
            "transaction_id",
            "user_id",
            "session_id",
            "transaction_date",
            "status",
            "total",
            "subtotal",
            "discount",
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.unit_price").alias("unit_price"),
            F.col("item.subtotal").alias("line_subtotal"),
        )
    )

    return valid_transactions, items


# ============================================================
# 9. REVENUE OVER TIME
# ============================================================

def revenue_over_time(valid_transactions):

    return (
        valid_transactions
        .filter(
            F.col("transaction_date").isNotNull()
            & F.col("total").isNotNull()
        )
        .groupBy("transaction_date")
        .agg(
            F.countDistinct("transaction_id").alias(
                "transactions"
            ),
            F.round(
                F.sum("total"), 2
            ).alias("revenue"),
            F.round(
                F.avg("total"), 2
            ).alias("average_order_value"),
        )
        .orderBy("transaction_date")
    )


# ============================================================
# 10. PRODUCT SALES PERFORMANCE
# ============================================================

def product_performance(items, products, categories):

    product_info = (
        products
        .select(
            "product_id",
            F.col("name").alias("product_name"),
            "category_id",
        )
        .join(
            F.broadcast(
                categories.select(
                    "category_id",
                    "category_name",
                )
            ),
            "category_id",
            "left",
        )
    )

    return (
        items
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("transaction_id").alias(
                "orders"
            ),
            F.round(
                F.sum("line_subtotal"), 2
            ).alias("gross_item_sales"),
        )
        .join(
            F.broadcast(product_info),
            "product_id",
            "left",
        )
        .orderBy(F.desc("gross_item_sales"))
    )


# ============================================================
# 11. CATEGORY SALES PERFORMANCE
# ============================================================

def category_performance(product_sales):

    return (
        product_sales
        .groupBy(
            "category_id",
            "category_name",
        )
        .agg(
            F.sum("units_sold").alias("units_sold"),
            F.round(
                F.sum("gross_item_sales"), 2
            ).alias("gross_item_sales"),
            F.sum("orders").alias(
                "sum_product_order_counts"
            ),
        )
        .orderBy(F.desc("gross_item_sales"))
    )


# ============================================================
# 12. RFM CUSTOMER SEGMENTATION
# ============================================================

def customer_rfm(valid_transactions):

    reference_date = (
        valid_transactions
        .agg(
            F.date_add(
                F.max("transaction_date"), 1
            ).alias("reference_date")
        )
        .first()["reference_date"]
    )

    if reference_date is None:
        raise ValueError(
            "No valid transaction dates for RFM analysis."
        )

    rfm = (
        valid_transactions
        .filter(
            F.col("user_id").isNotNull()
            & F.col("transaction_date").isNotNull()
        )
        .groupBy("user_id")
        .agg(
            F.max("transaction_date").alias(
                "last_purchase_date"
            ),
            F.countDistinct("transaction_id").alias(
                "frequency"
            ),
            F.round(
                F.sum("total"), 2
            ).alias("monetary"),
        )
        .withColumn(
            "recency_days",
            F.datediff(
                F.lit(reference_date),
                F.col("last_purchase_date"),
            ),
        )
    )

    # Business-oriented segmentation with explicit rules.
    # These are illustrative analytical thresholds,
    # not learned customer classifications.

    rfm = (
        rfm
        .withColumn(
            "customer_segment",
            F.when(
                (F.col("frequency") >= 5)
                & (F.col("recency_days") <= 30),
                "Loyal Active",
            )
            .when(
                (F.col("frequency") >= 3)
                & (F.col("recency_days") > 30),
                "At Risk",
            )
            .when(
                (F.col("frequency") == 1)
                & (F.col("recency_days") <= 30),
                "New or One-Time Active",
            )
            .when(
                F.col("recency_days") > 60,
                "Dormant",
            )
            .otherwise("Regular"),
        )
    )

    segment_summary = (
        rfm
        .groupBy("customer_segment")
        .agg(
            F.count("*").alias("customers"),
            F.round(
                F.avg("frequency"), 2
            ).alias("avg_purchase_frequency"),
            F.round(
                F.avg("monetary"), 2
            ).alias("avg_customer_spend"),
            F.round(
                F.sum("monetary"), 2
            ).alias("total_customer_spend"),
        )
        .orderBy(F.desc("customers"))
    )

    return rfm, segment_summary


# ============================================================
# 13. SESSION FUNNEL
# ============================================================

def conversion_funnel(sessions, transactions):

    # Purchase is confirmed only when a completed transaction
    # is linked to a session ID.

    purchased_sessions = (
        transactions
        .filter(
            (F.col("status") == PURCHASE_STATUS)
            & F.col("session_id").isNotNull()
        )
        .select("session_id")
        .distinct()
        .withColumn("purchased", F.lit(True))
    )

    session_features = (
        sessions
        .select(
            "session_id",
            "user_id",
            "device_type",
            "referrer",
            "conversion_status",
            F.expr(
                """
                coalesce(
                    exists(
                        page_views,
                        x -> x.page_type = 'product_detail'
                    ),
                    false
                )
                """
            ).alias("product_view"),
            F.expr(
                """
                coalesce(
                    size(map_keys(cart_contents)) > 0,
                    false
                )
                """
            ).alias("cart_added"),
            F.expr(
                """
                coalesce(
                    exists(
                        page_views,
                        x -> x.page_type = 'checkout'
                    ),
                    false
                )
                """
            ).alias("checkout"),
        )
        .join(
            purchased_sessions,
            "session_id",
            "left",
        )
        .withColumn(
            "purchased",
            F.coalesce(
                F.col("purchased"),
                F.lit(False),
            ),
        )
    )

    # Enforce a consistent, sequential funnel.
    # Purchase count is based on linked completed transactions.
    session_features = (
        session_features
        .withColumn(
            "funnel_product",
            F.col("product_view"),
        )
        .withColumn(
            "funnel_cart",
            F.col("product_view")
            & F.col("cart_added"),
        )
        .withColumn(
            "funnel_checkout",
            F.col("product_view")
            & F.col("cart_added")
            & F.col("checkout"),
        )
        .withColumn(
            "funnel_purchase",
            F.col("product_view")
            & F.col("cart_added")
            & F.col("checkout")
            & F.col("purchased"),
        )
    )

    counts = (
        session_features
        .agg(
            F.count("*").alias("all_sessions"),
            F.sum(
                F.col("funnel_product").cast("long")
            ).alias("product_views"),
            F.sum(
                F.col("funnel_cart").cast("long")
            ).alias("carts"),
            F.sum(
                F.col("funnel_checkout").cast("long")
            ).alias("checkouts"),
            F.sum(
                F.col("funnel_purchase").cast("long")
            ).alias("purchases"),
        )
        .first()
    )

    stages = [
        ("All Sessions", counts["all_sessions"]),
        ("Product View", counts["product_views"]),
        ("Cart", counts["carts"]),
        ("Checkout", counts["checkouts"]),
        ("Purchase", counts["purchases"]),
    ]

    total = counts["all_sessions"] or 0

    funnel_rows = []
    previous = None

    for stage, count in stages:
        count = int(count or 0)

        overall_rate = (
            round(count * 100.0 / total, 2)
            if total else 0.0
        )

        step_rate = (
            round(count * 100.0 / previous, 2)
            if previous else (
                100.0 if stage == "All Sessions" else 0.0
            )
        )

        funnel_rows.append(
            (
                stage,
                count,
                overall_rate,
                step_rate,
            )
        )

        previous = count

    spark = sessions.sparkSession

    funnel_schema = T.StructType([
        T.StructField("stage", T.StringType(), False),
        T.StructField("sessions", T.LongType(), False),
        T.StructField(
            "percent_of_all_sessions",
            T.DoubleType(),
            False,
        ),
        T.StructField(
            "percent_of_previous_stage",
            T.DoubleType(),
            False,
        ),
    ])

    funnel = spark.createDataFrame(
        funnel_rows,
        schema=funnel_schema,
    )

    return session_features, funnel


# ============================================================
# 14. DEVICE AND REFERRER CONVERSION
# ============================================================

def conversion_by_dimension(session_features, dimension):

    return (
        session_features
        .groupBy(dimension)
        .agg(
            F.count("*").alias("sessions"),
            F.sum(
                F.col("purchased").cast("long")
            ).alias("completed_purchase_sessions"),
        )
        .withColumn(
            "conversion_rate_pct",
            F.when(
                F.col("sessions") > 0,
                F.round(
                    F.col("completed_purchase_sessions")
                    * 100.0
                    / F.col("sessions"),
                    2,
                ),
            ).otherwise(F.lit(0.0)),
        )
        .orderBy(F.desc("sessions"))
    )


# ============================================================
# 15. PRODUCT VIEW PERFORMANCE
# ============================================================

def product_views(sessions, products):

    views = (
        sessions
        .select(
            "session_id",
            F.explode_outer("page_views").alias("view"),
        )
        .filter(
            (F.col("view.page_type") == "product_detail")
            & F.col("view.product_id").isNotNull()
        )
        .select(
            "session_id",
            F.col("view.product_id").alias("product_id"),
        )
        .groupBy("product_id")
        .agg(
            F.count("*").alias("page_views"),
            F.countDistinct("session_id").alias(
                "viewing_sessions"
            ),
        )
    )

    return (
        views
        .join(
            F.broadcast(
                products.select(
                    "product_id",
                    F.col("name").alias("product_name"),
                )
            ),
            "product_id",
            "left",
        )
        .orderBy(F.desc("page_views"))
    )


# ============================================================
# 16. PRODUCT AFFINITY / RECOMMENDATION INDICATOR
# ============================================================

def product_affinity(items):

    # Pair products purchased in the same transaction.
    # This is a co-purchase indicator, not a trained ML model.

    unique_items = (
        items
        .select(
            "transaction_id",
            "product_id",
        )
        .filter(F.col("product_id").isNotNull())
        .distinct()
    )

    a = unique_items.alias("a")
    b = unique_items.alias("b")

    pairs = (
        a.join(
            b,
            (F.col("a.transaction_id")
             == F.col("b.transaction_id"))
            & (
                F.col("a.product_id")
                < F.col("b.product_id")
            ),
            "inner",
        )
        .groupBy(
            F.col("a.product_id").alias("product_a"),
            F.col("b.product_id").alias("product_b"),
        )
        .agg(
            F.count("*").alias("co_purchase_count")
        )
        .orderBy(F.desc("co_purchase_count"))
    )

    return pairs


# ============================================================
# 17. SPARK SQL BUSINESS QUERY
# ============================================================

def spark_sql_analysis(spark, valid_transactions, users):

    valid_transactions.createOrReplaceTempView(
        "valid_transactions"
    )

    users.createOrReplaceTempView("users")

    query = """
        SELECT
            COALESCE(u.geo_data.country, 'Unknown')
                AS country,
            COUNT(DISTINCT t.transaction_id)
                AS transaction_count,
            COUNT(DISTINCT t.user_id)
                AS purchasing_customers,
            ROUND(SUM(t.total), 2)
                AS total_revenue,
            ROUND(AVG(t.total), 2)
                AS average_order_value
        FROM valid_transactions t
        LEFT JOIN users u
            ON t.user_id = u.user_id
        WHERE t.transaction_date IS NOT NULL
        GROUP BY
            COALESCE(u.geo_data.country, 'Unknown')
        ORDER BY total_revenue DESC
    """

    return spark.sql(query)


# ============================================================
# 18. INTEGRATED SESSION / TRANSACTION ANALYSIS
# ============================================================

def integrated_analytics(
    session_features,
    transactions,
):

    # This joins browsing activity and completed purchases
    # using session_id. It can later consume data exported
    # from HBase and MongoDB, but here uses source JSON.

    transaction_summary = (
        transactions
        .filter(
            (F.col("status") == PURCHASE_STATUS)
            & F.col("session_id").isNotNull()
        )
        .groupBy("session_id")
        .agg(
            F.countDistinct("transaction_id").alias(
                "completed_transactions"
            ),
            F.round(
                F.sum("total"), 2
            ).alias("completed_revenue"),
        )
    )

    integrated = (
        session_features
        .join(
            transaction_summary,
            "session_id",
            "left",
        )
        .fillna({
            "completed_transactions": 0,
            "completed_revenue": 0.0,
        })
    )

    summary = (
        integrated
        .groupBy(
            "device_type",
            "referrer",
        )
        .agg(
            F.count("*").alias("sessions"),
            F.sum(
                F.col("purchased").cast("long")
            ).alias("purchasing_sessions"),
            F.round(
                F.sum("completed_revenue"), 2
            ).alias("linked_completed_revenue"),
        )
        .withColumn(
            "conversion_rate_pct",
            F.round(
                F.col("purchasing_sessions")
                * 100.0
                / F.col("sessions"),
                2,
            ),
        )
        .orderBy(
            F.desc("linked_completed_revenue")
        )
    )

    return summary


# ============================================================
# 19. MAIN PIPELINE
# ============================================================

def main():

    args = parse_args()

    data_dir = resolve_data_dir(args.data_dir)

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = ROOT / output_dir
    output_dir = output_dir.resolve()

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    logger.info("Data directory: %s", data_dir)
    logger.info("Output directory: %s", output_dir)

    spark = None

    try:
        spark = create_spark(args.master)

        (
            users,
            products,
            categories,
            transactions,
            sessions,
        ) = load_data(spark, data_dir)

        (
            users,
            products,
            categories,
            transactions,
            sessions,
        ) = clean_data(
            users,
            products,
            categories,
            transactions,
            sessions,
        )

        # Avoid repeated parsing of session JSON files.
        sessions = sessions.persist()
        transactions = transactions.persist()

        validate_data(
            users,
            products,
            categories,
            transactions,
            sessions,
        )

        valid_transactions, items = (
            create_transaction_items(transactions)
        )

        # Revenue analysis
        revenue = revenue_over_time(
            valid_transactions
        )

        show_result(
            "REVENUE OVER TIME",
            revenue,
        )

        export_csv(
            revenue,
            output_dir,
            "revenue_over_time",
        )

        # Product sales
        product_sales = product_performance(
            items,
            products,
            categories,
        )

        show_result(
            "TOP SELLING PRODUCTS",
            product_sales,
        )

        export_csv(
            product_sales,
            output_dir,
            "product_performance",
        )

        # Category sales
        category_sales = category_performance(
            product_sales
        )

        show_result(
            "CATEGORY PERFORMANCE",
            category_sales,
        )

        export_csv(
            category_sales,
            output_dir,
            "category_performance",
        )

        # Customer segmentation
        rfm, segments = customer_rfm(
            valid_transactions
        )

        show_result(
            "CUSTOMER RFM SEGMENTS",
            segments,
        )

        export_csv(
            rfm,
            output_dir,
            "customer_rfm",
        )

        export_csv(
            segments,
            output_dir,
            "customer_segments",
        )

        # Conversion funnel
        session_features, funnel = (
            conversion_funnel(
                sessions,
                transactions,
            )
        )

        session_features = session_features.persist()

        show_result(
            "ECOMMERCE CONVERSION FUNNEL",
            funnel,
        )

        export_csv(
            funnel,
            output_dir,
            "conversion_funnel",
        )

        # Device performance
        device_performance = (
            conversion_by_dimension(
                session_features,
                "device_type",
            )
        )

        show_result(
            "CONVERSION BY DEVICE",
            device_performance,
        )

        export_csv(
            device_performance,
            output_dir,
            "device_conversion",
        )

        # Referrer performance
        referrer_performance = (
            conversion_by_dimension(
                session_features,
                "referrer",
            )
        )

        show_result(
            "CONVERSION BY REFERRER",
            referrer_performance,
        )

        export_csv(
            referrer_performance,
            output_dir,
            "referrer_conversion",
        )

        # Product views
        views = product_views(
            sessions,
            products,
        )

        show_result(
            "MOST VIEWED PRODUCTS",
            views,
        )

        export_csv(
            views,
            output_dir,
            "product_views",
        )

        # Product affinity
        affinity = product_affinity(items)

        show_result(
            "FREQUENTLY PURCHASED PRODUCT PAIRS",
            affinity,
        )

        export_csv(
            affinity,
            output_dir,
            "product_affinity",
        )

        # Spark SQL
        sql_results = spark_sql_analysis(
            spark,
            valid_transactions,
            users,
        )

        show_result(
            "SPARK SQL: REVENUE BY COUNTRY",
            sql_results,
        )

        export_csv(
            sql_results,
            output_dir,
            "sql_revenue_by_country",
        )

        # Integrated analysis
        integrated = integrated_analytics(
            session_features,
            transactions,
        )

        show_result(
            "INTEGRATED SESSION AND PURCHASE ANALYSIS",
            integrated,
        )

        export_csv(
            integrated,
            output_dir,
            "integrated_analytics",
        )

        print("\n" + "=" * 65)
        print("SPARK ANALYTICS COMPLETED SUCCESSFULLY")
        print("=" * 65)
        print(f"Results saved in: {output_dir}")
        print("All exported datasets are CSV directories.")
        print("Use the part-*.csv files for visualization.")

        session_features.unpersist()
        sessions.unpersist()
        transactions.unpersist()

    except Exception:
        logger.exception(
            "Spark analytics pipeline failed"
        )
        sys.exit(1)

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
