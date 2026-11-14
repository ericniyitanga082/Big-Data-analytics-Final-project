
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"

spark = (
    SparkSession.builder
    .appName("AUCA_Data_Validation")
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
            "transaction_id", "session_id", "user_id",
            "timestamp", "subtotal", "discount", "total",
            "status"
        )
    )

    sessions = (
        spark.read.option("multiLine", "true")
        .json(str(DATA / "sessions_*.json"))
        .select("session_id", "user_id", "start_time", "end_time")
    )

    print("\n========== DATA VALIDATION ==========")

    for name, df, key in [
        ("Transactions", transactions, "transaction_id"),
        ("Sessions", sessions, "session_id"),
    ]:
        total = df.count()
        missing = df.filter(
            F.col(key).isNull() | (F.trim(F.col(key)) == "")
        ).count()
        distinct = df.select(key).distinct().count()

        print(f"\n{name}")
        print(f"Total records: {total:,}")
        print(f"Missing identifiers: {missing:,}")
        print(f"Duplicate identifiers: {total - distinct:,}")

    invalid_amounts = transactions.filter(
        (F.col("total") < 0) |
        (F.col("subtotal") < 0) |
        (F.col("discount") < 0)
    ).count()

    arithmetic_errors = transactions.filter(
        F.abs(
            F.col("subtotal") -
            F.col("discount") -
            F.col("total")
        ) > 0.02
    ).count()

    invalid_transaction_dates = transactions.filter(
        F.col("timestamp").isNotNull() &
        F.to_timestamp("timestamp").isNull()
    ).count()

    print("\nTransaction quality")
    print(f"Negative amounts: {invalid_amounts:,}")
    print(f"Arithmetic mismatches: {arithmetic_errors:,}")
    print(f"Invalid timestamps: {invalid_transaction_dates:,}")

    print("\nValidation completed.")

finally:
    spark.stop()
