
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType
import matplotlib.pyplot as plt

root = Path(__file__).resolve().parent.parent
data = root / "data"
output = root / "outputs" / "spark_session_join"
charts = root / "outputs" / "charts"

output.mkdir(parents=True, exist_ok=True)
charts.mkdir(parents=True, exist_ok=True)

transaction_files = sorted(data.glob("transactions_*.json"))
session_files = sorted(data.glob("sessions_*.json"))

if not transaction_files or not session_files:
    raise FileNotFoundError("Transaction or session files missing")

spark = (
    SparkSession.builder
    .appName("AUCA_Session_Transaction_Integration")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

txn_schema = StructType([
    StructField("session_id", StringType()),
    StructField("status", StringType())
])

session_schema = StructType([
    StructField("session_id", StringType()),
    StructField("conversion_status", StringType())
])

try:
    # 1. Extract distinct successful transaction session IDs
    txn_parts = output / "transaction_ids"
    txn_parts.mkdir(exist_ok=True)

    for i, file in enumerate(transaction_files, 1):
        df = (
            spark.read.schema(txn_schema)
            .option("multiLine", "true")
            .json(str(file))
        )

        linked = (
            df.filter(
                F.col("status").isin("completed", "delivered")
                & F.col("session_id").isNotNull()
            )
            .select("session_id")
            .distinct()
        )

        linked.write.mode("overwrite").parquet(
            str(txn_parts / f"batch_{i:03d}")
        )

        print(f"Transactions: {i}/{len(transaction_files)}",
              flush=True)

    # 2. Combine and deduplicate transaction session IDs
    transaction_ids = (
        spark.read.parquet(str(txn_parts / "batch_*"))
        .distinct()
    )

    transaction_ids.write.mode("overwrite").parquet(
        str(output / "distinct_transaction_sessions")
    )

    # 3. Extract session IDs and conversion labels
    session_parts = output / "session_ids"
    session_parts.mkdir(exist_ok=True)

    for i, file in enumerate(session_files, 1):
        df = (
            spark.read.schema(session_schema)
            .option("multiLine", "true")
            .json(str(file))
        )

        df.select(
            "session_id", "conversion_status"
        ).write.mode("overwrite").parquet(
            str(session_parts / f"batch_{i:03d}")
        )

        print(f"Sessions: {i}/{len(session_files)}",
              flush=True)

    # 4. Load compact session data
    sessions = spark.read.parquet(
        str(session_parts / "batch_*")
    ).dropDuplicates(["session_id"])

    # 5. Join transactions with sessions
    matched = transaction_ids.join(
        sessions, "session_id", "inner"
    )

    missing = transaction_ids.join(
        sessions.select("session_id"),
        "session_id",
        "left_anti"
    )

    # 6. Summary metrics
    linked_distinct = transaction_ids.count()
    matched_count = matched.count()
    missing_count = missing.count()

    print("\nSESSION LINKAGE VERIFICATION")
    print("Distinct linked sessions:", linked_distinct)
    print("Matched sessions:", matched_count)
    print("Missing sessions:", missing_count)

    # 7. Conversion status among matched sessions
    status = (
        matched.groupBy("conversion_status")
        .count()
        .orderBy(F.desc("count"))
    )

    print("\nMATCHED SESSION CONVERSION STATUS")
    status.show(truncate=False)

    # 8. Save results
    status.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "matched_status"))

    with open(output / "linkage_summary.txt", "w") as f:
        f.write(f"Distinct linked sessions: {linked_distinct}\n")
        f.write(f"Matched sessions: {matched_count}\n")
        f.write(f"Missing sessions: {missing_count}\n")

    # 9. Chart
    rows = status.collect()

    labels = [
        r["conversion_status"] or "unknown"
        for r in rows
    ]
    values = [int(r["count"]) for r in rows]

    plt.figure(figsize=(9, 5))
    plt.bar(labels, values)
    plt.title("Conversion Status of Transaction-Linked Sessions")
    plt.xlabel("Recorded Session Status")
    plt.ylabel("Distinct Sessions")
    plt.tight_layout()
    plt.savefig(
        charts / "verified_session_conversion.png",
        dpi=200
    )
    plt.close()

    print("\nSESSION INTEGRATION COMPLETED")
    print("Saved verified_session_conversion.png")

finally:
    spark.stop()
 