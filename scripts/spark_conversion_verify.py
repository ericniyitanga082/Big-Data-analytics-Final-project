
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType
import csv
import matplotlib.pyplot as plt

root = Path(__file__).resolve().parent.parent
files = sorted((root / "data").glob("transactions_*.json"))
output = root / "outputs" / "spark_conversion"
charts = root / "outputs" / "charts"

output.mkdir(parents=True, exist_ok=True)
charts.mkdir(parents=True, exist_ok=True)

if not files:
    raise FileNotFoundError("No transaction files found")

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("session_id", StringType()),
    StructField("status", StringType())
])

spark = (
    SparkSession.builder
    .appName("AUCA_Conversion_Verification")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    partial = output / "partial"
    partial.mkdir(exist_ok=True)

    # Process every transaction file separately
    for i, file in enumerate(files, 1):
        df = (
            spark.read.schema(schema)
            .option("multiLine", "true")
            .json(str(file))
        )

        valid = df.filter(
            F.col("transaction_id").isNotNull()
        )

        metrics = valid.agg(
            F.count("*").alias("total_transactions"),

            F.sum(
                F.when(
                    F.col("session_id").isNotNull(), 1
                ).otherwise(0)
            ).alias("linked_transactions"),

            F.sum(
                F.when(
                    F.col("status").isin(
                        "completed", "delivered"
                    ), 1
                ).otherwise(0)
            ).alias("successful_transactions"),

            F.sum(
                F.when(
                    F.col("session_id").isNotNull()
                    & F.col("status").isin(
                        "completed", "delivered"
                    ), 1
                ).otherwise(0)
            ).alias("linked_successful")
        )

        metrics.write.mode("overwrite").parquet(
            str(partial / f"batch_{i:03d}")
        )

        print(f"Processed file {i}/{len(files)}",
              flush=True)

    # Combine all batch results
    combined = spark.read.parquet(
        str(partial / "batch_*")
    )

    totals = combined.agg(
        *[
            F.sum(c).alias(c)
            for c in combined.columns
        ]
    )

    print("\nTRANSACTION-LINKED CONVERSION SUMMARY")
    totals.show(truncate=False)

    totals.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "conversion_summary"))

finally:
    spark.stop()


# Generate a chart from saved Spark results
csv_files = list(
    (output / "conversion_summary").glob("part-*.csv")
)

with open(csv_files[0], newline="", encoding="utf-8") as f:
    result = next(csv.DictReader(f))

labels = [
    "All transactions",
    "Linked transactions",
    "Successful transactions",
    "Linked successful transactions"
]

values = [
    int(result["total_transactions"]),
    int(result["linked_transactions"]),
    int(result["successful_transactions"]),
    int(result["linked_successful"])
]

plt.figure(figsize=(11, 6))
plt.barh(labels[::-1], values[::-1])
plt.title("Transaction and Session Linkage")
plt.xlabel("Number of Transactions")
plt.tight_layout()

plt.savefig(
    charts / "transaction_linkage.png",
    dpi=200,
    bbox_inches="tight"
)
plt.close()

print("\nCONVERSION VERIFICATION COMPLETED")
print("Saved transaction_linkage.png")
print("Results saved in:", output)
