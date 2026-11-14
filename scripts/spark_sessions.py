
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, ArrayType
)
import csv
import matplotlib.pyplot as plt

# Project directories
root = Path(__file__).resolve().parent.parent
data = root / "data"
output = root / "outputs" / "spark_sessions"
charts = root / "outputs" / "charts"

output.mkdir(parents=True, exist_ok=True)
charts.mkdir(parents=True, exist_ok=True)

files = sorted(data.glob("sessions_*.json"))

if not files:
    raise FileNotFoundError("No session files found")

# Read only the necessary session fields
device_schema = StructType([
    StructField("browser", StringType()),
    StructField("os", StringType()),
    StructField("type", StringType())
])

schema = StructType([
    StructField("session_id", StringType()),
    StructField("conversion_status", StringType()),
    StructField("referrer", StringType()),
    StructField("device_profile", device_schema),
    StructField("duration_seconds", LongType()),
    StructField("viewed_products", ArrayType(StringType()))
])

spark = (
    SparkSession.builder
    .appName("AUCA_Session_Analytics")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    partial = output / "partial"
    partial.mkdir(exist_ok=True)

    # 1. Process each session file separately
    for i, file in enumerate(files, 1):
        df = (
            spark.read.schema(schema)
            .option("multiLine", "true")
            .json(str(file))
        )

        summary = (
            df.select(
                F.coalesce(
                    F.col("conversion_status"),
                    F.lit("unknown")
                ).alias("conversion_status"),
                F.coalesce(
                    F.col("device_profile.type"),
                    F.lit("unknown")
                ).alias("device_type"),
                F.coalesce(
                    F.col("referrer"),
                    F.lit("unknown")
                ).alias("referrer"),
                F.col("duration_seconds")
            )
            .groupBy(
                "conversion_status",
                "device_type",
                "referrer"
            )
            .agg(
                F.count("*").alias("sessions"),
                F.sum("duration_seconds").alias("duration_total"),
                F.count("duration_seconds").alias("duration_count")
            )
        )

        summary.coalesce(1).write.mode("overwrite") \
            .parquet(str(partial / f"batch_{i:03d}"))

        print(f"Processed session file {i}/{len(files)}",
              flush=True)

    # 2. Combine compact partial results
    combined = spark.read.parquet(
        str(partial / "batch_*")
    )

    summary = (
        combined.groupBy(
            "conversion_status",
            "device_type",
            "referrer"
        )
        .agg(
            F.sum("sessions").alias("sessions"),
            F.sum("duration_total").alias("duration_total"),
            F.sum("duration_count").alias("duration_count")
        )
    )

    summary.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "session_summary"))

    # 3. Conversion status analysis
    funnel = (
        summary.groupBy("conversion_status")
        .agg(F.sum("sessions").alias("sessions"))
        .orderBy(F.desc("sessions"))
    )

    print("\nCONVERSION STATUS")
    funnel.show(truncate=False)

    funnel.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "conversion_status"))

    # 4. Device analysis
    devices = (
        summary.groupBy("device_type")
        .agg(F.sum("sessions").alias("sessions"))
        .orderBy(F.desc("sessions"))
    )

    print("\nDEVICE PERFORMANCE")
    devices.show(truncate=False)

    devices.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "device_performance"))

    # 5. Referrer analysis
    referrers = (
        summary.groupBy("referrer")
        .agg(F.sum("sessions").alias("sessions"))
        .orderBy(F.desc("sessions"))
    )

    print("\nREFERRER PERFORMANCE")
    referrers.show(truncate=False)

    referrers.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(output / "referrer_performance"))

    print("\nSPARK SESSION ANALYTICS COMPLETED")

finally:
    spark.stop()


# 6. Read the small Spark CSV outputs
def read_csv(folder):
    files = list(folder.glob("part-*.csv"))
    if not files:
        raise FileNotFoundError(folder)
    with open(files[0], newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# 7. Save conversion status chart
funnel_data = read_csv(output / "conversion_status")

names = [r["conversion_status"] for r in funnel_data]
values = [int(r["sessions"]) for r in funnel_data]

plt.figure(figsize=(10, 5))
plt.bar(names, values)
plt.title("E-commerce Session Conversion Status")
plt.xlabel("Conversion Status")
plt.ylabel("Number of Sessions")
plt.xticks(rotation=25)
plt.tight_layout()
plt.savefig(charts / "conversion_funnel.png", dpi=200)
plt.close()

print("Saved conversion_funnel.png")


# 8. Save device distribution chart
device_data = read_csv(output / "device_performance")

names = [r["device_type"] for r in device_data]
values = [int(r["sessions"]) for r in device_data]

plt.figure(figsize=(9, 5))
plt.bar(names, values)
plt.title("Sessions by Device Type")
plt.xlabel("Device Type")
plt.ylabel("Number of Sessions")
plt.tight_layout()
plt.savefig(charts / "device_distribution.png", dpi=200)
plt.close()

print("Saved device_distribution.png")
print("\nALL RESULTS SAVED SUCCESSFULLY")
