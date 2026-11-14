
from pathlib import Path
import csv
import matplotlib.pyplot as plt

root = Path(__file__).resolve().parent.parent
output = root / "outputs"
charts = output / "charts"
charts.mkdir(parents=True, exist_ok=True)

def read_spark_csv(folder):
    files = list(folder.glob("part-*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV found in {folder}")
    with open(files[0], newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

# Chart 1: Daily revenue
daily = read_spark_csv(
    output / "spark_sales" / "daily_revenue"
)
daily.sort(key=lambda x: x["date"])

dates = [r["date"] for r in daily]
revenue = [float(r["revenue"]) for r in daily]

plt.figure(figsize=(11, 5))
plt.plot(dates, revenue)
plt.title("Daily E-commerce Revenue")
plt.xlabel("Date")
plt.ylabel("Revenue (dataset currency units)")
plt.xticks(rotation=45)
plt.locator_params(axis="x", nbins=10)
plt.tight_layout()
plt.savefig(charts / "daily_revenue.png", dpi=200)
plt.close()

print("Saved daily_revenue.png")

# Chart 2: Top 10 products
products = read_spark_csv(
    output / "spark_products" / "top_10_products"
)
products.sort(key=lambda x: float(x["revenue"]))

names = [r["product_id"] for r in products]
values = [float(r["revenue"]) for r in products]

plt.figure(figsize=(10, 6))
plt.barh(names, values)
plt.title("Top 10 Products by Revenue")
plt.xlabel("Item Revenue Before Order Discounts")
plt.ylabel("Product ID")
plt.tight_layout()
plt.savefig(charts / "top_10_products.png", dpi=200)
plt.close()

print("Saved top_10_products.png")
print("Charts saved to:", charts)
