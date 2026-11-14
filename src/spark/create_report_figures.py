
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "outputs"
FIGURES = OUTPUT / "figures"
FIGURES.mkdir(parents=True, exist_ok=True)

def load_csv(folder):
    files = list((OUTPUT / folder).glob("part-*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No Spark CSV found in outputs/{folder}"
        )
    return pd.read_csv(files[0])

def save_chart(filename):
    plt.savefig(
        FIGURES / filename,
        dpi=300,
        bbox_inches="tight",
        facecolor="white"
    )
    plt.close()
    print(f"Saved: {filename}")

def format_amount(x, pos):
    return f"{x / 1_000_000:.1f}M"

daily = load_csv("daily_revenue")
products = load_csv("product_performance")
categories = load_csv("category_performance")

plt.rcParams.update({
    "font.size": 10,
    "axes.titlesize": 14,
    "axes.labelsize": 11,
    "figure.figsize": (10, 5.5)
})

# Figure 1: Daily revenue trend
daily["sale_date"] = pd.to_datetime(daily["sale_date"])
daily = daily.sort_values("sale_date")

fig, ax = plt.subplots()
ax.plot(daily["sale_date"], daily["revenue"], linewidth=1.8)
ax.set_title("Daily E-commerce Revenue Trend")
ax.set_xlabel("Transaction Date")
ax.set_ylabel("Revenue (dataset monetary units)")
ax.yaxis.set_major_formatter(FuncFormatter(format_amount))
ax.grid(alpha=0.25)
fig.autofmt_xdate()
save_chart("01_daily_revenue.png")

# Figure 2: Top products by revenue
top_revenue = products.nlargest(10, "revenue")
fig, ax = plt.subplots()
ax.barh(top_revenue["product_id"], top_revenue["revenue"])
ax.invert_yaxis()
ax.set_title("Top 10 Products by Revenue")
ax.set_xlabel("Revenue (dataset monetary units)")
ax.xaxis.set_major_formatter(
    FuncFormatter(lambda x, pos: f"{x / 1000:.0f}K")
)
save_chart("02_top_products_revenue.png")

# Figure 3: Top products by units sold
top_units = products.nlargest(10, "units_sold")
fig, ax = plt.subplots()
ax.barh(top_units["product_id"], top_units["units_sold"])
ax.invert_yaxis()
ax.set_title("Top 10 Products by Quantity Sold")
ax.set_xlabel("Units Sold")
save_chart("03_top_products_quantity.png")

# Figure 4: Revenue by category
top_categories = categories.nlargest(10, "revenue")
fig, ax = plt.subplots()
ax.barh(
    top_categories["category_name"],
    top_categories["revenue"]
)
ax.invert_yaxis()
ax.set_title("Top 10 Categories by Revenue")
ax.set_xlabel("Revenue (dataset monetary units)")
ax.xaxis.set_major_formatter(FuncFormatter(format_amount))
save_chart("04_category_revenue.png")

print("\nAll four report figures created successfully.")
