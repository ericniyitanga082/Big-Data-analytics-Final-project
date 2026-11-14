
from pathlib import Path
import csv
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "outputs" / "customer_segments"
FIGURES = ROOT / "outputs" / "figures"

FIGURES.mkdir(parents=True, exist_ok=True)

csv_files = list(SOURCE.glob("part-*.csv"))

if not csv_files:
    raise FileNotFoundError(
        "Customer segmentation CSV not found."
    )

with csv_files[0].open(
    "r", encoding="utf-8-sig", newline=""
) as file:
    rows = list(csv.DictReader(file))

rows.sort(
    key=lambda row: int(row["customers"]),
    reverse=True
)

segments = [row["segment"] for row in rows]
counts = [int(row["customers"]) for row in rows]

total = sum(counts)

fig, ax = plt.subplots(figsize=(11, 6))

bars = ax.barh(segments, counts)
ax.invert_yaxis()

ax.set_title(
    "Customer Segmentation Using RFM Analysis",
    fontsize=15,
    fontweight="bold"
)
ax.set_xlabel("Number of Customers")
ax.set_ylabel("Customer Segment")

for bar, count in zip(bars, counts):
    ax.text(
        bar.get_width() + 35,
        bar.get_y() + bar.get_height() / 2,
        f"{count:,} ({count / total:.1%})",
        va="center",
        fontsize=10
    )

ax.set_xlim(0, max(counts) * 1.23)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

fig.tight_layout()

output = FIGURES / "05_customer_segmentation.png"

fig.savefig(output, dpi=300, bbox_inches="tight")
plt.close(fig)

print("Saved:", output)
print("Total customers:", total)
