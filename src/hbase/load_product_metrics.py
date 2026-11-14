
import argparse
import json
from collections import defaultdict
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

import happybase

# Project configuration
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"

HBASE_HOST = "localhost"
HBASE_PORT = 9091
TABLE_NAME = "product_daily_metrics"


def read_records(path):
    """Read one JSON file at a time."""
    with path.open("r", encoding="utf-8") as file:
        records = json.load(file)

    if not isinstance(records, list):
        raise ValueError(f"Expected JSON list: {path}")

    return records


def find_files(prefix):
    """Find and numerically sort matching JSON files."""
    files = list(DATA_DIR.glob(f"{prefix}_*.json"))

    def file_number(path):
        return int(path.stem.rsplit("_", 1)[1])

    return sorted(files, key=file_number)


def money(value):
    """Convert monetary values to Decimal."""
    return Decimal(str(value))


def calculate_views(files, metrics):
    """Count product views and distinct viewing sessions."""
    processed = 0

    for path in files:
        sessions = read_records(path)

        for session in sessions:
            viewed_today = set()

            for event in session.get("page_views", []):
                if event.get("page_type") != "product_detail":
                    continue

                product_id = event.get("product_id")
                timestamp = event.get("timestamp")

                if not product_id or not timestamp:
                    continue

                date = timestamp[:10].replace("-", "")
                key = (product_id, date)

                metrics[key]["views"] += 1
                viewed_today.add(key)

            for key in viewed_today:
                metrics[key]["viewing_sessions"] += 1

            processed += 1

        print(f"Views processed: {path.name} ({processed:,} sessions)")

    return processed


def calculate_sales(files, metrics):
    """Aggregate daily sales and discount-adjusted revenue."""
    processed = 0

    for path in files:
        transactions = read_records(path)

        for transaction in transactions:
            timestamp = transaction.get("timestamp")
            items = transaction.get("items") or []

            if not timestamp or not items:
                continue

            # Exclude transactions not yet completed or delivered.
            if transaction.get("status") not in ("completed", "delivered"):
                continue

            date = timestamp[:10].replace("-", "")
            subtotal = money(transaction.get("subtotal", 0))
            total = money(transaction.get("total", 0))

            # Allocate transaction-level net revenue to products
            # proportionally to their line-item subtotals.
            if subtotal <= 0:
                continue

            seen_products = set()

            for item in items:
                product_id = item.get("product_id")
                if not product_id:
                    continue

                key = (product_id, date)
                quantity = int(item.get("quantity", 0))
                item_subtotal = money(item.get("subtotal", 0))

                allocated_revenue = total * item_subtotal / subtotal

                metrics[key]["quantity"] += quantity
                metrics[key]["revenue"] += allocated_revenue

                if product_id not in seen_products:
                    metrics[key]["transactions"] += 1
                    seen_products.add(product_id)

            processed += 1

        print(f"Sales processed: {path.name}")

    return processed


def write_to_hbase(metrics):
    """Write aggregated product-day records using batch operations."""
    connection = happybase.Connection(
        HBASE_HOST,
        port=HBASE_PORT,
        timeout=120000
    )

    connection.open()

    try:
        table = connection.table(TABLE_NAME)

        with table.batch(batch_size=100) as batch:
            for (product_id, date), values in metrics.items():
                row_key = f"{product_id}#{date}"

                revenue = values["revenue"].quantize(
                    Decimal("0.01"),
                    rounding=ROUND_HALF_UP
                )

                # This ratio compares daily transaction counts
                # with daily viewing sessions. It is NOT a
                # session-level purchase conversion rate.
                viewing_sessions = values["viewing_sessions"]

                ratio = (
                    values["transactions"] / viewing_sessions
                    if viewing_sessions > 0 else 0
                )

                batch.put(row_key, {
                    "views:count": str(values["views"]),
                    "views:unique_sessions": str(viewing_sessions),
                    "sales:quantity": str(values["quantity"]),
                    "sales:transactions": str(values["transactions"]),
                    "sales:revenue": str(revenue),
                    "conversion:transaction_view_ratio": f"{ratio:.6f}"
                })

        print(f"Successfully wrote {len(metrics):,} product-day rows.")

    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--session-files", type=int, default=1
    )
    parser.add_argument(
        "--transaction-files", type=int, default=1
    )
    parser.add_argument(
        "--write", action="store_true",
        help="Write calculated metrics to HBase"
    )

    args = parser.parse_args()

    session_files = find_files("sessions")
    transaction_files = find_files("transactions")

    if not session_files or not transaction_files:
        raise FileNotFoundError(
            "Session or transaction JSON files not found."
        )

    if args.session_files < 1 or args.transaction_files < 1:
        raise ValueError("File counts must be positive.")

    if args.session_files > len(session_files):
        raise ValueError("Not enough session files.")

    if args.transaction_files > len(transaction_files):
        raise ValueError("Not enough transaction files.")

    metrics = defaultdict(lambda: {
        "views": 0,
        "viewing_sessions": 0,
        "quantity": 0,
        "transactions": 0,
        "revenue": Decimal("0")
    })

    session_count = calculate_views(
        session_files[:args.session_files], metrics
    )

    transaction_count = calculate_sales(
        transaction_files[:args.transaction_files], metrics
    )

    print("\nSUMMARY")
    print(f"Sessions processed: {session_count:,}")
    print(f"Eligible transactions: {transaction_count:,}")
    print(f"Product-day records: {len(metrics):,}")

    if args.write:
        write_to_hbase(metrics)
    else:
        print("Dry run only. No HBase records written.")


if __name__ == "__main__":
    main()
