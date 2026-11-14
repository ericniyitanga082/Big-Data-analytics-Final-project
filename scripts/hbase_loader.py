
import argparse
import json
import subprocess
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------

DATA_DIR = Path("data")
CONTAINER = "auca-hbase-clean"

USER_TABLE = "user_activity"
PRODUCT_TABLE = "product_daily_metrics"

BATCH_SIZE = 1000


# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def hbase_quote(value):
    """Escape a value for an HBase Shell Ruby string."""
    value = str(value)
    return (
        value.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def make_put(table, row_key, column, value):
    return (
        f"put '{hbase_quote(table)}', "
        f"'{hbase_quote(row_key)}', "
        f"'{hbase_quote(column)}', "
        f"'{hbase_quote(value)}'"
    )


def execute_batch(commands):
    """
    Copy a temporary HBase Shell script into Docker,
    execute it, then remove the temporary container file.
    """
    if not commands:
        return

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".hbase",
        encoding="utf-8",
        delete=False
    ) as temp:
        temp.write("\n".join(commands) + "\n")
        local_path = Path(temp.name)

    container_path = "/tmp/auca_hbase_batch.hbase"

    try:
        subprocess.run(
            [
                "docker", "cp",
                str(local_path),
                f"{CONTAINER}:{container_path}"
            ],
            check=True
        )

        subprocess.run(
            [
                "docker", "exec",
                CONTAINER,
                "hbase", "shell", "-n",
                container_path
            ],
            check=True
        )

    finally:
        local_path.unlink(missing_ok=True)


def send_commands(commands):
    """Execute commands in manageable batches."""
    for start in range(0, len(commands), BATCH_SIZE):
        batch = commands[start:start + BATCH_SIZE]

        print(
            f"Executing commands "
            f"{start + 1} to {start + len(batch)} "
            f"of {len(commands)}"
        )

        execute_batch(batch)


def find_files(prefix):
    files = list(DATA_DIR.glob(f"{prefix}_*.json"))

    return sorted(
        files,
        key=lambda p: int(p.stem.split("_")[-1])
    )


# ---------------------------------------------------------
# 1. LOAD USER PROFILES
# ---------------------------------------------------------

def load_users():
    print("\n========== LOADING USERS ==========")

    path = DATA_DIR / "users.json"
    users = load_json(path)

    commands = []

    for user in users:
        user_id = user.get("user_id")

        if not user_id:
            continue

        geo = user.get("geo_data") or {}

        values = {
            "profile:city": geo.get("city", ""),
            "profile:state": geo.get("state", ""),
            "profile:country": geo.get("country", ""),
            "profile:registration_date": user.get(
                "registration_date", ""
            ),
            "profile:last_active": user.get(
                "last_active", ""
            )
        }

        for column, value in values.items():
            commands.append(
                make_put(
                    USER_TABLE,
                    user_id,
                    column,
                    value
                )
            )

    send_commands(commands)

    print(f"Loaded {len(users):,} user profiles.")


# ---------------------------------------------------------
# 2. SUMMARIZE BROWSING SESSIONS
# ---------------------------------------------------------

def load_sessions():
    print("\n========== PROCESSING SESSIONS ==========")

    files = find_files("sessions")

    print(f"Found {len(files)} session files.")

    # Per-user aggregation, not all raw session records.
    user_stats = defaultdict(
        lambda: {
            "sessions": 0,
            "page_views": 0,
            "product_views": 0,
            "cart_sessions": 0,
            "purchased_sessions": 0,
            "duration_seconds": 0
        }
    )

    total_sessions = 0

    for file_number, path in enumerate(files, start=1):
        sessions = load_json(path)

        for session in sessions:
            user_id = session.get("user_id")

            if not user_id:
                continue

            stats = user_stats[user_id]

            stats["sessions"] += 1

            page_views = session.get("page_views") or []
            viewed_products = (
                session.get("viewed_products") or []
            )

            stats["page_views"] += len(page_views)

            stats["product_views"] += len(viewed_products)

            stats["duration_seconds"] += (
                session.get("duration_seconds") or 0
            )

            cart = session.get("cart_contents") or {}

            if cart:
                stats["cart_sessions"] += 1

            if session.get("conversion_status") == "purchased":
                stats["purchased_sessions"] += 1

            total_sessions += 1

        print(
            f"Processed {file_number}/{len(files)}: "
            f"{path.name} "
            f"({total_sessions:,} sessions total)"
        )

        del sessions

    print("\nWriting aggregated session statistics to HBase...")

    commands = []

    for user_id, stats in user_stats.items():
        for metric, value in stats.items():
            commands.append(
                make_put(
                    USER_TABLE,
                    user_id,
                    f"activity:{metric}",
                    value
                )
            )

        if len(commands) >= BATCH_SIZE:
            execute_batch(commands)
            commands.clear()

    if commands:
        execute_batch(commands)

    print(
        f"Finished processing {total_sessions:,} sessions "
        f"for {len(user_stats):,} users."
    )


# ---------------------------------------------------------
# 3. PROCESS TRANSACTIONS
# ---------------------------------------------------------

def load_transactions():
    print("\n========== PROCESSING TRANSACTIONS ==========")

    files = find_files("transactions")

    print(f"Found {len(files)} transaction files.")

    user_stats = defaultdict(
        lambda: {
            "completed_orders": 0,
            "total_spent": 0.0,
            "items_purchased": 0
        }
    )

    # Key: (date, product_id)
    product_stats = defaultdict(
        lambda: {
            "quantity_sold": 0,
            "revenue": 0.0,
            "orders": 0
        }
    )

    total_transactions = 0
    completed_transactions = 0

    for file_number, path in enumerate(files, start=1):
        transactions = load_json(path)

        for transaction in transactions:
            total_transactions += 1

            if transaction.get("status") != "completed":
                continue

            completed_transactions += 1

            user_id = transaction.get("user_id")
            timestamp = transaction.get("timestamp", "")
            date = timestamp[:10]

            if not date:
                continue

            items = transaction.get("items") or []
            total = float(transaction.get("total") or 0)

            if user_id:
                stats = user_stats[user_id]

                stats["completed_orders"] += 1
                stats["total_spent"] += total

                stats["items_purchased"] += sum(
                    int(item.get("quantity") or 0)
                    for item in items
                )

            for item in items:
                product_id = item.get("product_id")

                if not product_id:
                    continue

                key = (date, product_id)

                quantity = int(
                    item.get("quantity") or 0
                )

                revenue = float(
                    item.get("subtotal") or 0
                )

                product_stats[key]["quantity_sold"] += quantity
                product_stats[key]["revenue"] += revenue
                product_stats[key]["orders"] += 1

        print(
            f"Processed {file_number}/{len(files)}: "
            f"{path.name} "
            f"({total_transactions:,} transactions total)"
        )

        del transactions

    # -----------------------------------------------------
    # WRITE USER TRANSACTION STATISTICS
    # -----------------------------------------------------

    print("\nWriting user purchase statistics...")

    commands = []

    for user_id, stats in user_stats.items():
        for metric, value in stats.items():
            if isinstance(value, float):
                value = round(value, 2)

            commands.append(
                make_put(
                    USER_TABLE,
                    user_id,
                    f"activity:{metric}",
                    value
                )
            )

        if len(commands) >= BATCH_SIZE:
            execute_batch(commands)
            commands.clear()

    if commands:
        execute_batch(commands)

    # -----------------------------------------------------
    # WRITE DAILY PRODUCT METRICS
    # -----------------------------------------------------

    print("\nWriting product daily metrics...")

    commands = []

    for (date, product_id), stats in product_stats.items():

        # Product-first row key supports product history scans.
        row_key = f"{product_id}#{date}"

        values = {
            "metrics:date": date,
            "metrics:product_id": product_id,
            "metrics:quantity_sold": stats["quantity_sold"],
            "metrics:revenue": round(stats["revenue"], 2),
            "metrics:orders": stats["orders"]
        }

        for column, value in values.items():
            commands.append(
                make_put(
                    PRODUCT_TABLE,
                    row_key,
                    column,
                    value
                )
            )

        if len(commands) >= BATCH_SIZE:
            execute_batch(commands)
            commands.clear()

    if commands:
        execute_batch(commands)

    print(
        f"\nProcessed {total_transactions:,} transactions."
    )

    print(
        f"Completed transactions: "
        f"{completed_transactions:,}"
    )

    print(
        f"Daily product metric rows: "
        f"{len(product_stats):,}"
    )


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--part",
        choices=["users", "sessions", "transactions", "all"],
        default="all"
    )

    parser.add_argument(
        "--data-dir",
        default="data"
    )

    parser.add_argument(
        "--container",
        default="auca-hbase-clean"
    )

    args = parser.parse_args()

    global DATA_DIR, CONTAINER

    DATA_DIR = Path(args.data_dir)
    CONTAINER = args.container

    print("=" * 60)
    print("AUCA E-COMMERCE HBASE DATA LOADER")
    print("=" * 60)

    print("Data directory:", DATA_DIR)
    print("Docker container:", CONTAINER)

    if args.part in ("users", "all"):
        load_users()

    if args.part in ("sessions", "all"):
        load_sessions()

    if args.part in ("transactions", "all"):
        load_transactions()

    print("\nHBase loading completed successfully.")


if __name__ == "__main__":
    main()
