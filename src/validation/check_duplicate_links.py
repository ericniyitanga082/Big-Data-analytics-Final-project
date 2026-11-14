import json
from pathlib import Path

DATA_DIR = Path(r"D:\AUCA_Ecommerce_BigData\data")
TARGET_ID = "sess_3a15f1cc10"

print("Checking duplicate session records...")

for path in sorted(DATA_DIR.glob("sessions_*.json")):
    with open(path, "r", encoding="utf-8") as f:
        sessions = json.load(f)

    for session in sessions:
        if session.get("session_id") == TARGET_ID:
            print(
                "Session found:",
                path.name,
                "| User:",
                session.get("user_id"),
                "| Start:",
                session.get("start_time")
            )

print("\nChecking linked transactions...")

linked_count = 0

for path in sorted(DATA_DIR.glob("transactions_*.json")):
    with open(path, "r", encoding="utf-8") as f:
        transactions = json.load(f)

    for transaction in transactions:
        if transaction.get("session_id") == TARGET_ID:
            linked_count += 1
            print(
                "Transaction:",
                transaction.get("transaction_id"),
                "| File:",
                path.name,
                "| User:",
                transaction.get("user_id")
            )

print(f"\nTotal linked transactions: {linked_count}")