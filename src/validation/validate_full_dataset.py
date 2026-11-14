"""Validate the full AUCA e-commerce dataset without loading all batches into RAM.

Run from project root: python src/validation/validate_full_dataset.py
"""
import json
import sqlite3
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from contextlib import closing

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
EXPECTED = {"Users": 10000, "Products": 5000, "Categories": 25,
            "Transactions": 500000, "Sessions": 2000000}
TOLERANCE = 0.011  # Allow rounding to cents in generated financial amounts.
SESSION_FIELDS = ("session_id", "user_id", "start_time", "end_time")
TRANSACTION_FIELDS = ("transaction_id", "user_id", "timestamp", "subtotal", "total")
VALID_STATUSES = {"converted", "abandoned", "browsed"}


def load_json(path):
    with path.open("r", encoding="utf-8") as handle:
        records = json.load(handle)
    if not isinstance(records, list):
        raise ValueError(f"Expected JSON array in {path}")
    return records


def batch_files(prefix, expected_count):
    files = sorted(DATA_DIR.glob(f"{prefix}_*.json"),
                   key=lambda p: int(p.stem.rsplit("_", 1)[1]))
    actual_indices = [int(p.stem.rsplit("_", 1)[1]) for p in files]
    if actual_indices != list(range(expected_count)):
        raise ValueError(f"Missing or unexpected {prefix} batches: expected indices "
                         f"0..{expected_count - 1}, found {len(files)} files")
    return files


def report(label, bad):
    print(f"{label:<39} Invalid: {bad:>8,} | {'PASS' if bad == 0 else 'FAIL'}")


def missing_count(records, fields, prefix, issues):
    for field in fields:
        bad = sum(record.get(field) is None for record in records)
        issues[f"Missing {prefix}.{field}"] += bad


def main():
    print("=" * 65)
    print("AUCA E-COMMERCE FULL DATASET VALIDATION")
    print("=" * 65)
    users = load_json(DATA_DIR / "users.json")
    products = load_json(DATA_DIR / "products.json")
    categories = load_json(DATA_DIR / "categories.json")
    sessions_files = batch_files("sessions", 200)
    transactions_files = batch_files("transactions", 50)
    print(f"Found {len(sessions_files)} session batches and "
          f"{len(transactions_files)} transaction batches")

    counts = Counter(Users=len(users), Products=len(products), Categories=len(categories))
    issues = Counter()
    user_ids = {x.get("user_id") for x in users}
    product_ids = {x.get("product_id") for x in products}
    category_ids = {x.get("category_id") for x in categories}

    for name, records, field, fields in (
        ("Users", users, "user_id", ("user_id", "registration_date")),
        ("Products", products, "product_id", ("product_id", "category_id", "base_price", "current_stock")),
        ("Categories", categories, "category_id", ("category_id", "name")),
    ):
        ids = [r.get(field) for r in records]
        issues[f"Duplicate {name} IDs"] += len(ids) - len(set(ids))
        missing_count(records, fields, name, issues)
    issues["Product -> Category"] += sum(p.get("category_id") not in category_ids for p in products)

    # Disk-backed ID indexes avoid keeping 2.5 million identifiers in Python RAM.
    with tempfile.TemporaryDirectory(prefix="auca_validation_") as temp_dir:
        db_path = Path(temp_dir) / "ids.sqlite"
        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("CREATE TABLE session_ids (id TEXT PRIMARY KEY)")
            conn.execute("CREATE TABLE transaction_ids (id TEXT PRIMARY KEY)")
            conn.execute("PRAGMA synchronous = OFF")  # Disposable validation index only.

            for number, path in enumerate(sessions_files, 1):
                batch = load_json(path)
                counts["Sessions"] += len(batch)
                missing_count(batch, SESSION_FIELDS, "Sessions", issues)
                ids = [s.get("session_id") for s in batch if s.get("session_id") is not None]
                before = conn.total_changes
                conn.executemany("INSERT OR IGNORE INTO session_ids VALUES (?)", ((x,) for x in ids))
                issues["Duplicate Sessions IDs"] += len(ids) - (conn.total_changes - before)
                for s in batch:
                    issues["Session -> User"] += s.get("user_id") not in user_ids
                    issues["Negative session duration"] += (s.get("duration_seconds") is not None and
                                                             s["duration_seconds"] < 0)
                    issues["Invalid conversion status"] += s.get("conversion_status") not in VALID_STATUSES
                    try:
                        if datetime.fromisoformat(s["end_time"]) < datetime.fromisoformat(s["start_time"]):
                            issues["End time before start time"] += 1
                    except (ValueError, TypeError, KeyError):
                        issues["Invalid session timestamps"] += 1
                conn.commit()
                if number % 25 == 0:
                    print(f"Validated session batches: {number}/200")
                del batch

            for number, path in enumerate(transactions_files, 1):
                batch = load_json(path)
                counts["Transactions"] += len(batch)
                missing_count(batch, TRANSACTION_FIELDS, "Transactions", issues)
                ids = [t.get("transaction_id") for t in batch if t.get("transaction_id") is not None]
                before = conn.total_changes
                conn.executemany("INSERT OR IGNORE INTO transaction_ids VALUES (?)", ((x,) for x in ids))
                issues["Duplicate Transactions IDs"] += len(ids) - (conn.total_changes - before)
                linked_ids = list({t.get("session_id") for t in batch if t.get("session_id") is not None})
                # Look up linked sessions against the complete disk-backed session index.
                for session_id in linked_ids:
                    if conn.execute("SELECT 1 FROM session_ids WHERE id=?", (session_id,)).fetchone() is None:
                        issues["Transaction -> Session"] += sum(t.get("session_id") == session_id for t in batch)
                for t in batch:
                    issues["Transaction -> User"] += t.get("user_id") not in user_ids
                    item_sum = 0.0
                    for item in t.get("items", []):
                        issues["Transaction Item -> Product"] += item.get("product_id") not in product_ids
                        try:
                            q, price, subtotal = item["quantity"], item["unit_price"], item["subtotal"]
                            issues["Item quantity x price"] += abs(q * price - subtotal) > TOLERANCE
                            issues["Negative financial values"] += q < 0 or price < 0 or subtotal < 0
                            item_sum += subtotal
                        except (KeyError, TypeError, ValueError):
                            issues["Malformed transaction item"] += 1
                    try:
                        subtotal, discount, total = t["subtotal"], t["discount"], t["total"]
                        issues["Transaction subtotal"] += abs(item_sum - subtotal) > TOLERANCE
                        issues["Subtotal - discount = total"] += abs(subtotal - discount - total) > TOLERANCE
                        issues["Negative financial values"] += subtotal < 0 or discount < 0 or total < 0
                    except (KeyError, TypeError, ValueError):
                        issues["Malformed transaction totals"] += 1
                conn.commit()
                if number % 10 == 0:
                    print(f"Validated transaction batches: {number}/50")
                del batch

    print("\nCHECK 1: RECORD COUNTS")
    count_failures = 0
    for name, expected in EXPECTED.items():
        actual = counts[name]
        passed = actual == expected
        count_failures += not passed
        print(f"{name:<15} Expected: {expected:>9,} | Actual: {actual:>9,} | {'PASS' if passed else 'FAIL'}")

    print("\nCHECKS 2-6: DATA QUALITY")
    for label in sorted(issues):
        report(label, issues[label])
    # Report checks even when no failures were recorded.
    for label in ("Duplicate Sessions IDs", "Duplicate Transactions IDs", "Session -> User",
                  "Product -> Category", "Transaction -> User", "Transaction -> Session",
                  "Transaction Item -> Product", "Item quantity x price", "Transaction subtotal",
                  "Subtotal - discount = total", "Negative financial values",
                  "End time before start time", "Negative session duration", "Invalid conversion status"):
        if label not in issues:
            report(label, 0)

    passed = count_failures == 0 and not any(issues.values())
    print(f"\nFINAL RESULT: {'PASS' if passed else 'FAIL'}")
    print("Note: This validates the generated dataset, not the MongoDB/HBase imports.")
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
