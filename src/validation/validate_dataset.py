import json
from pathlib import Path

# ---------------------------------------------------------
# AUCA E-commerce Big Data Project
# Dataset Validation Script
# ---------------------------------------------------------

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Data directory
DATA_DIR = PROJECT_ROOT / "data"


def load_json(filename):
    """
    Load a JSON file from the project's data directory.
    """
    file_path = DATA_DIR / filename

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


print("=" * 60)
print("AUCA E-COMMERCE DATASET VALIDATION")
print("=" * 60)

print("\nLoading datasets...")

users = load_json("users.json")
products = load_json("products.json")
categories = load_json("categories.json")
transactions = load_json("transactions.json")
sessions = load_json("sessions_0.json")

print("Datasets loaded successfully.")

# ---------------------------------------------------------
# CHECK 1: Record Counts
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("CHECK 1: RECORD COUNTS")
print("=" * 60)

expected_counts = {
    "Users": 1000,
    "Products": 500,
    "Categories": 25,
    "Transactions": 5000,
    "Sessions": 10000
}

actual_counts = {
    "Users": len(users),
    "Products": len(products),
    "Categories": len(categories),
    "Transactions": len(transactions),
    "Sessions": len(sessions)
}

for dataset, expected in expected_counts.items():
    actual = actual_counts[dataset]

    if actual == expected:
        status = "PASS"
    else:
        status = "FAIL"

    print(
        f"{dataset:<15} "
        f"Expected: {expected:>8,} | "
        f"Actual: {actual:>8,} | "
        f"{status}"
    )

# ---------------------------------------------------------
# CHECK 2: Duplicate IDs
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("CHECK 2: DUPLICATE IDs")
print("=" * 60)


def check_duplicates(records, id_field, dataset_name):
    ids = [record.get(id_field) for record in records]

    duplicate_count = len(ids) - len(set(ids))

    if duplicate_count == 0:
        status = "PASS"
    else:
        status = "FAIL"

    print(
        f"{dataset_name:<15} "
        f"Records: {len(ids):>8,} | "
        f"Duplicates: {duplicate_count:>5,} | "
        f"{status}"
    )


check_duplicates(users, "user_id", "Users")
check_duplicates(products, "product_id", "Products")
check_duplicates(categories, "category_id", "Categories")
check_duplicates(transactions, "transaction_id", "Transactions")
check_duplicates(sessions, "session_id", "Sessions")

# ---------------------------------------------------------
# CHECK 3: Referential Integrity
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("CHECK 3: REFERENTIAL INTEGRITY")
print("=" * 60)

# Create sets of valid IDs for fast lookup
user_ids = {user["user_id"] for user in users}
product_ids = {product["product_id"] for product in products}
category_ids = {category["category_id"] for category in categories}
session_ids = {session["session_id"] for session in sessions}

# 1. Sessions must reference valid users
invalid_session_users = sum(
    1 for session in sessions
    if session.get("user_id") not in user_ids
)

# 2. Products must reference valid categories
invalid_product_categories = sum(
    1 for product in products
    if product.get("category_id") not in category_ids
)

# 3. Transactions must reference valid users
invalid_transaction_users = sum(
    1 for transaction in transactions
    if transaction.get("user_id") not in user_ids
)

# 4. Non-null transaction session IDs must reference valid sessions
invalid_transaction_sessions = sum(
    1 for transaction in transactions
    if transaction.get("session_id") is not None
    and transaction.get("session_id") not in session_ids
)

# 5. Transaction items must reference valid products
invalid_transaction_products = 0

for transaction in transactions:
    for item in transaction.get("items", []):
        if item.get("product_id") not in product_ids:
            invalid_transaction_products += 1


def print_relationship_result(name, invalid_count):
    status = "PASS" if invalid_count == 0 else "FAIL"

    print(
        f"{name:<35} "
        f"Invalid: {invalid_count:>6,} | "
        f"{status}"
    )


print_relationship_result(
    "Session -> User",
    invalid_session_users
)

print_relationship_result(
    "Product -> Category",
    invalid_product_categories
)

print_relationship_result(
    "Transaction -> User",
    invalid_transaction_users
)

print_relationship_result(
    "Transaction -> Session",
    invalid_transaction_sessions
)

print_relationship_result(
    "Transaction Item -> Product",
    invalid_transaction_products
)

# ---------------------------------------------------------
# CHECK 4: Transaction Arithmetic
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("CHECK 4: TRANSACTION ARITHMETIC")
print("=" * 60)

invalid_item_subtotals = 0
invalid_transaction_subtotals = 0
invalid_transaction_totals = 0
negative_values = 0

TOLERANCE = 0.01

for transaction in transactions:

    calculated_subtotal = 0

    for item in transaction.get("items", []):

        quantity = item.get("quantity", 0)
        unit_price = item.get("unit_price", 0)
        stored_subtotal = item.get("subtotal", 0)

        calculated_item_subtotal = quantity * unit_price

        # Check item arithmetic
        if abs(calculated_item_subtotal - stored_subtotal) > TOLERANCE:
            invalid_item_subtotals += 1

        calculated_subtotal += stored_subtotal

        # Check negative quantity or price
        if quantity < 0 or unit_price < 0 or stored_subtotal < 0:
            negative_values += 1

    # Check transaction subtotal
    stored_transaction_subtotal = transaction.get("subtotal", 0)

    if abs(calculated_subtotal - stored_transaction_subtotal) > TOLERANCE:
        invalid_transaction_subtotals += 1

    # Check final total
    discount = transaction.get("discount", 0)
    stored_total = transaction.get("total", 0)

    calculated_total = stored_transaction_subtotal - discount

    if abs(calculated_total - stored_total) > TOLERANCE:
        invalid_transaction_totals += 1


def arithmetic_result(name, invalid_count):
    status = "PASS" if invalid_count == 0 else "FAIL"

    print(
        f"{name:<35} "
        f"Invalid: {invalid_count:>6,} | "
        f"{status}"
    )


arithmetic_result(
    "Item quantity x price",
    invalid_item_subtotals
)

arithmetic_result(
    "Transaction subtotal",
    invalid_transaction_subtotals
)

arithmetic_result(
    "Subtotal - discount = total",
    invalid_transaction_totals
)

arithmetic_result(
    "Negative financial values",
    negative_values
)

# ---------------------------------------------------------
# CHECK 5: Critical Missing / Null Values
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("CHECK 5: CRITICAL MISSING / NULL VALUES")
print("=" * 60)


def count_missing(records, fields):
    results = {}

    for field in fields:
        missing = sum(
            1 for record in records
            if record.get(field) is None
        )
        results[field] = missing

    return results


critical_fields = {
    "Users": (
        users,
        ["user_id", "registration_date"]
    ),
    "Products": (
        products,
        ["product_id", "category_id", "base_price", "current_stock"]
    ),
    "Categories": (
        categories,
        ["category_id", "name"]
    ),
    "Sessions": (
        sessions,
        ["session_id", "user_id", "start_time", "end_time"]
    ),
    "Transactions": (
        transactions,
        ["transaction_id", "user_id", "timestamp", "subtotal", "total"]
    )
}


for dataset_name, (records, fields) in critical_fields.items():

    missing_results = count_missing(records, fields)

    for field, missing_count in missing_results.items():

        status = "PASS" if missing_count == 0 else "FAIL"

        print(
            f"{dataset_name + '.' + field:<35} "
            f"Missing: {missing_count:>6,} | "
            f"{status}"
        )

# ---------------------------------------------------------
# CHECK 6: Session Time & Conversion Consistency
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("CHECK 6: SESSION TIME & CONVERSION CONSISTENCY")
print("=" * 60)

from datetime import datetime

invalid_session_times = 0
negative_durations = 0
invalid_conversion_status = 0

valid_conversion_statuses = {
    "converted",
    "abandoned",
    "browsed"
}

for session in sessions:

    # Convert ISO timestamp strings into datetime objects
    start_time = datetime.fromisoformat(session["start_time"])
    end_time = datetime.fromisoformat(session["end_time"])

    # End time should never be earlier than start time
    if end_time < start_time:
        invalid_session_times += 1

    # Duration should never be negative
    if session.get("duration", 0) < 0:
        negative_durations += 1

    # Conversion status should contain an expected value
    if session.get("conversion_status") not in valid_conversion_statuses:
        invalid_conversion_status += 1


def consistency_result(name, invalid_count):
    status = "PASS" if invalid_count == 0 else "FAIL"

    print(
        f"{name:<35} "
        f"Invalid: {invalid_count:>6,} | "
        f"{status}"
    )


consistency_result(
    "End time >= start time",
    invalid_session_times
)

consistency_result(
    "Non-negative duration",
    negative_durations
)

consistency_result(
    "Valid conversion status",
    invalid_conversion_status
)