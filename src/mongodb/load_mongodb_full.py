import json
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# ---------------------------------------------------------
# AUCA E-commerce Big Data Project
# MongoDB Data Loading Script
# ---------------------------------------------------------

# Locate project directories
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"

# MongoDB configuration
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "ecommerce_analytics_full"

def load_json(filename):
    """
    Load a JSON dataset from the project's data directory.
    """
    file_path = DATA_DIR / filename

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


# Connect to MongoDB
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

    # Force MongoDB to respond so we know the connection works
    client.admin.command("ping")

    print("MongoDB connection successful.")

except ConnectionFailure as error:
    print(f"MongoDB connection failed: {error}")
    raise


# Select database
db = client[DATABASE_NAME]

print(f"Using database: {DATABASE_NAME}")

# ---------------------------------------------------------
# Load datasets
# ---------------------------------------------------------

print("\nLoading JSON datasets...")

users = load_json("users.json")
products = load_json("products.json")
categories = load_json("categories.json")

# Find all transaction batch files
transaction_files = sorted(
    DATA_DIR.glob("transactions_*.json"),
    key=lambda p: int(p.stem.split("_")[-1])
)

print(f"Transaction batches found: {len(transaction_files)}")

if len(transaction_files) != 50:
    raise ValueError(
        f"Expected 50 transaction batches, found {len(transaction_files)}"
    )
print(f"Users loaded from JSON: {len(users):,}")
print(f"Products loaded from JSON: {len(products):,}")
print(f"Categories loaded from JSON: {len(categories):,}")


# ---------------------------------------------------------
# MongoDB collections
# ---------------------------------------------------------

collections = {
    "users": users,
    "products": products,
    "categories": categories
}

# ---------------------------------------------------------
# Insert datasets into MongoDB
# ---------------------------------------------------------

print("\nInserting datasets into MongoDB...")

for collection_name, records in collections.items():

    collection = db[collection_name]

      # Prevent accidental replacement of existing MongoDB data.
      # Stop loading if this collection already contains records.

    if collection.count_documents({}) > 0:
    	raise RuntimeError(
        f"Collection '{collection_name}' already contains data. "
        "Loading stopped to prevent accidental replacement."
    )

    if records:
        result = collection.insert_many(records)

        print(
            f"{collection_name:<15} "
            f"Inserted: {len(result.inserted_ids):>6,}"
        )
    else:
        print(f"{collection_name:<15} No records found.")

# ------------------------------------------------------------
# Load transactions in batches to reduce RAM usage
# ------------------------------------------------------------

print("\nLoading transaction batches...")

transaction_collection = db["transactions"]

# Prevent duplicate loading
if transaction_collection.count_documents({}) > 0:
    raise RuntimeError(
        "Transactions collection already contains data. "
        "Loading stopped to prevent duplicate records."
    )

total_inserted = 0

for batch_number, file_path in enumerate(transaction_files, start=1):

    # Load one JSON file at a time
    with open(file_path, "r", encoding="utf-8") as file:
        batch = json.load(file)

    if not batch:
        continue

    # Insert transactions into MongoDB
    result = transaction_collection.insert_many(batch)

    total_inserted += len(result.inserted_ids)

    print(
        f"Batch {batch_number}/{len(transaction_files)} | "
        f"Inserted: {len(result.inserted_ids):,} | "
        f"Total: {total_inserted:,}"
    )

    # Release memory
    del batch

print(f"\nTotal transactions inserted: {total_inserted:,}")

if total_inserted != 500000:
    raise ValueError(
        f"Expected 500,000 transactions, but inserted {total_inserted:,}"
    )
print("\nMongoDB data loading completed successfully.")

client.close()