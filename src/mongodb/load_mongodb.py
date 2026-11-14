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
DATABASE_NAME = "ecommerce_analytics"


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
transactions = load_json("transactions.json")

print(f"Users loaded from JSON: {len(users):,}")
print(f"Products loaded from JSON: {len(products):,}")
print(f"Categories loaded from JSON: {len(categories):,}")
print(f"Transactions loaded from JSON: {len(transactions):,}")


# ---------------------------------------------------------
# MongoDB collections
# ---------------------------------------------------------

collections = {
    "users": users,
    "products": products,
    "categories": categories,
    "transactions": transactions
}


# ---------------------------------------------------------
# Insert datasets into MongoDB
# ---------------------------------------------------------

print("\nInserting datasets into MongoDB...")

for collection_name, records in collections.items():

    collection = db[collection_name]

    # Make the script safe to run again during development.
    # Existing development data is removed before reloading.
    collection.delete_many({})

    if records:
        result = collection.insert_many(records)

        print(
            f"{collection_name:<15} "
            f"Inserted: {len(result.inserted_ids):>6,}"
        )
    else:
        print(f"{collection_name:<15} No records found.")


print("\nMongoDB data loading completed successfully.")

client.close()