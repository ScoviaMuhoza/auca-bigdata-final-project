"""
load_mongo.py - loads the e-commerce dataset into MongoDB with a document model
designed for analytics.

Run from the project root:
    python mongodb\\load_mongo.py              (reads JSON from current folder)
    python mongodb\\load_mongo.py data_small   (reads JSON from data_small)

DESIGN (be ready to explain this):
  categories   : reference data, one document per category with its subcategories embedded
  products     : one document per product, price_history EMBEDDED (always read with the product),
                 category name DENORMALIZED into the product (avoids a join on every query)
  users        : profile + geo_data EMBEDDED + a pre-computed purchase_summary
                 (frequently read together, so we compute it once at load time)
  transactions : line items EMBEDDED as an array (a transaction is always read with its items);
                 category_id is copied into each item so revenue-by-category needs no join
Sessions are NOT loaded here: they are high-volume time-series, so they go to HBase.
"""
import json, os, sys
from datetime import datetime
from collections import defaultdict
from pymongo import MongoClient, ASCENDING

DATA_DIR = sys.argv[1] if len(sys.argv) > 1 else "."
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "ecommerce"
BATCH = 5000


def load(name):
    with open(os.path.join(DATA_DIR, name), encoding="utf-8") as f:
        return json.load(f)


def to_dt(value):
    """ISO string -> real datetime (BSON Date) so MongoDB can group/sort by time."""
    return datetime.fromisoformat(value) if value else None


def insert_in_batches(collection, docs):
    for i in range(0, len(docs), BATCH):
        collection.insert_many(docs[i:i + BATCH], ordered=False)


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    print(f"Reading from '{DATA_DIR}' and writing to database '{DB_NAME}'")

    # Start clean so the script can be re-run safely
    for col in ("categories", "products", "users", "transactions"):
        db[col].drop()

    # ---------- categories ----------
    categories = load("categories.json")
    cat_name = {c["category_id"]: c["name"] for c in categories}
    db.categories.insert_many(categories)
    print(f"categories   : {len(categories)}")

    # ---------- products (denormalize category name, convert dates) ----------
    products = load("products.json")
    prod_cat = {}
    for p in products:
        p["category"] = {"category_id": p["category_id"], "name": cat_name.get(p["category_id"])}
        p["creation_date"] = to_dt(p["creation_date"])
        for ph in p["price_history"]:
            ph["date"] = to_dt(ph["date"])
        prod_cat[p["product_id"]] = p["category_id"]
    insert_in_batches(db.products, products)
    print(f"products     : {len(products)}")

    # ---------- transactions (embed items, add category_id per item) ----------
    transactions = load("transactions.json")
    summary = defaultdict(lambda: {"total_orders": 0, "total_spent": 0.0,
                                   "first_purchase": None, "last_purchase": None})
    for t in transactions:
        t["timestamp"] = to_dt(t["timestamp"])
        t["total_items"] = sum(i["quantity"] for i in t["items"])
        for item in t["items"]:
            item["category_id"] = prod_cat.get(item["product_id"])
        s = summary[t["user_id"]]
        s["total_orders"] += 1
        s["total_spent"] = round(s["total_spent"] + t["total"], 2)
        s["first_purchase"] = t["timestamp"] if s["first_purchase"] is None else min(s["first_purchase"], t["timestamp"])
        s["last_purchase"] = t["timestamp"] if s["last_purchase"] is None else max(s["last_purchase"], t["timestamp"])
    insert_in_batches(db.transactions, transactions)
    print(f"transactions : {len(transactions)}")

    # ---------- users (embed purchase_summary computed above) ----------
    users = load("users.json")
    for u in users:
        u["registration_date"] = to_dt(u["registration_date"])
        u["last_active"] = to_dt(u["last_active"])
        u["purchase_summary"] = summary.get(
            u["user_id"], {"total_orders": 0, "total_spent": 0.0,
                           "first_purchase": None, "last_purchase": None})
    insert_in_batches(db.users, users)
    print(f"users        : {len(users)}")

    # ---------- indexes (match our query patterns) ----------
    db.transactions.create_index([("timestamp", ASCENDING)])
    db.transactions.create_index([("user_id", ASCENDING)])
    db.transactions.create_index([("items.product_id", ASCENDING)])
    db.products.create_index([("category_id", ASCENDING)])
    db.users.create_index([("purchase_summary.total_orders", ASCENDING)])
    print("indexes created. DONE.")


if __name__ == "__main__":
    main()
