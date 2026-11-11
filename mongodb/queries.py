"""
queries.py - three MongoDB aggregation pipelines for the report.
Run from the project root:  python mongodb\\queries.py
"""
import os
from pymongo import MongoClient

db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))["ecommerce"]


def show(title, cursor):
    print(f"\n=== {title} ===")
    for doc in cursor:
        print(doc)


# 1) REVENUE ANALYTICS: monthly gross revenue per category (top 10 rows)
#    $unwind turns each line item into its own row, $group sums it, $lookup adds the name.
revenue_by_category_month = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": {"category_id": "$items.category_id",
                "month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}}},
        "revenue": {"$sum": "$items.subtotal"},
        "units_sold": {"$sum": "$items.quantity"},
        "orders": {"$sum": 1}}},
    {"$lookup": {"from": "categories", "localField": "_id.category_id",
                 "foreignField": "category_id", "as": "cat"}},
    {"$project": {"_id": 0, "month": "$_id.month", "category_id": "$_id.category_id",
                  "category_name": {"$arrayElemAt": ["$cat.name", 0]},
                  "revenue": {"$round": ["$revenue", 2]}, "units_sold": 1, "orders": 1}},
    {"$sort": {"revenue": -1}},
    {"$limit": 10},
]

# 2) USER SEGMENTATION by purchase frequency (users have no age in this dataset)
#    $bucket groups users into frequency bands and computes size and average spend.
segmentation_by_frequency = [
    {"$bucket": {
        "groupBy": "$purchase_summary.total_orders",
        "boundaries": [0, 1, 3, 5, 8, 12, 1000],
        "default": "other",
        "output": {"users": {"$sum": 1},
                   "avg_spent": {"$avg": "$purchase_summary.total_spent"},
                   "total_spent": {"$sum": "$purchase_summary.total_spent"}}}},
    {"$project": {"users": 1, "avg_spent": {"$round": ["$avg_spent", 2]},
                  "total_spent": {"$round": ["$total_spent", 2]}}},
]
LABELS = {0: "0 orders (never bought)", 1: "1-2 orders", 3: "3-4 orders",
          5: "5-7 orders", 8: "8-11 orders", 12: "12+ orders"}

# 3) PRODUCT POPULARITY: top 10 best-selling products by units sold
top_products = [
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.product_id",
                "units_sold": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"}}},
    {"$sort": {"units_sold": -1}},
    {"$limit": 10},
    {"$lookup": {"from": "products", "localField": "_id",
                 "foreignField": "product_id", "as": "p"}},
    {"$project": {"_id": 0, "product_id": "$_id",
                  "name": {"$arrayElemAt": ["$p.name", 0]},
                  "category": {"$arrayElemAt": ["$p.category.name", 0]},
                  "units_sold": 1, "revenue": {"$round": ["$revenue", 2]}}},
]

if __name__ == "__main__":
    show("Q1 Top revenue (month x category)", db.transactions.aggregate(revenue_by_category_month))
    print("\n=== Q2 User segmentation by purchase frequency ===")
    for d in db.users.aggregate(segmentation_by_frequency):
        print(LABELS.get(d["_id"], d["_id"]), "->", {k: v for k, v in d.items() if k != "_id"})
    show("Q3 Top 10 best-selling products", db.transactions.aggregate(top_products))
