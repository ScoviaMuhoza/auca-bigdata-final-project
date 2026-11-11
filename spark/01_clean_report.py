"""
01_clean_report.py - BATCH JOB 1: clean + normalize the raw dataset and report data quality.
Run:  python spark\\01_clean_report.py
Outputs: output\\dq_report.csv and cleaned flat CSVs in output\\clean\\ (used later for charts).
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from pyspark.sql import functions as F
import pandas as pd
from common import get_spark, load_clean, transaction_items, OUT_DIR

spark = get_spark("01_clean_report")
d, dq = load_clean(spark)

print("\n=== DATA QUALITY REPORT ===")
for k, v in dq.items():
    print(f"{k:45s} {v:>10,}")
pd.DataFrame(list(dq.items()), columns=["check", "count"]).to_csv(os.path.join(OUT_DIR, "dq_report.csv"), index=False)

print("\n=== Schemas after cleaning ===")
d["sessions"].drop("page_views", "cart_contents", "viewed_products").printSchema()

# ---- export the cleaned, FLAT tables (arrays removed) for the visualisation step ----
clean_dir = os.path.join(OUT_DIR, "clean"); os.makedirs(clean_dir, exist_ok=True)
flat = {
    "users": d["users"],
    "products": d["products"],
    "transactions": d["transactions"].drop("items"),
    "transaction_items": transaction_items(d["transactions"], d["products"], d["categories"]),
    "sessions": d["sessions"].select(
        "session_id", "user_id", "start_time", "duration_seconds", "country", "device_type", "os",
        "browser", "referrer", "conversion_status", "has_txn", "verified_conversion", "cart_value",
        F.size("page_views").alias("n_page_views"), F.size("viewed_products").alias("n_products_viewed")),
}
for name, df in flat.items():
    df.toPandas().to_csv(os.path.join(clean_dir, f"{name}.csv"), index=False)
    print(f"  exported {name}.csv")
spark.stop()
