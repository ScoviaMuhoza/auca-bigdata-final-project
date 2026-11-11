"""
funnel_integration.py - PART 3: integrated analytics across MongoDB + HBase + Spark.

BUSINESS QUESTION
  Where do shoppers drop out between browsing and paying, and which product categories,
  devices and weeks convert worst - using only VERIFIED purchases?

DATA SOURCES (why each system is used)
  HBase   user_sessions  -> browsing behaviour: page views, cart contents, device, referrer
                            (high-volume time-series; read with one scan)
  MongoDB transactions   -> the ground truth of what was really bought (embedded items)
  MongoDB products       -> product -> category mapping
  Spark                  -> joins data coming from the two systems and runs the aggregations

WORKFLOW
  1. EXTRACT   happybase scan of HBase (only the column families we need)
  2. EXTRACT   pymongo query on MongoDB (only the fields we need)
  3. LOAD      both result sets into Spark DataFrames (explicit schemas)
  4. JOIN      sessions (HBase)  LEFT JOIN  transactions (MongoDB)  ON session_id
  5. AGGREGATE overall funnel, funnel by device, by week and by category

Run from the project root:
    python integration\\funnel_integration.py            (all sessions found in HBase)
    python integration\\funnel_integration.py 20000      (only the first 20,000 - quick test)
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark"))
import pandas as pd
from pyspark.sql import functions as F, types as T
from common import get_spark, save_csv

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
HBASE_PORT = int(os.getenv("HBASE_PORT", 9090))


# ------------------------------------------------------------------ 1. HBase extract
def extract_hbase_sessions(table, limit=None):
    """Scan user_sessions and turn every HBase row into one flat tuple."""
    rows = []
    for key, data in table.scan(columns=[b"info", b"pv", b"cart"], batch_size=500, limit=limit):
        get = lambda col: data.get(b"info:" + col, b"").decode()
        viewed_cats, reached_checkout = set(), False
        for col, val in data.items():
            if col.startswith(b"pv:"):
                page_type, product_id, category_id, _dur, _ts = val.decode().split("|")
                if page_type == "product_detail" and category_id:
                    viewed_cats.add(category_id)
                if page_type == "checkout":
                    reached_checkout = True
        cart_products = [col[5:].decode() for col in data if col.startswith(b"cart:")]
        rows.append((get(b"session_id"), key.decode().split("#")[0], get(b"start_time"),
                     get(b"device_type"), get(b"referrer"), get(b"status"),
                     sorted(viewed_cats), cart_products, reached_checkout))
    return rows


SESSION_SCHEMA = T.StructType([
    T.StructField("session_id", T.StringType()), T.StructField("user_id", T.StringType()),
    T.StructField("start_time", T.StringType()), T.StructField("device_type", T.StringType()),
    T.StructField("referrer", T.StringType()), T.StructField("label", T.StringType()),
    T.StructField("viewed_cats", T.ArrayType(T.StringType())),
    T.StructField("cart_products", T.ArrayType(T.StringType())),
    T.StructField("reached_checkout", T.BooleanType())])


# ------------------------------------------------------------------ 2. MongoDB extract
def extract_mongo(db):
    txns = []
    for t in db.transactions.find({"session_id": {"$ne": None}},
                                  {"_id": 0, "transaction_id": 1, "session_id": 1, "total": 1,
                                   "items.category_id": 1}):
        cats = sorted({i["category_id"] for i in t["items"] if i.get("category_id")})
        txns.append((t["transaction_id"], t["session_id"], float(t["total"]), cats))
    products = [(p["product_id"], p["category_id"]) for p in
                db.products.find({}, {"_id": 0, "product_id": 1, "category_id": 1})]
    categories = [(c["category_id"], c["name"]) for c in
                  db.categories.find({}, {"_id": 0, "category_id": 1, "name": 1})]
    return txns, products, categories


TXN_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()), T.StructField("session_id", T.StringType()),
    T.StructField("total", T.DoubleType()), T.StructField("txn_cats", T.ArrayType(T.StringType()))])


def pct(a, b):
    return round(100.0 * a / b, 1) if b else None


def main(hbase_table, mongo_db, limit=None):
    spark = get_spark("funnel_integration")

    # ---- extract + load ----
    print("Extracting sessions from HBase ...")
    sess_rows = extract_hbase_sessions(hbase_table, limit)
    print(f"  {len(sess_rows):,} sessions read from HBase")
    print("Extracting transactions/products from MongoDB ...")
    txns, products, categories = extract_mongo(mongo_db)
    print(f"  {len(txns):,} session-linked transactions, {len(products):,} products")

    sessions = spark.createDataFrame(sess_rows, SESSION_SCHEMA)
    txn_df = spark.createDataFrame(txns, TXN_SCHEMA)
    prod_df = spark.createDataFrame(products, ["product_id", "category_id"])
    cat_df = spark.createDataFrame(categories, ["category_id", "category_name"])

    # ---- cross-system join: HBase session  LEFT JOIN  MongoDB transaction ----
    s = (sessions
         .join(F.broadcast(txn_df.select("session_id", "transaction_id", "total")), "session_id", "left")
         .withColumn("viewed_product", F.size("viewed_cats") > 0)
         .withColumn("has_cart", F.size("cart_products") > 0)
         .withColumn("purchased", F.col("transaction_id").isNotNull())      # VERIFIED purchase
         .withColumn("week", F.date_format(F.date_trunc("week", F.to_timestamp("start_time")), "yyyy-MM-dd"))
         ).cache()

    # ---- (a) overall funnel ----
    r = s.agg(F.count("*").alias("all"),
              F.sum(F.col("viewed_product").cast("int")).alias("viewed"),
              F.sum(F.col("has_cart").cast("int")).alias("cart"),
              F.sum(F.col("reached_checkout").cast("int")).alias("checkout"),
              F.sum(F.col("purchased").cast("int")).alias("purchased"),
              F.sum((F.col("label") == "converted").cast("int")).alias("label_converted"),
              F.sum(((F.col("label") == "converted") & F.col("purchased")).cast("int")).alias("label_and_verified")
              ).first()
    stages = [("1 sessions", r["all"]), ("2 viewed a product", r["viewed"]), ("3 cart not empty", r["cart"]),
              ("4 reached checkout", r["checkout"]), ("5 verified purchase (MongoDB)", r["purchased"])]
    funnel = pd.DataFrame({
        "stage": [x[0] for x in stages], "sessions": [x[1] for x in stages],
        "pct_of_all": [pct(x[1], r["all"]) for x in stages],
        "pct_of_previous_stage": [None] + [pct(stages[i][1], stages[i - 1][1]) for i in range(1, 5)]})
    print("\n=== (a) OVERALL FUNNEL (HBase sessions joined with MongoDB transactions) ===")
    print(funnel.to_string(index=False))
    funnel.to_csv("output/integration_funnel.csv", index=False)
    print(f"\nHBase label says 'converted' for {r['label_converted']:,} sessions, but only "
          f"{r['label_and_verified']:,} have a real transaction in MongoDB "
          f"-> the label overstates conversion by {pct(r['label_converted'] - r['label_and_verified'], r['label_converted'])}%.")

    # ---- (b) by device, (c) by week ----
    def stage_table(group_col):
        return (s.groupBy(group_col)
                .agg(F.count("*").alias("sessions"),
                     F.sum(F.col("has_cart").cast("int")).alias("with_cart"),
                     F.sum(F.col("reached_checkout").cast("int")).alias("checkout"),
                     F.sum(F.col("purchased").cast("int")).alias("purchased"))
                .withColumn("cart_to_purchase_pct", F.round(100 * F.col("purchased") / F.col("with_cart"), 1))
                .withColumn("overall_conversion_pct", F.round(100 * F.col("purchased") / F.col("sessions"), 1))
                .orderBy(group_col))

    dev = stage_table("device_type")
    print("\n=== (b) FUNNEL BY DEVICE ===");  dev.show(truncate=False); save_csv(dev, "integration_funnel_by_device.csv")
    wk = stage_table("week")
    print("=== (c) FUNNEL BY WEEK (first/last week may be partial) ==="); wk.show(20, truncate=False)
    save_csv(wk, "integration_funnel_by_week.csv")

    # ---- (d) funnel by product CATEGORY: view -> cart -> purchase ----
    viewed = s.select("session_id", F.explode("viewed_cats").alias("category_id")).distinct()
    carted = (s.select("session_id", F.explode("cart_products").alias("product_id"))
              .join(F.broadcast(prod_df), "product_id").select("session_id", "category_id").distinct())
    bought = (s.filter("purchased").select("session_id")
              .join(txn_df.select("session_id", F.explode("txn_cats").alias("category_id")), "session_id")
              .distinct())
    cnt = lambda df, name: df.groupBy("category_id").agg(F.count("*").alias(name))
    cat = (cnt(viewed, "viewing_sessions").join(cnt(carted, "cart_sessions"), "category_id", "left")
           .join(cnt(bought, "purchasing_sessions"), "category_id", "left")
           .na.fill(0).join(F.broadcast(cat_df), "category_id")
           .withColumn("view_to_cart_pct", F.round(100 * F.col("cart_sessions") / F.col("viewing_sessions"), 1))
           .withColumn("cart_to_purchase_pct", F.round(100 * F.col("purchasing_sessions") / F.col("cart_sessions"), 1))
           .select("category_name", "viewing_sessions", "cart_sessions", "purchasing_sessions",
                   "view_to_cart_pct", "cart_to_purchase_pct")
           .orderBy(F.desc("cart_to_purchase_pct")))
    print("=== (d) FUNNEL BY CATEGORY (best and worst cart-to-purchase) ===")
    cat.show(8, truncate=False)
    print("... worst:"); cat.orderBy("cart_to_purchase_pct").show(5, truncate=False)
    save_csv(cat, "integration_funnel_by_category.csv")
    spark.stop()


if __name__ == "__main__":
    import happybase
    from pymongo import MongoClient
    lim = int(sys.argv[1]) if len(sys.argv) > 1 else None
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=120000)
    main(conn.table("user_sessions"), MongoClient(MONGO_URI)["ecommerce"], lim)
