"""
02_recommendations.py - BATCH JOB 2: recommendation indicators (market-basket style).
Run:  python spark\\02_recommendations.py

For two items A and B we compute:
    support(A,B)      = share of baskets that contain BOTH
    confidence(A->B)  = P(B | A) = baskets with A and B / baskets with A   ("users who bought A also bought B")
    lift(A,B)         = P(A and B) / (P(A) * P(B))
                        lift > 1: appear together MORE than chance;  lift ~ 1: independent
A "basket" = a transaction (co-purchase) or a session (products viewed together).
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from pyspark.sql import functions as F
from common import get_spark, load_clean, transaction_items, save_csv

spark = get_spark("02_recommendations")
d, _ = load_clean(spark, dq_checks=False)   # skip the DQ counting jobs: faster start


def affinity(baskets, min_pairs):
    """baskets: DataFrame(basket_id, item). Returns pair statistics with support/confidence/lift."""
    baskets = baskets.distinct()
    n_baskets = baskets.select("basket_id").distinct().count()
    item_cnt = baskets.groupBy("item").agg(F.count("*").alias("n"))
    a, b = baskets.alias("a"), baskets.alias("b")
    # self-join inside each basket; a.item < b.item keeps each pair once (A,B) and drops (A,A)
    pairs = (a.join(b, (F.col("a.basket_id") == F.col("b.basket_id")) & (F.col("a.item") < F.col("b.item")))
             .groupBy(F.col("a.item").alias("item_a"), F.col("b.item").alias("item_b"))
             .agg(F.count("*").alias("pair_count"))
             .filter(F.col("pair_count") >= min_pairs))                 # drop rare pairs early (less shuffle)
    ca = item_cnt.select(F.col("item").alias("item_a"), F.col("n").alias("n_a"))
    cb = item_cnt.select(F.col("item").alias("item_b"), F.col("n").alias("n_b"))
    return (pairs.join(F.broadcast(ca), "item_a").join(F.broadcast(cb), "item_b")
            .withColumn("support", F.round(F.col("pair_count") / n_baskets, 6))
            .withColumn("confidence_a_to_b", F.round(F.col("pair_count") / F.col("n_a"), 4))
            .withColumn("confidence_b_to_a", F.round(F.col("pair_count") / F.col("n_b"), 4))
            .withColumn("lift", F.round(F.col("pair_count") * n_baskets / (F.col("n_a") * F.col("n_b")), 3))
            .select("item_a", "item_b", "pair_count", "n_a", "n_b", "support",
                    "confidence_a_to_b", "confidence_b_to_a", "lift")
            ), n_baskets


items = transaction_items(d["transactions"], d["products"], d["categories"]).cache()

# ---- A) products bought together (transactions) ----
bought = items.select(F.col("transaction_id").alias("basket_id"), F.col("product_id").alias("item"))
res, n = affinity(bought, min_pairs=2)
print(f"\n=== A) PRODUCTS BOUGHT TOGETHER ({n:,} transactions, pairs seen >= 2 times) ===")
top = res.orderBy(F.desc("pair_count"), F.desc("lift")).limit(15)
top.show(15, truncate=False); save_csv(top, "reco_products_bought_together.csv")

# ---- B) products viewed together (sessions) ----
viewed = (d["sessions"].select(F.col("session_id").alias("basket_id"),
                               F.explode("viewed_products").alias("item")))
res_v, n_v = affinity(viewed, min_pairs=3)
print(f"=== B) PRODUCTS VIEWED TOGETHER ({n_v:,} sessions, pairs seen >= 3 times) ===")
top_v = res_v.orderBy(F.desc("pair_count"), F.desc("lift")).limit(15)
top_v.show(15, truncate=False); save_csv(top_v, "reco_products_viewed_together.csv")

# ---- C) category affinity: customers buying in category A also buy in category B ----
cats = items.select(F.col("transaction_id").alias("basket_id"), F.col("category_name").alias("item")).filter("item is not null")
res_c, n_c = affinity(cats, min_pairs=1)
print("=== C) CATEGORY AFFINITY (more data per pair -> more reliable) ===")
top_c = res_c.orderBy(F.desc("lift")).limit(10)
top_c.show(10, truncate=False); save_csv(top_c, "reco_category_affinity.csv")
print("Lift values close to 1.0 mean items appear together about as often as chance.")

# ---- D) simple recommender: 'users who bought X also bought ...' for the best-selling product ----
best = bought.groupBy("item").count().orderBy(F.desc("count")).first()["item"]
rec = (res.filter((F.col("item_a") == best) | (F.col("item_b") == best))
       .withColumn("recommended", F.when(F.col("item_a") == best, F.col("item_b")).otherwise(F.col("item_a")))
       .withColumn("confidence", F.when(F.col("item_a") == best, F.col("confidence_a_to_b")).otherwise(F.col("confidence_b_to_a")))
       .select("recommended", "pair_count", "confidence", "lift").orderBy(F.desc("pair_count")).limit(5))
print(f"=== D) Recommendations for best-selling product {best} ===")
rec.show(truncate=False)
spark.stop()
