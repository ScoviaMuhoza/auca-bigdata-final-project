"""
03_cohort.py - BATCH JOB 3: cohort analysis of purchasing by REGISTRATION MONTH.
Run:  python spark\\03_cohort.py

Cohort  = users grouped by the month they registered.
Offset  = whole months between registration month and the purchase month.
For every (cohort, offset) we compute revenue, active buyers, spend per cohort user, buyer rate.
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from pyspark.sql import functions as F
from common import get_spark, load_clean, save_csv

spark = get_spark("03_cohort")
d, _ = load_clean(spark)

users = d["users"].select("user_id", "reg_month")
cohort_size = users.groupBy("reg_month").agg(F.count("*").alias("cohort_users"))

purchases = (d["transactions"].select("user_id", "total", F.date_trunc("month", "timestamp").alias("buy_month"))
             .join(users, "user_id")
             .withColumn("month_offset", F.round(F.months_between("buy_month", "reg_month")).cast("int")))

cohort = (purchases.groupBy("reg_month", "month_offset")
          .agg(F.round(F.sum("total"), 2).alias("revenue"),
               F.countDistinct("user_id").alias("buyers"),
               F.count("*").alias("orders"))
          .join(F.broadcast(cohort_size), "reg_month")
          .withColumn("spend_per_cohort_user", F.round(F.col("revenue") / F.col("cohort_users"), 2))
          .withColumn("buyer_rate", F.round(F.col("buyers") / F.col("cohort_users"), 3))
          .withColumn("cohort", F.date_format("reg_month", "yyyy-MM"))
          .orderBy("reg_month", "month_offset")).cache()

print("\n=== COHORT TABLE (long format) ===")
cohort.select("cohort", "month_offset", "cohort_users", "buyers", "orders", "revenue",
              "spend_per_cohort_user", "buyer_rate").show(60, truncate=False)

print("=== Spend per cohort user: cohort (rows) x months since registration (columns) ===")
pivot = cohort.groupBy("cohort").pivot("month_offset").agg(F.first("spend_per_cohort_user")).orderBy("cohort")
pivot.show(truncate=False)

save_csv(cohort.drop("reg_month"), "cohort_analysis.csv")
spark.stop()
