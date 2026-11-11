"""
04_spark_sql.py - SPARK SQL analytics on the cleaned DataFrames.
Run:  python spark\\04_spark_sql.py

The temp views are named after WHERE the data would live in the real architecture:
    mongo_users, mongo_transactions, mongo_items, mongo_products  -> MongoDB collections
    hbase_sessions                                                -> HBase table user_sessions
Joining them in SQL shows the logic of a cross-system query. (Part 3 loads the data from the
real databases with Python connectors and runs this same style of query.)
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from common import get_spark, load_clean, transaction_items, save_csv

spark = get_spark("04_spark_sql")
d, _ = load_clean(spark)

d["users"].createOrReplaceTempView("mongo_users")
d["products"].createOrReplaceTempView("mongo_products")
d["categories"].createOrReplaceTempView("mongo_categories")
d["transactions"].createOrReplaceTempView("mongo_transactions")
transaction_items(d["transactions"], d["products"], d["categories"]).createOrReplaceTempView("mongo_items")
d["sessions"].createOrReplaceTempView("hbase_sessions")


def run(title, sql, csv=None, rows=15):
    print(f"\n=== {title} ===")
    df = spark.sql(sql)
    df.show(rows, truncate=False)
    if csv:
        save_csv(df, csv)


# Q1  window function: the TOP 3 categories by revenue inside each month
run("Q1 Top 3 categories per month (window function ROW_NUMBER)", """
    SELECT month, category_name, revenue, rank FROM (
        SELECT date_format(timestamp, 'yyyy-MM') AS month, category_name,
               ROUND(SUM(subtotal), 2) AS revenue,
               ROW_NUMBER() OVER (PARTITION BY date_format(timestamp, 'yyyy-MM')
                                  ORDER BY SUM(subtotal) DESC) AS rank
        FROM mongo_items GROUP BY date_format(timestamp, 'yyyy-MM'), category_name
    ) WHERE rank <= 3 ORDER BY month, rank
""", "sql_top_categories_per_month.csv", 30)

# Q2  conversion by device and referrer (uses VERIFIED conversions only)
run("Q2 Conversion rate by device and referrer", """
    SELECT device_type, referrer, COUNT(*) AS sessions,
           SUM(CASE WHEN has_cart THEN 1 ELSE 0 END) AS sessions_with_cart,
           SUM(CASE WHEN verified_conversion THEN 1 ELSE 0 END) AS verified_orders,
           ROUND(100.0 * SUM(CASE WHEN verified_conversion THEN 1 ELSE 0 END) / COUNT(*), 2) AS conversion_pct
    FROM hbase_sessions GROUP BY device_type, referrer ORDER BY conversion_pct DESC
""", "sql_conversion_device_referrer.csv", 20)

# Q3  CROSS-SYSTEM JOIN: MongoDB users + MongoDB transactions + HBase sessions -> value tiers
run("Q3 Customer value tiers (users + transactions + sessions, NTILE)", """
    WITH spend AS (
        SELECT user_id, COUNT(*) AS orders, SUM(total) AS revenue FROM mongo_transactions GROUP BY user_id),
    engagement AS (
        SELECT user_id, COUNT(*) AS sessions, ROUND(AVG(duration_seconds), 0) AS avg_session_sec
        FROM hbase_sessions GROUP BY user_id),
    scored AS (
        SELECT u.user_id, u.country, COALESCE(s.orders, 0) AS orders, COALESCE(s.revenue, 0) AS revenue,
               COALESCE(e.sessions, 0) AS sessions, e.avg_session_sec,
               NTILE(4) OVER (ORDER BY COALESCE(s.revenue, 0)) AS value_quartile
        FROM mongo_users u LEFT JOIN spend s ON u.user_id = s.user_id
                           LEFT JOIN engagement e ON u.user_id = e.user_id)
    SELECT value_quartile, COUNT(*) AS users, ROUND(AVG(revenue), 2) AS avg_revenue,
           ROUND(AVG(orders), 1) AS avg_orders, ROUND(AVG(sessions), 1) AS avg_sessions,
           ROUND(AVG(avg_session_sec), 0) AS avg_session_sec
    FROM scored GROUP BY value_quartile ORDER BY value_quartile
""", "sql_value_tiers.csv")

# Q4  funnel using the higher-order function EXISTS on the page_views array
run("Q4 Page-level conversion funnel", """
    SELECT stage_no, stage, sessions,
           ROUND(100.0 * sessions / FIRST_VALUE(sessions) OVER (ORDER BY stage_no), 1) AS pct_of_all_sessions
    FROM (
        SELECT 1 AS stage_no, 'any session' AS stage, COUNT(*) AS sessions FROM hbase_sessions
        UNION ALL SELECT 2, 'viewed a product', COUNT(*) FROM hbase_sessions
                  WHERE exists(page_views, p -> p.page_type = 'product_detail')
        UNION ALL SELECT 3, 'cart not empty', COUNT(*) FROM hbase_sessions WHERE has_cart
        UNION ALL SELECT 4, 'reached checkout', COUNT(*) FROM hbase_sessions
                  WHERE exists(page_views, p -> p.page_type = 'checkout')
        UNION ALL SELECT 5, 'verified purchase', COUNT(*) FROM hbase_sessions WHERE verified_conversion
    ) ORDER BY stage_no
""", "sql_funnel.csv")

# Optimisation evidence: the plan shows a BroadcastHashJoin for the small dimension table
print("\n=== Physical plan of a join with a small table (look for BroadcastHashJoin) ===")
spark.sql("""SELECT c.category_name, COUNT(*) FROM mongo_items i JOIN mongo_categories c
             ON i.category_id = c.category_id GROUP BY c.category_name""").explain()
spark.stop()
