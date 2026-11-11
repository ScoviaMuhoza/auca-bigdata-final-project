# Sample queries and expected results

All results below come from the full generated dataset (10,000 users, 5,000 products, 25 categories,
200,000 sessions, 50,000 transactions). Exact figures can differ slightly if you generate a new dataset,
because the generator is random and dates are relative to the day it is run.

## 1. MongoDB (run in `mongosh`, database `ecommerce`, after `python mongodb\load_mongo.py`)

Open the shell: `docker exec -it bd-mongo mongosh ecommerce`

**Top 10 best-selling products** (`mongodb/queries.py`, Q3)
```js
db.transactions.aggregate([
  { $unwind: "$items" },
  { $group: { _id: "$items.product_id", units_sold: { $sum: "$items.quantity" },
              revenue: { $sum: "$items.subtotal" } } },
  { $sort: { units_sold: -1 } }, { $limit: 10 }
])
```
Expected: the top product is `prod_01751` with 102 units and about 33,779 revenue.

**User segmentation by purchase frequency** (Q2)
```js
db.users.aggregate([
  { $bucket: { groupBy: "$purchase_summary.total_orders", boundaries: [0, 1, 3, 5, 8, 12, 1000],
               default: "other",
               output: { users: { $sum: 1 }, avg_spent: { $avg: "$purchase_summary.total_spent" } } } }
])
```
Expected users per band (0, 1-2, 3-4, 5-7, 8-11, 12+): 58, 1,176, 3,155, 4,326, 1,226, 59.
Average spend rises from 0 to about 13,916.

**Monthly revenue by category** (Q1) is in `mongodb/queries.py` (`revenue_by_category_month`):
`$unwind` items, `$group` by category and month, `$lookup` the category name.
Expected: `Kelly-Santiago` leads in 2026-10 with about 937,121 revenue.

## 2. HBase (shell: `docker exec -it bd-hbase hbase shell`, after `python hbase\load_hbase.py`)

```
list
scan 'user_sessions', {ROWPREFIXFILTER => 'user_000042#', LIMIT => 3, COLUMNS => ['info']}
scan 'product_daily_metrics', {STARTROW => 'prod_00123#20260901', STOPROW => 'prod_00123#20261001'}
```
The first scan returns the 3 most recent sessions of one user (newest first, because of the reverse-timestamp
row key). The same queries from Python: `python hbase\hbase_queries.py user_000042`.
The full schema creation commands are in `hbase/hbase_shell_commands.txt`.

## 3. Spark SQL (`spark/04_spark_sql.py`)

**Page-level funnel**
```sql
SELECT stage_no, stage, sessions FROM (
  SELECT 1 AS stage_no, 'any session' AS stage, COUNT(*) AS sessions FROM hbase_sessions
  UNION ALL SELECT 2, 'viewed a product', COUNT(*) FROM hbase_sessions
            WHERE exists(page_views, p -> p.page_type = 'product_detail')
  UNION ALL SELECT 3, 'cart not empty', COUNT(*) FROM hbase_sessions WHERE has_cart
  UNION ALL SELECT 4, 'reached checkout', COUNT(*) FROM hbase_sessions
            WHERE exists(page_views, p -> p.page_type = 'checkout')
  UNION ALL SELECT 5, 'verified purchase', COUNT(*) FROM hbase_sessions WHERE verified_conversion
) ORDER BY stage_no
```
Expected: 200,000 / 197,422 / 130,170 / 87,087 / 25,094 (12.5% verified purchases).

**Top 3 categories per month** (window function) is Q1 in the same script. Expected: `Kelly-Santiago` is first in
2026-08, 2026-09 and 2026-10.

## 4. Data-quality report (`python spark\01_clean_report.py`)

| Check | Expected |
|---|---|
| Transactions without a session | 24,906 |
| Sessions labelled converted without a transaction | 15,515 |
| Products active with zero stock | 163 |
| Duplicates, bad durations, amount mismatches | 0 |

## 5. Integrated query (`python integration\funnel_integration.py`)

Joins HBase sessions with MongoDB transactions in Spark. On the 100,000 sessions loaded into HBase (the first
generated file) it gives 20.2% verified purchases; the full-dataset figure from Spark is 12.5% (see the report,
Section 4.1, for why these differ).
