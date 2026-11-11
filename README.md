# Distributed Multi-Model Analytics for E-commerce Data

AUCA Big Data Analytics final project (individual). The project combines **MongoDB** (document model),
**HBase** (wide-column model) and **Apache Spark** (distributed processing) on a synthetic e-commerce
dataset: 10,000 users, 5,000 products, 25 categories, 200,000 sessions and 50,000 transactions.

## Architecture

| System  | Holds | Why |
|---------|-------|-----|
| MongoDB | products, users, transactions (items embedded) | nested documents, flexible aggregation pipelines |
| HBase   | browsing sessions (time series) and product daily metrics | very high write volume; "latest sessions of user X" is one key-range read |
| Spark   | cleaning, recommendations, cohorts, SQL, cross-system joins | distributed processing of all session files |

```
JSON files --> MongoDB (products, users, transactions) --\
          \--> HBase  (user_sessions, product_daily_metrics) --> Spark (join, analyse) --> CSV --> charts
```

## Repository layout

```
dataset_generator.py        generates the dataset (sizes configurable with environment variables)
check_data.py               counts records, orphan transactions and session labels
check_label_by_file.py      shows where the "converted" label and real transactions disagree
docker-compose.yml          MongoDB 7 + HBase containers
mongodb/load_mongo.py       document model + loading + indexes
mongodb/queries.py          3 aggregation pipelines (revenue, segmentation, top products)
hbase/load_hbase.py         creates tables (row-key design) and loads sessions
hbase/hbase_queries.py      user-session queries (prefix scan, time-range scan, journey, product metrics)
hbase/hbase_shell_commands.txt   schema creation and sample queries for the HBase shell
spark/common.py             Spark setup + the shared cleaning pipeline
spark/01_clean_report.py    cleaning job + data-quality report + flat CSV export
spark/02_recommendations.py bought-together / viewed-together / category affinity (support, confidence, lift)
spark/03_cohort.py          cohort analysis by registration month
spark/04_spark_sql.py       Spark SQL analytics (window functions, CTEs, cross-system join, funnel)
integration/funnel_integration.py   Part 3: HBase + MongoDB + Spark funnel analysis
viz/make_charts.py          builds the report charts
```

## Requirements

Python 3.10+, Java 17 or 21 (for PySpark), Docker Desktop.

```
python -m venv venv
venv\Scripts\activate          (Windows)   |   source venv/bin/activate   (Linux/Mac)
pip install -r requirements.txt
```

## Reproduce the results

1. **Generate data** (reduced sizes; the original script uses 2,000,000 sessions and needs a lot of RAM):
   ```
   set NUM_SESSIONS=200000
   set NUM_TRANSACTIONS=50000
   python dataset_generator.py
   python check_data.py
   ```
   (PowerShell: `$env:NUM_SESSIONS=200000`; Linux/Mac: `NUM_SESSIONS=200000 python dataset_generator.py`.)
   Use a small test set with `NUM_SESSIONS=20000 NUM_TRANSACTIONS=5000`.
2. **Start the databases:** `docker compose up -d`, wait about 3 minutes, then start the HBase Thrift server:
   `docker exec -d bd-hbase hbase thrift start -p 9090`
3. **MongoDB:** `python mongodb\load_mongo.py` then `python mongodb\queries.py`
4. **HBase:** `python hbase\load_hbase.py . 100000 2` (every 2nd session of all files) then `python hbase\hbase_queries.py`
5. **Spark** (run from the project root, in this order):
   ```
   python spark\01_clean_report.py
   python spark\02_recommendations.py
   python spark\03_cohort.py
   python spark\04_spark_sql.py
   ```
   Use `set DATA_DIR=data_small` for a quick run and `set SPARK_MEM=2g` on small laptops.
6. **Integration:** `python integration\funnel_integration.py`
7. **Charts:** `python viz\make_charts.py` (PNG files in `output\charts\`)

## Key results (full dataset)

* Verified conversion: **12.5%** (25,094 of 200,000 sessions). The raw "converted" label says 20.3%.
* Funnel: 98.7% view a product, 65.1% fill a cart, 43.5% reach checkout, 12.5% buy.
* Revenue is driven by purchase frequency, not basket size: average spend per user rises from about 1.8k (1-2 orders) to 13.9k (12+ orders) while the average order value stays near 1,000.
* Conversion is the same across devices, referrers, weeks and categories (differences of 1-3 points).
* Category affinity lift is 0.80-0.85: shoppers do not cross-buy more than chance.

## Data-quality findings handled in the Spark cleaning job

* 24,906 transactions (49.8%) have no session (flag `is_orphan`).
* 15,515 sessions are labelled "converted" but have no transaction (all after about session 124,000, when the generator's 50,000-transaction budget ran out); the cleaning job derives `verified_conversion` by joining with the transactions.
* 163 products are active with zero stock. No duplicates, bad durations or amount mismatches were found.
* The generator creates no user age and no product subcategory, so segmentation uses purchase frequency.

## Limitations and future work

* Synthetic, random data: category and channel differences are noise, and lift values show no real affinity.
* Only about 90 days of history: cohort rows are short and the last month is partial.
* HBase held a subset of the sessions (memory limits of a single-laptop Docker setup); a sample of the first 100,000 sessions has no false labels, which is why its conversion rate (20.2%) differs from the full-data 12.5%.
* Data moves into Spark through Python extraction. At scale, use the HBase-Spark and MongoDB Spark connectors so the load runs in parallel, run HBase/MongoDB as clusters (sharding, region servers), and write results to Parquet.
