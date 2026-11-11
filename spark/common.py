"""
common.py - shared Spark setup + the CLEANING pipeline used by every Spark script.

Why one shared module?  Every job starts from the SAME cleaned DataFrames, so results are
consistent and the cleaning logic is written (and explained) only once.

Environment variables (optional):
    DATA_DIR   folder with the JSON files      (default: current folder)
    SPARK_MEM  driver memory                   (default: 4g; use 2g on small laptops)
"""
import os, sys, glob, warnings
warnings.filterwarnings("ignore")   # hide harmless pandas/pyarrow version warnings
from pathlib import Path
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

DATA_DIR = os.getenv("DATA_DIR", ".")
OUT_DIR = "output"
os.makedirs(OUT_DIR, exist_ok=True)


def get_spark(app_name):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.appName(app_name).master("local[*]")
             .config("spark.driver.memory", os.getenv("SPARK_MEM", "4g"))
             .config("spark.sql.shuffle.partitions", "16")   # default 200 is far too many for one laptop
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.execution.arrow.pyspark.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------- explicit schemas (faster than inferring, and documents the data) ----------
GEO = T.StructType([T.StructField(n, T.StringType()) for n in ("city", "state", "country")])
USERS_SCHEMA = T.StructType([
    T.StructField("user_id", T.StringType()), T.StructField("geo_data", GEO),
    T.StructField("registration_date", T.StringType()), T.StructField("last_active", T.StringType())])
PRODUCTS_SCHEMA = T.StructType([
    T.StructField("product_id", T.StringType()), T.StructField("name", T.StringType()),
    T.StructField("category_id", T.StringType()), T.StructField("base_price", T.DoubleType()),
    T.StructField("current_stock", T.IntegerType()), T.StructField("is_active", T.BooleanType()),
    T.StructField("creation_date", T.StringType())])
ITEM = T.StructType([
    T.StructField("product_id", T.StringType()), T.StructField("quantity", T.IntegerType()),
    T.StructField("unit_price", T.DoubleType()), T.StructField("subtotal", T.DoubleType())])
TXN_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()), T.StructField("session_id", T.StringType()),
    T.StructField("user_id", T.StringType()), T.StructField("timestamp", T.StringType()),
    T.StructField("items", T.ArrayType(ITEM)), T.StructField("subtotal", T.DoubleType()),
    T.StructField("discount", T.DoubleType()), T.StructField("total", T.DoubleType()),
    T.StructField("payment_method", T.StringType()), T.StructField("status", T.StringType())])
PAGE_VIEW = T.StructType([
    T.StructField("timestamp", T.StringType()), T.StructField("page_type", T.StringType()),
    T.StructField("product_id", T.StringType()), T.StructField("category_id", T.StringType()),
    T.StructField("view_duration", T.IntegerType())])
SESSION_SCHEMA = T.StructType([
    T.StructField("session_id", T.StringType()), T.StructField("user_id", T.StringType()),
    T.StructField("start_time", T.StringType()), T.StructField("end_time", T.StringType()),
    T.StructField("duration_seconds", T.IntegerType()),
    T.StructField("geo_data", T.StructType([T.StructField(n, T.StringType()) for n in ("city", "state", "country", "ip_address")])),
    T.StructField("device_profile", T.StructType([T.StructField(n, T.StringType()) for n in ("type", "os", "browser")])),
    T.StructField("viewed_products", T.ArrayType(T.StringType())),
    T.StructField("page_views", T.ArrayType(PAGE_VIEW)),
    # cart_contents has a DIFFERENT KEY PER PRODUCT -> a MapType, not a fixed struct
    T.StructField("cart_contents", T.MapType(T.StringType(), T.StructType([
        T.StructField("quantity", T.IntegerType()), T.StructField("price", T.DoubleType())]))),
    T.StructField("conversion_status", T.StringType()), T.StructField("referrer", T.StringType())])


def read_json(spark, pattern, schema):
    files = sorted(Path(DATA_DIR).glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matching {pattern} in '{DATA_DIR}'")
    # the files are JSON ARRAYS (one big [ ... ]), so multiLine=True is required
    return spark.read.schema(schema).option("multiLine", True).json([f.resolve().as_posix() for f in files])


def ts(col):
    return F.to_timestamp(F.col(col))


def load_clean(spark, dq_checks=True):
    """Reads the raw JSON files and returns CLEANED DataFrames + a dict of data-quality counts.
    dq_checks=False skips the counting jobs (each count re-reads the JSON files), so analysis
    scripts that only need the cleaned data start much faster."""
    dq = {}

    # ---------------- users ----------------
    raw_users = read_json(spark, "users.json", USERS_SCHEMA)
    users = (raw_users.filter(F.col("user_id").isNotNull())
             .dropDuplicates(["user_id"])                                   # remove duplicate profiles
             .select("user_id",
                     F.coalesce(F.trim("geo_data.city"), F.lit("unknown")).alias("city"),
                     F.coalesce(F.trim("geo_data.state"), F.lit("unknown")).alias("state"),
                     F.upper(F.coalesce(F.trim("geo_data.country"), F.lit("UNKNOWN"))).alias("country"),
                     ts("registration_date").alias("registration_date"),
                     ts("last_active").alias("last_active"))
             .withColumn("reg_month", F.date_trunc("month", "registration_date")))
    if dq_checks: dq["users_raw"] = raw_users.count()
    if dq_checks: dq["users_clean"] = users.count()
    if dq_checks: dq["users_last_active_before_registration"] = users.filter(F.col("last_active") < F.col("registration_date")).count()

    # ---------------- products ----------------
    raw_products = read_json(spark, "products.json", PRODUCTS_SCHEMA)
    products = (raw_products.filter(F.col("product_id").isNotNull())
                .dropDuplicates(["product_id"])
                .select("product_id", F.trim("name").alias("name"), "category_id",
                        F.col("base_price"), F.coalesce("current_stock", F.lit(0)).alias("current_stock"),
                        F.coalesce("is_active", F.lit(False)).alias("is_active"),
                        ts("creation_date").alias("creation_date"))
                .withColumn("active_but_no_stock", F.col("is_active") & (F.col("current_stock") <= 0)))
    if dq_checks: dq["products_raw"] = raw_products.count()
    if dq_checks: dq["products_clean"] = products.count()
    if dq_checks: dq["products_active_but_no_stock"] = products.filter("active_but_no_stock").count()
    if dq_checks: dq["products_non_positive_price"] = products.filter(F.col("base_price") <= 0).count()

    # ---------------- categories ----------------
    categories = (spark.read.option("multiLine", True)
                  .json((Path(DATA_DIR) / "categories.json").resolve().as_posix())
                  .select("category_id", F.col("name").alias("category_name")))

    # ---------------- transactions ----------------
    raw_txn = read_json(spark, "transactions.json", TXN_SCHEMA)
    items_sum = F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["subtotal"])
    txns = (raw_txn.filter(F.col("transaction_id").isNotNull())
            .dropDuplicates(["transaction_id"])
            .withColumn("timestamp", ts("timestamp"))
            .withColumn("status", F.lower(F.trim("status")))               # standardize text
            .withColumn("payment_method", F.lower(F.trim("payment_method")))
            .withColumn("discount", F.coalesce("discount", F.lit(0.0)))
            .withColumn("is_orphan", F.col("session_id").isNull())          # no linked session
            .withColumn("amount_mismatch",
                        (F.abs(F.col("total") - (F.col("subtotal") - F.col("discount"))) > 0.01) |
                        (F.abs(F.col("subtotal") - items_sum) > 0.01)))
    if dq_checks: dq["transactions_raw"] = raw_txn.count()
    if dq_checks: dq["transactions_clean"] = txns.count()
    if dq_checks: dq["transactions_without_session"] = txns.filter("is_orphan").count()
    if dq_checks: dq["transactions_amount_mismatch"] = txns.filter("amount_mismatch").count()

    # ---------------- sessions ----------------
    raw_sessions = read_json(spark, "sessions_*.json", SESSION_SCHEMA)
    txn_sessions = txns.filter(F.col("session_id").isNotNull()).select("session_id").distinct()
    cart_value = F.aggregate(F.map_values("cart_contents"), F.lit(0.0), lambda acc, x: acc + x["quantity"] * x["price"])
    sessions = (raw_sessions.filter(F.col("session_id").isNotNull() & F.col("user_id").isNotNull())
                .dropDuplicates(["session_id"])
                .select("session_id", "user_id",
                        ts("start_time").alias("start_time"), ts("end_time").alias("end_time"),
                        "duration_seconds",
                        F.coalesce("geo_data.country", F.lit("UNKNOWN")).alias("country"),
                        F.coalesce("device_profile.type", F.lit("unknown")).alias("device_type"),
                        F.coalesce("device_profile.os", F.lit("unknown")).alias("os"),
                        F.coalesce("device_profile.browser", F.lit("unknown")).alias("browser"),
                        F.coalesce(F.lower("referrer"), F.lit("unknown")).alias("referrer"),
                        F.lower(F.coalesce("conversion_status", F.lit("unknown"))).alias("conversion_status"),
                        "viewed_products", "page_views", "cart_contents")
                .withColumn("cart_value", cart_value)
                .withColumn("has_cart", F.size(F.map_keys("cart_contents")) > 0)
                .withColumn("bad_duration", (F.col("end_time") < F.col("start_time")) | (F.col("duration_seconds") < 0))
                # a session labelled 'converted' is only TRUSTED if a transaction really exists for it
                .join(F.broadcast(txn_sessions.withColumn("has_txn", F.lit(True))), "session_id", "left")
                .withColumn("has_txn", F.coalesce("has_txn", F.lit(False)))
                .withColumn("verified_conversion", (F.col("conversion_status") == "converted") & F.col("has_txn")))
    sessions = sessions.persist(StorageLevel.MEMORY_AND_DISK)   # reused many times -> cache it
    if dq_checks: dq["sessions_raw"] = raw_sessions.count()
    if dq_checks: dq["sessions_clean"] = sessions.count()
    if dq_checks: dq["sessions_bad_duration"] = sessions.filter("bad_duration").count()
    if dq_checks: dq["sessions_converted_label"] = sessions.filter(F.col("conversion_status") == "converted").count()
    if dq_checks: dq["sessions_converted_without_transaction"] = sessions.filter(
        (F.col("conversion_status") == "converted") & ~F.col("has_txn")).count()

    return {"users": users, "products": products, "categories": categories,
            "transactions": txns, "sessions": sessions}, dq


def transaction_items(txns, products=None, categories=None):
    """One row per line item (explodes the embedded items array)."""
    it = txns.select("transaction_id", "user_id", "timestamp",
                     F.explode("items").alias("it")).select(
        "transaction_id", "user_id", "timestamp",
        F.col("it.product_id").alias("product_id"), F.col("it.quantity").alias("quantity"),
        F.col("it.unit_price").alias("unit_price"), F.col("it.subtotal").alias("subtotal"))
    if products is not None:
        it = it.join(F.broadcast(products.select("product_id", "category_id")), "product_id", "left")
        if categories is not None:
            it = it.join(F.broadcast(categories), "category_id", "left")
    return it


def save_csv(df, name):
    """Small result tables -> CSV via pandas (avoids Hadoop/winutils problems on Windows)."""
    path = os.path.join(OUT_DIR, name)
    df.toPandas().to_csv(path, index=False)
    print(f"  saved -> {path}")
