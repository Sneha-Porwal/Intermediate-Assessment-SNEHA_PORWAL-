import sys
import os

# -------------------------------------------------
# Allow project root imports
# -------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from pyspark.sql.functions import (
    col,
    trim,
    lower,
    to_date,
    broadcast
)
from utils.logger import get_logger

# -------------------------------------------------
# Paths
# -------------------------------------------------
BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze")
SILVER_PATH = os.path.join(BASE_DIR, "data", "silver")
LOG_PATH = os.path.join(BASE_DIR, "logs", "silver_pipeline.log")

os.makedirs(SILVER_PATH, exist_ok=True)

logger = get_logger(LOG_PATH)

# -------------------------------------------------
# Silver Layer Function
# -------------------------------------------------
def run_silver(spark):

    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Silver Layer Started")

    # ---------------------------------------------
    # Load Bronze Data
    # ---------------------------------------------
    customers = spark.read.parquet(f"{BRONZE_PATH}/customers")
    products = spark.read.parquet(f"{BRONZE_PATH}/products")
    transactions = spark.read.parquet(f"{BRONZE_PATH}/transactions")

    # ---------------------------------------------
    # Customers – SCD Type 2 Preservation
    # ---------------------------------------------
    logger.info("Processing Customers")

    customers = customers.dropDuplicates()

    customers = (
        customers
        .withColumn("name", trim(col("name")))
        .withColumn("region", lower(trim(col("region"))))
        .withColumn("signup_date", to_date(col("signup_date")))
        .withColumn("effective_from", to_date(col("effective_from")))
        .withColumn("effective_to", to_date(col("effective_to")))
        .fillna({"region": "unknown"})
    )

    logger.info(f"Customers count: {customers.count()}")

    customers.write.mode("overwrite") \
        .parquet(f"{SILVER_PATH}/dim_customers")

    logger.info("dim_customers created")

    # ---------------------------------------------
    # Products
    # ---------------------------------------------
    logger.info("Processing Products")

    products = products.dropDuplicates()

    products = (
        products
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", lower(trim(col("category"))))
        .fillna({"category": "unknown"})
    )

    logger.info(f"Products count: {products.count()}")

    products.write.mode("overwrite") \
        .parquet(f"{SILVER_PATH}/dim_products")

    logger.info("dim_products created")

    # ---------------------------------------------
    # Transactions
    # ---------------------------------------------
    logger.info("Processing Transactions")

    transactions = transactions.dropDuplicates(["transaction_id"])

    transactions = (
        transactions
        .withColumn("transaction_date", to_date(col("transaction_date")))
        .fillna({"status": "unknown", "channel": "unknown"})
    )

    logger.info(f"Transactions before validation: {transactions.count()}")

    # ---------------------------------------------
    # Referential Integrity Enforcement
    # ---------------------------------------------
    transactions = transactions.join(
        broadcast(customers.filter(col("is_current") == True)
                  .select("customer_id")),
        "customer_id",
        "inner"
    )

    transactions = transactions.join(
        broadcast(products.select("product_id")),
        "product_id",
        "inner"
    )

    logger.info(f"Transactions after validation: {transactions.count()}")

    # ---------------------------------------------
    # Partition for performance
    # ---------------------------------------------
    transactions.write.mode("overwrite") \
        .partitionBy("transaction_date") \
        .parquet(f"{SILVER_PATH}/fact_transactions")

    logger.info("fact_transactions created")
    logger.info("Silver Layer Completed Successfully")
    logger.info("--------------------------------------------------")

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("SilverLayer").getOrCreate()

    run_silver(spark)

    spark.stop()
