import sys
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(BASE_DIR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_date, broadcast
from utils.logger import get_logger

bronze_path = os.path.join(BASE_DIR, "data", "bronze")
silver_path = os.path.join(BASE_DIR, "data", "silver")
log_path = os.path.join(BASE_DIR, "logs", "silver_pipeline.log")

os.makedirs(silver_path, exist_ok=True)

# Logger
logger = get_logger(log_path)
logger.info("Silver Layer Job Started")

# Spark Session
spark = SparkSession.builder.appName("SilverLayer").getOrCreate()

# Reduce Spark internal logs
spark.sparkContext.setLogLevel("ERROR")

# Load Bronze Data
logger.info("Loading Bronze Data")

customers = spark.read.parquet(os.path.join(bronze_path, "customers"))
products = spark.read.parquet(os.path.join(bronze_path, "products"))
transactions = spark.read.parquet(os.path.join(bronze_path, "transactions"))

#  Customers Transformation
logger.info("Processing Customers")

customers = customers.dropDuplicates(["customer_id"]).withColumn("name", trim(col("name"))) \
    .withColumn("region", lower(trim(col("region")))).withColumn("signup_date", to_date(col("signup_date"))) \
    .fillna({"region": "unknown"})

logger.info(f"Customers Count: {customers.count()}")

customers.write.mode("overwrite").option("header", True) \
    .csv(os.path.join(silver_path, "dim_customers"))

logger.info("dim_customers created")

# Products Transformation
logger.info("Processing Products")

products = products.dropDuplicates(["product_id"]) \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("category", lower(trim(col("category")))) \
    .fillna({"category": "unknown"})

logger.info(f"Products Count: {products.count()}")

products.write.mode("overwrite") \
    .option("header", True) \
    .csv(os.path.join(silver_path, "dim_products"))

logger.info("dim_products created")

#  Transactions Transformation
logger.info("Processing Transactions")

transactions = transactions.dropDuplicates(["transaction_id"]) \
    .withColumn("transaction_date", to_date(col("transaction_date"))) \
    .fillna({"status": "unknown", "channel": "unknown"})

logger.info(f"Transactions before join: {transactions.count()}")

#  Optimized Broadcast Joins
transactions = transactions.join(
    broadcast(customers.select("customer_id")),
    "customer_id",
    "inner"
)

logger.info(f"After customer join: {transactions.count()}")

transactions = transactions.join(
    broadcast(products.select("product_id")),
    "product_id",
    "inner"
)

logger.info(f"After product join: {transactions.count()}")

transactions.write.mode("overwrite").option("header", True).partitionBy("transaction_date") \
    .csv(os.path.join(silver_path, "fact_transactions"))

logger.info("fact_transactions created")

logger.info("Silver Layer Created Successfully")
logger.info("--------------------------------------------------")

spark.stop()
