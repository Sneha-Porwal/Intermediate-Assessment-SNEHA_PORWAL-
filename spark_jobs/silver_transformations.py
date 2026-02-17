from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_date, broadcast
from utils.logger import get_logger
import os

bronze_path = "data/bronze"
silver_path = "data/silver"
log_path = "logs/silver_pipeline.log"

os.makedirs(silver_path, exist_ok=True)

logger = get_logger(log_path)
logger.info("Silver Layer Job Started")

spark = SparkSession.builder.appName("SilverLayer").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

logger.info("Loading Bronze Data")

customers = spark.read.parquet(f"{bronze_path}/customers")
products = spark.read.parquet(f"{bronze_path}/products")
transactions = spark.read.parquet(f"{bronze_path}/transactions")

# 1 Customers Transformation
logger.info("Processing Customers")

customers = customers.dropDuplicates(["customer_id"]) \
    .withColumn("name", trim(col("name"))) \
    .withColumn("region", lower(trim(col("region")))) \
    .withColumn("signup_date", to_date(col("signup_date"))) \
    .fillna({"region": "unknown"})

logger.info(f"Customers Count: {customers.count()}")

customers.write.mode("overwrite") .option("header", True).csv(f"{silver_path}/dim_customers")

logger.info("dim_customers created")

# 2️ Products Transformation
logger.info("Processing Products")

products = products.dropDuplicates(["product_id"]) \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("category", lower(trim(col("category")))).fillna({"category": "unknown"})

logger.info(f"Products Count: {products.count()}")

products.write.mode("overwrite").option("header", True).csv(f"{silver_path}/dim_products")

logger.info("dim_products created")

# 3️ Transactions Transformation
logger.info("Processing Transactions")

transactions = transactions.dropDuplicates(["transaction_id"]) \
    .withColumn("transaction_date", to_date(col("transaction_date"))) \
    .fillna({"status": "unknown", "channel": "unknown"})

logger.info(f"Transactions before join: {transactions.count()}")

# Broadcast joins
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

# Partitioned write
transactions.write.mode("overwrite").option("header", True).partitionBy("transaction_date") \
    .csv(f"{silver_path}/fact_transactions")

logger.info("fact_transactions created")

logger.info("Silver Layer Created Successfully")
logger.info("--------------------------------------------------")

spark.stop()
