import sys
import os
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(BASE_DIR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
from utils.logger import get_logger

raw_path = os.path.join(BASE_DIR, "data", "raw")
bronze_path = os.path.join(BASE_DIR, "data", "bronze")
log_path = os.path.join(BASE_DIR, "logs", "pipeline.log")

os.makedirs(bronze_path, exist_ok=True)

# -------------------------------------------------
# Logger
# -------------------------------------------------
logger = get_logger(log_path)
logger.info("Bronze Layer Job Started")

# -------------------------------------------------
# Spark Session
# -------------------------------------------------
spark = SparkSession.builder \
    .appName("BronzeIngestion") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -------------------------------------------------
# Schema Definitions (Schema Enforcement)
# -------------------------------------------------
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("signup_date", StringType(), True)
])

product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True)
])

transaction_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True)
])

# -------------------------------------------------
# Ingestion Function
# -------------------------------------------------
def ingest(file_name, schema, partition_col=None):

    logger.info(f"Processing {file_name}")

    file_path = os.path.join(raw_path, f"{file_name}.csv")

    try:
        df = spark.read \
            .option("header", True) \
            .option("mode", "DROPMALFORMED") \
            .schema(schema) \
            .csv(file_path)

        count_before = df.count()
        logger.info(f"{file_name} rows read: {count_before}")

        # Minimal Cleaning (Bronze should not transform heavily)
        df = df.dropDuplicates()

        count_after = df.count()
        logger.info(f"{file_name} rows after deduplication: {count_after}")
 
        writer = df.write.mode("overwrite")

        if partition_col:
            writer = writer.partitionBy(partition_col)

        # Write Parquet
        writer.parquet(os.path.join(bronze_path, file_name))

        # Write CSV (Readable Version)
        df.write.mode("overwrite").option("header", True).csv(os.path.join(bronze_path, f"{file_name}_csv"))

        logger.info(f"{file_name} written to Bronze as parquet and csv")

    except Exception as e:
        logger.error(f"Error processing {file_name}: {str(e)}")

# -------------------------------------------------
# Run Bronze Layer
# -------------------------------------------------
if __name__ == "__main__":

    ingest("customers", customer_schema)
    ingest("products", product_schema)
    ingest("transactions", transaction_schema, partition_col="transaction_date")

    logger.info("Bronze Layer Completed Successfully")
    logger.info("--------------------------------------------------")

    spark.stop()
