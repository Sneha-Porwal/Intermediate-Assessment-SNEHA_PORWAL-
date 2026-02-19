import sys
import os

# Allow project root imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from pyspark.sql.types import *
from utils.logger import get_logger
import shutil
from pathlib import Path

# -------------------------------------------------
# Paths
# -------------------------------------------------
RAW_PATH = os.path.join(BASE_DIR, "data", "raw")
BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze")
LOG_PATH = os.path.join(BASE_DIR, "logs", "pipeline.log")

os.makedirs(BRONZE_PATH, exist_ok=True)

logger = get_logger(LOG_PATH)

# -------------------------------------------------
# Schema Definitions
# -------------------------------------------------
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("effective_from", StringType(), True),
    StructField("effective_to", StringType(), True)
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

log_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("service", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("response_time_ms", IntegerType(), True)
])

# -------------------------------------------------
# Generic Ingestion Function
# -------------------------------------------------
def ingest(spark, file_name, schema, file_type="csv", partition_col=None):

    logger.info(f"Processing {file_name}")

    try:
        if file_type == "csv":
            file_path = os.path.join(RAW_PATH, f"{file_name}.csv")
            df = (
                spark.read.option("header", True)
                .option("mode", "DROPMALFORMED")
                .schema(schema)
                .csv(file_path)
            )

        elif file_type == "json":
            file_path = os.path.join(RAW_PATH, f"{file_name}.json")
            df = spark.read.schema(schema).json(file_path)

        else:
            logger.error(f"Unsupported file type for {file_name}")
            return

        df = df.cache()

        count_before = df.count()
        logger.info(f"{file_name} rows read: {count_before}")

        df = df.dropDuplicates()

        count_after = df.count()
        logger.info(f"{file_name} rows after deduplication: {count_after}")

        writer = df.write.mode("overwrite")

        if partition_col:
            writer = writer.partitionBy(partition_col)

        writer.parquet(os.path.join(BRONZE_PATH, file_name))

        logger.info(f"{file_name} written to Bronze successfully")

    except Exception as e:
        logger.error(f"Error processing {file_name}: {str(e)}")
        raise


# -------------------------------------------------
# Bronze Layer Entry Point (Modular)
# -------------------------------------------------
def run_bronze(spark):

    logger.info("Bronze Layer Started")

    ingest(spark, "customers", customer_schema)
    ingest(spark, "products", product_schema)
    ingest(spark, "transactions", transaction_schema, partition_col="transaction_date")
    ingest(spark, "app_logs", log_schema, file_type="json")

    logger.info("Bronze Layer Completed Successfully")
    logger.info("--------------------------------------------------")


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    run_bronze(spark)

    spark.stop()
