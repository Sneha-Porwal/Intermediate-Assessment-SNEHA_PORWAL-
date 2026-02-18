import sys
import os

# Add project root to path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from utils.logger import get_logger
import shutil
from pathlib import Path

raw_path = os.path.join(BASE_DIR, "data", "raw")
bronze_path = os.path.join(BASE_DIR, "data", "bronze")
log_path = os.path.join(BASE_DIR, "logs", "pipeline.log")

os.makedirs(bronze_path, exist_ok=True)

# -------------------------------------------------
# Logger
# -------------------------------------------------
logger = get_logger(log_path)
logger.info("Bronze Layer Job Started")
logger.info(f"BASE_DIR: {BASE_DIR}")
logger.info(f"raw_path: {raw_path}")
logger.info(f"bronze_path: {bronze_path}")

# Quick runtime checks for Java on Windows/paths
java_on_path = shutil.which("java")
logger.info(f"java on PATH: {java_on_path}")
logger.info(f"JAVA_HOME env: {os.environ.get('JAVA_HOME')}")
if not java_on_path:
    logger.error("Java not found on PATH. Install a JDK and set JAVA_HOME.")
    # don't exit here; let Spark attempt to start and produce its error
else:
    # If JAVA_HOME not set, try to infer it from java executable location
    if not os.environ.get('JAVA_HOME'):
        try:
            inferred = str(Path(java_on_path).parents[1])
            os.environ['JAVA_HOME'] = inferred
            logger.info(f"Inferred JAVA_HOME: {inferred}")
        except Exception:
            pass

# -------------------------------------------------
# Spark Session
# -------------------------------------------------
try:
    spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()
except Exception as e:
    logger.error("Failed to start Spark Session. Check Java and Spark installation.")
    logger.error(str(e))
    raise

spark.sparkContext.setLogLevel("ERROR")                  #Reduce the internal logs and only shows the error logs

# -------------------------------------------------
# Schema Definitions
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
    logger.info(f"Computed file_path: {file_path}")

    try:
        df = spark.read.option("header", True).option("mode", "DROPMALFORMED").schema(schema).csv(file_path)           #skips the corrupted values

        count_before = df.count()
        logger.info(f"{file_name} rows read: {count_before}")

        df = df.dropDuplicates()

        count_after = df.count()
        logger.info(f"{file_name} rows after deduplication: {count_after}")

        writer = df.write.mode("overwrite")

        if partition_col:
            writer = writer.partitionBy(partition_col)

        # Write Parquet
        writer.parquet(os.path.join(bronze_path, file_name))

        # Write CSV
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
