import os
from pyspark.sql.functions import when, col

# -------------------------------------------------
# Paths
# -------------------------------------------------
SILVER_PATH = "data/silver/fact_transactions"
GOLD_PATH = "data/gold/fraud_analysis"

os.makedirs(GOLD_PATH, exist_ok=True)

# -------------------------------------------------
# Fraud Detection Function
# -------------------------------------------------
def run_fraud(spark):

    spark.sparkContext.setLogLevel("ERROR")

    # Read from Silver (parquet)
    df = spark.read.parquet(SILVER_PATH)

    # Ensure amount is double
    df = df.withColumn("amount", col("amount").cast("double"))

    # Simple fraud rule (amount > 5000)
    fraud_df = df.withColumn(
        "fraud_flag",
        when(col("amount") > 5000, True).otherwise(False)
    )

    # Write to Gold
    fraud_df.write.mode("overwrite") \
        .parquet(GOLD_PATH)

    print("Fraud detection completed successfully.")


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

    run_fraud(spark)

    spark.stop()
