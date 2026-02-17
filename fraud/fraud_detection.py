from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Read from Silver
df = spark.read.csv("data/silver/fact_transactions", header=True)

# Convert amount to double first
df = df.withColumn("amount", col("amount").cast("double"))

# Add fraud flag column
fraud_df = df.withColumn(
    "fraud_flag",
    when(col("amount") > 10000, "YES").otherwise("NO")
)

# Write to Gold
fraud_df.write.mode("overwrite").option("header", True).csv("data/gold/fraud_analysis")

print("Fraud detection completed successfully.")

spark.stop()
