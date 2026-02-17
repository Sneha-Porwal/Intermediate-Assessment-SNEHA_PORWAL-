from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

spark = SparkSession.builder.appName("StreamingPipeline").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

stream_path = "data/streaming_input"
gold_path = "data/gold/streaming_kpi"
checkpoint_path = "data/checkpoint"

os.makedirs(gold_path, exist_ok=True)
os.makedirs(checkpoint_path, exist_ok=True)

schema = """
transaction_id INT,
customer_id INT,
product_id INT,
amount DOUBLE,
transaction_timestamp TIMESTAMP,
status STRING,
channel STRING
"""

# Read Streaming Files
stream_df = spark.readStream \
    .schema(schema) \
    .option("header", True) \
    .csv(stream_path)


# Handle Late Data Using Watermark
stream_df = stream_df.withWatermark(
    "transaction_timestamp",
    "10 minutes"
)

# Window Aggregation
windowed_df = stream_df.groupBy(
    window(col("transaction_timestamp"), "5 minutes")
).agg(
    sum("amount").alias("total_revenue"),
    count("transaction_id").alias("total_transactions")
)

# Write Streaming Output
query = windowed_df.writeStream.outputMode("append").format("csv") \
    .option("path", gold_path) \
    .option("checkpointLocation", checkpoint_path) \
    .option("header", True).start()

print("Streaming Job Started...")

query.awaitTermination()
