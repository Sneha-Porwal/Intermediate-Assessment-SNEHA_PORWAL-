import os
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -------------------------------------------------
# Paths
# -------------------------------------------------
STREAM_PATH = "data/streaming_input"
GOLD_PATH = "data/gold/streaming_kpi"
CHECKPOINT_PATH = "data/checkpoint"

os.makedirs(GOLD_PATH, exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)

# -------------------------------------------------
# Streaming Function
# -------------------------------------------------
def run_streaming(spark):

    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("channel", StringType(), True),
    ])

    # Read Streaming Files
    stream_df = (
        spark.readStream
        .schema(schema)
        .option("header", True)
        .csv(STREAM_PATH)
    )

    # Watermark for Late Data Handling
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

    final_df = windowed_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_revenue"),
        col("total_transactions")
    )

    # Write Streaming Output
    query = (
        final_df.writeStream
        .outputMode("append")
        .format("csv")
        .option("path", GOLD_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("header", True)
        .start()
    )

    print("Streaming Job Started...")
    print("Drop batch files into data/streaming_input to simulate streaming.")

    return query


# -------------------------------------------------
# Optional Standalone Run
# -------------------------------------------------
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("StreamingPipeline").getOrCreate()

    query = run_streaming(spark)

    query.awaitTermination()
