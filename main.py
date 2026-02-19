from pyspark.sql import SparkSession
from ingestion.bronze_ingestion import run_bronze
from spark_jobs.silver_transformations import run_silver
from fraud.fraud_detection import run_fraud
from streaming.streaming_pipeline import run_streaming

from warehouse.create_tables import create_tables
from warehouse.load_data import run_warehouse_load
from warehouse.gold_layer import run_gold_sql


def run_pipeline():

    spark = (
        SparkSession.builder
        .appName("FullDataEngineeringPipeline")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\nRunning Bronze Layer")
        run_bronze(spark)

        print("\nRunning Silver Layer")
        run_silver(spark)

        print("\nCreating Warehouse Schema")
        create_tables()

        print("\nLoading Data Into Warehouse")
        run_warehouse_load()

        print("\nRunning Gold Layer (SQL Aggregations)")
        run_gold_sql()

        print("\nRunning Fraud Detection")
        run_fraud(spark)

        print("\nRunning Streaming Pipeline")
        run_streaming(spark)

        print("\nPipeline Completed Successfully ")

    except Exception as e:
        print("\nPipeline Failed ")
        print(str(e))
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()

