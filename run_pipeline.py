import os

print("Running Bronze Layer...")
os.system("spark-submit ingestion/bronze_ingestion.py")

print("Running Silver Layer...")
os.system("spark-submit spark_jobs/silver_transformations.py")

print("Running Gold Aggregations...")
os.system("spark-submit spark_jobs/gold_aggregations.py")

print("Running Fraud Detection...")
os.system("spark-submit fraud/fraud_detection.py")

print("Loading Data to PostgreSQL...")
os.system("python warehouse/load_data.py")

print("Pipeline Completed Successfully!")
