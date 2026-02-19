import os

def run_pipeline():

    # ---------------------------------------------
    # 1️⃣ RAW DATA GENERATION
    # ---------------------------------------------
    print("\nGenerating Customers...")
    if os.system("python ingestion/generate_customers.py") != 0:
        print("Customer Generation Failed!")
        return

    print("Generating Products...")
    if os.system("python ingestion/generate_products.py") != 0:
        print("Product Generation Failed!")
        return

    print("Generating Transactions...")
    if os.system("python ingestion/generate_transactions.py") != 0:
        print("Transaction Generation Failed!")
        return

    print("Generating Logs...")
    if os.system("python ingestion/generate_logs.py") != 0:
        print("Log Generation Failed!")
        return

    print("Generating Streaming Batches...")
    if os.system("python ingestion/generate_streaming_batches.py") != 0:
        print("Streaming Batch Generation Failed!")
        return

    print("Raw Data Generation Completed Successfully!")

    # ---------------------------------------------
    # 2️⃣ BRONZE LAYER
    # ---------------------------------------------
    print("\nBronze Layer Started...")
    if os.system("spark-submit ingestion/bronze_ingestion.py") != 0:
        print("Bronze Layer Failed!")
        return
    print("Bronze Layer Completed Successfully!")

    # ---------------------------------------------
    # 3️⃣ SILVER LAYER
    # ---------------------------------------------
    print("\nSilver Layer Started...")
    if os.system("spark-submit spark_jobs/silver_transformation.py") != 0:
        print("Silver Layer Failed!")
        return
    print("Silver Layer Completed Successfully!")

    # ---------------------------------------------
    # 4️⃣ GOLD LAYER
    # ---------------------------------------------
    print("\nGold Layer Started...")
    if os.system("spark-submit spark_jobs/gold_aggregations.py") != 0:
        print("Gold Layer Failed!")
        return
    print("Gold Layer Completed Successfully!")

    # ---------------------------------------------
    # 5️⃣ FRAUD DETECTION
    # ---------------------------------------------
    print("\nFraud Detection Started...")
    if os.system("spark-submit fraud/fraud_detection.py") != 0:
        print("Fraud Detection Failed!")
        return
    print("Fraud Detection Completed Successfully!")

    # ---------------------------------------------
    # LOAD TO DATABASE
    # ---------------------------------------------
    print("\nLoading Data to PostgreSQL...")
    if os.system("python warehouse/load_data.py") != 0:
        print("Database Loading Failed!")
        return
    print("Database Loading Completed Successfully!")

    print("\n🎉 FULL DATA PIPELINE EXECUTED SUCCESSFULLY!")


if __name__ == "__main__":
    run_pipeline()
