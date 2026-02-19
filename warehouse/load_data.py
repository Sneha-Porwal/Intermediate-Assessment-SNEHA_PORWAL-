import os
import psycopg2
import pandas as pd


def run_warehouse_load():

    # -------------------------------------------------
    # Config
    # -------------------------------------------------
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    SILVER_PATH = os.path.join(BASE_DIR, "data", "silver")

    DB_CONFIG = {
        "host": "localhost",
        "database": "assessment_db",
        "user": "postgres",
        "password": "Tiger",
        "port": 5432
    }

    print("Connecting to PostgreSQL...")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # -------------------------------------------------
    # Idempotent Load (Clean Tables)
    # -------------------------------------------------
    print("Cleaning existing warehouse tables...")

    cursor.execute("TRUNCATE fact_transactions RESTART IDENTITY CASCADE;")
    cursor.execute("TRUNCATE dim_customers RESTART IDENTITY CASCADE;")
    cursor.execute("TRUNCATE dim_products CASCADE;")
    conn.commit()

    # -------------------------------------------------
    # Helper: Read Spark Parquet with Partition Support
    # -------------------------------------------------
    def read_parquet_folder(folder_name):

        folder_path = os.path.join(SILVER_PATH, folder_name)
        dfs = []

        for root, _, files in os.walk(folder_path):
            for f in files:
                if f.endswith(".parquet"):
                    full_path = os.path.join(root, f)
                    df = pd.read_parquet(full_path)

                    # Extract partition columns (e.g., transaction_date=2025-01-01)
                    relative_path = os.path.relpath(root, folder_path)
                    parts = relative_path.split(os.sep)

                    for part in parts:
                        if "=" in part:
                            col_name, col_value = part.split("=")
                            df[col_name] = col_value

                    dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    # -------------------------------------------------
    # Load Customers
    # -------------------------------------------------
    def load_dim_customers():

        print("Loading dim_customers...")

        df = read_parquet_folder("dim_customers")

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO dim_customers
                (customer_id, name, region, signup_date,
                 is_current, effective_from, effective_to)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (
                row.get("customer_id"),
                row.get("name"),
                row.get("region"),
                row.get("signup_date"),
                row.get("is_current"),
                row.get("effective_from"),
                row.get("effective_to")
            ))

        conn.commit()
        print("dim_customers loaded.")

    # -------------------------------------------------
    # Load Products
    # -------------------------------------------------
    def load_dim_products():

        print("Loading dim_products...")

        df = read_parquet_folder("dim_products")

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO dim_products
                (product_id, product_name, category, price)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (product_id) DO NOTHING
            """, (
                row.get("product_id"),
                row.get("product_name"),
                row.get("category"),
                row.get("price")
            ))

        conn.commit()
        print("dim_products loaded.")

    # -------------------------------------------------
    # Load Fact Transactions
    # -------------------------------------------------
    def load_fact_transactions():

        print("Loading fact_transactions...")

        df = read_parquet_folder("fact_transactions")

        for _, row in df.iterrows():

            cursor.execute("""
                SELECT customer_sk
                FROM dim_customers
                WHERE customer_id = %s AND is_current = TRUE
            """, (row.get("customer_id"),))

            result = cursor.fetchone()

            if result:
                customer_sk = result[0]

                cursor.execute("""
                    INSERT INTO fact_transactions
                    (transaction_id, customer_sk, product_id,
                     amount, transaction_date, status, channel, fraud_flag)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (transaction_id) DO NOTHING
                """, (
                    row.get("transaction_id"),
                    customer_sk,
                    row.get("product_id"),
                    row.get("amount"),
                    row.get("transaction_date"),
                    row.get("status"),
                    row.get("channel"),
                    row.get("fraud_flag", False)
                ))

        conn.commit()
        print("fact_transactions loaded.")

    # -------------------------------------------------
    # Execute Loading
    # -------------------------------------------------
    load_dim_customers()
    load_dim_products()
    load_fact_transactions()

    cursor.close()
    conn.close()

    print("Warehouse Load Completed Successfully.")
