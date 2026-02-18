import psycopg2
import pandas as pd
import os
import glob                           # Group multiple csv file inside a folder
from datetime import date

silver_path = "data/silver"


def read_spark_csv(folder_name):
    folder_path = os.path.join(silver_path, folder_name)

    # Recursively read all Spark part files
    files = glob.glob(
        os.path.join(folder_path, "**", "*.csv"),
        recursive=True
    )

    # Keep only actual Spark part files
    files = [f for f in files if "part-" in os.path.basename(f)]

    if not files:
        raise Exception(f"No CSV files found in {folder_path}")

    df_list = [pd.read_csv(file) for file in files]
    df = pd.concat(df_list, ignore_index=True)

    # Replace NaN with None (PostgreSQL compatibility)
    df = df.where(pd.notnull(df), None)

    return df


conn = psycopg2.connect(
    host="localhost",
    database="assessment_db",
    user="postgres",
    password="Tiger"
)

cursor = conn.cursor()

try:

    #  LOAD PRODUCTS
    products = read_spark_csv("dim_products")

    product_data = [
        (
            int(row["product_id"]),
            row["product_name"],
            row["category"],
            float(row["price"])
        )
        for _, row in products.iterrows()
    ]

    cursor.executemany("""
        INSERT INTO dim_products (product_id, product_name, category, price)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING;
    """, product_data)

    conn.commit()
    print("Products Loaded")

    #  LOAD CUSTOMERS (SCD TYPE 2)
    customers = read_spark_csv("dim_customers")
    today = date.today()

    for _, row in customers.iterrows():

        customer_id = int(row["customer_id"])

        cursor.execute("""
            SELECT customer_sk, name, region
            FROM dim_customers
            WHERE customer_id = %s AND is_current = TRUE;
        """, (customer_id,))

        existing = cursor.fetchone()

        # New customer → Insert
        if existing is None:
            cursor.execute("""
                INSERT INTO dim_customers
                (customer_id, name, region,
                 effective_from, effective_to, is_current)
                VALUES (%s, %s, %s, %s, %s, TRUE);
            """, (
                customer_id,
                row["name"],
                row["region"],
                today,
                None
            ))

        # Existing customer → Check for changes
        else:
            customer_sk, old_name, old_region = existing

            if old_name != row["name"] or old_region != row["region"]:

                # Expire old record
                cursor.execute("""
                    UPDATE dim_customers
                    SET is_current = FALSE,
                        effective_to = %s
                    WHERE customer_sk = %s;
                """, (today, customer_sk))

                # Insert new version
                cursor.execute("""
                    INSERT INTO dim_customers
                    (customer_id, name, region,
                     effective_from, effective_to, is_current)
                    VALUES (%s, %s, %s, %s, %s, TRUE);
                """, (
                    customer_id,
                    row["name"],
                    row["region"],
                    today,
                    None
                ))

    conn.commit()
    print("Customers Loaded (SCD Type 2 Applied)")


    #  LOAD TRANSACTIONS
    transactions = read_spark_csv("fact_transactions")

    # Load current customer surrogate keys once
    cursor.execute("""
        SELECT customer_id, customer_sk
        FROM dim_customers
        WHERE is_current = TRUE;
    """)

    customer_map = dict(cursor.fetchall())

    transaction_data = []

    for _, row in transactions.iterrows():

        customer_id = int(row["customer_id"])

        if customer_id in customer_map:

            transaction_data.append((
                int(row["transaction_id"]),
                customer_map[customer_id],
                int(row["product_id"]),
                float(row["amount"]),
                row["transaction_date"],
                row["status"],
                row["channel"]
            ))

    cursor.executemany("""
        INSERT INTO fact_transactions
        (transaction_id, customer_sk, product_id,
         amount, transaction_date, status, channel)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING;
    """, transaction_data)

    conn.commit()
    print("Transactions Loaded")

    print("\nAll Data Loaded Successfully!")

except Exception as e:
    conn.rollback()
    print("Error occurred:", e)

finally:
    cursor.close()
    conn.close()
