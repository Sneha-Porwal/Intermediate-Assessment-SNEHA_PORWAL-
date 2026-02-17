import psycopg2
import pandas as pd
import os
import glob
from datetime import date

# Simple relative path
silver_path = "data/silver"


def read_spark_csv(folder_name):
    folder_path = os.path.join(silver_path, folder_name)
    files = glob.glob(os.path.join(folder_path, "*.csv"))

    if not files:
        raise Exception(f"No CSV files found in {folder_path}")

    df_list = [pd.read_csv(file) for file in files]
    df = pd.concat(df_list, ignore_index=True)
    df = df.where(pd.notnull(df), None)
    return df


# -------------------------------------------------
# PostgreSQL Connection
# -------------------------------------------------
conn = psycopg2.connect(
    host="localhost",
    database="assessment_db",
    user="postgres",
    password="Tiger"
)

cursor = conn.cursor()

# 1️ LOAD PRODUCTS
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


#  LOAD CUSTOMERS (Dynamic SCD Type 2)
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

    else:
        customer_sk, old_name, old_region = existing

        if old_name != row["name"] or old_region != row["region"]:

            cursor.execute("""
                UPDATE dim_customers
                SET is_current = FALSE,
                    effective_to = %s
                WHERE customer_sk = %s;
            """, (today, customer_sk))

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

# Load current surrogate keys once
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

cursor.close()
conn.close()

print("All Data Loaded Successfully!")
