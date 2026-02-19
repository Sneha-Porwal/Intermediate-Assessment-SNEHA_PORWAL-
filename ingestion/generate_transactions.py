import pandas as pd
import random
import sys
import os
from datetime import datetime, timedelta

# Default size
N = 20000  


random.seed(42)

start = datetime(2025, 1, 1)
data = []

for i in range(1, N + 1):
    txn_date = start + timedelta(days=random.randint(0, 365))

    # Skew: product_id 1 is very popular
    product_id = random.choices(
        [1, random.randint(2, 1000)],  # match products range
        weights=[0.2, 0.8]
    )[0]

    amount = round(random.uniform(10, 2000), 2)
    status = random.choice(["completed", "failed"])

    # Fraud spike pattern (1%)
    if random.random() < 0.01:
        amount = round(random.uniform(5000, 10000), 2)
        status = "completed"

    data.append([
        i,
        random.randint(1, 5000),  # match customers range
        product_id,
        amount,
        txn_date.strftime("%Y-%m-%d"),
        status,
        None if random.random() < 0.02 else "online"
    ])

df = pd.DataFrame(data, columns=[
    "transaction_id", "customer_id", "product_id",
    "amount", "transaction_date", "status", "channel"
])

# Add duplicates (2%)
duplicates = df.sample(int(N * 0.02), random_state=42)
df = pd.concat([df, duplicates], ignore_index=True)

output_path = "../data/raw/transactions.csv"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

df.to_csv(output_path, index=False)

print(f"Generated {len(df)} transactions (including duplicates).")
