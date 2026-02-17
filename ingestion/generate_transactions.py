import pandas as pd
import random
from datetime import datetime, timedelta

N = 150000
start = datetime(2025,1,1)

data = []

for i in range(1, N+1):
    txn_date = start + timedelta(days=random.randint(0, 365))
    
    # Skew: product_id 1 is very popular
    product_id = random.choices([1, random.randint(2,5000)], weights=[0.2, 0.8])[0]

    amount = round(random.uniform(10, 2000), 2)

    # Fraud spike pattern
    if random.random() < 0.01:
        amount = random.uniform(5000,10000)

    data.append([
        i,
        random.randint(1,50000),
        product_id,
        amount,
        txn_date.strftime("%Y-%m-%d"),
        random.choice(["completed","failed"]),
        None if random.random() < 0.02 else "online"
    ])

df = pd.DataFrame(data, columns=[
    "transaction_id","customer_id","product_id",
    "amount","transaction_date","status","channel"
])

# Add duplicates
df = pd.concat([df, df.sample(2000)])

df.to_csv("../data/raw/transactions.csv", index=False)

print("Generated 150K transactions + duplicates.")
