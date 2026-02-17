import pandas as pd
import random
from datetime import datetime, timedelta

N = 50000
regions = ["North","South","East","West","Central"]

data = []
start = datetime(2024,1,1)

for i in range(1, N+1):
    signup = start + timedelta(days=random.randint(0, 700))
    data.append([
        i,
        f"Customer{i}",
        random.choice(regions),
        signup.strftime("%Y-%m-%d"),
        True,
        signup.strftime("%Y-%m-%d"),
        None
    ])

df = pd.DataFrame(data, columns=[
    "customer_id","name","region",
    "signup_date","is_current",
    "effective_from","effective_to"
])

# Add SCD Type 2 changes (10% customers move region)
''' updates = df.sample(int(N*0.1))

scd_records = []
for _, row in updates.iterrows():
    change_date = datetime(2026,1,1)
    row["is_current"] = False
    row["effective_to"] = change_date.strftime("%Y-%m-%d")
    scd_records.append(row)

    scd_records.append([
        row["customer_id"],
        row["name"],
        random.choice(regions),
        row["signup_date"],
        True,
        change_date.strftime("%Y-%m-%d"),
        None
    ])

df = pd.concat([df, pd.DataFrame(scd_records, columns=df.columns)])  '''

# Add SCD Type 2 changes (10% customers move region)
updates = df.sample(int(N * 0.1))

scd_records = []

for _, row in updates.iterrows():
    change_date = datetime(2026, 1, 1)

    old_record = [
        row["customer_id"],
        row["name"],
        row["region"],
        row["signup_date"],
        False,
        row["effective_from"],
        change_date.strftime("%Y-%m-%d")
    ]

    # New record (new region)
    new_record = [
        row["customer_id"],
        row["name"],
        random.choice(regions),
        row["signup_date"],
        True,
        change_date.strftime("%Y-%m-%d"),
        None
    ]

    scd_records.append(old_record)
    scd_records.append(new_record)

# Append SCD records properly
scd_df = pd.DataFrame(scd_records, columns=df.columns)

df = pd.concat([df, scd_df], ignore_index=True)
df.to_csv("../data/raw/customers.csv", index=False)

print("Generated 50K customers + SCD records.")
