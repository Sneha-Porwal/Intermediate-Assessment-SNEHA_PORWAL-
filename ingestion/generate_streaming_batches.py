import pandas as pd
import random
import os
from datetime import datetime, timedelta

random.seed(42)

num_batches = 10
records_per_batch = 1000

output_folder = "../data/streaming_input"
os.makedirs(output_folder, exist_ok=True)

start_time = datetime(2026, 1, 1)

print("Creating streaming batch files...")

for batch in range(1, num_batches + 1):

    data = []

    for _ in range(records_per_batch):

        timestamp = start_time + timedelta(
            seconds=batch * 1000 + random.randint(0, 100)
        )

        data.append([
            random.randint(1, 100000),
            random.randint(1, 5000),
            random.randint(1, 1000),  # match products range
            round(random.uniform(10, 5000), 2),
            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            random.choice(["success", "failed"]),
            random.choice(["online", "store"])
        ])

    df = pd.DataFrame(data, columns=[
        "transaction_id",
        "customer_id",
        "product_id",
        "amount",
        "transaction_timestamp",
        "status",
        "channel"
    ])

    file_name = f"batch_{batch}.csv"
    file_path = os.path.join(output_folder, file_name)

    df.to_csv(file_path, index=False)

    print(file_name + " created")

print("All streaming batches created successfully!")
