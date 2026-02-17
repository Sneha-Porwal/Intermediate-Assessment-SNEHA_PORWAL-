import pandas as pd
import random
import os
from datetime import datetime, timedelta

num_batches = 10
records_per_batch = 1000

output_folder = "../data/streaming_input/"
os.makedirs(output_folder, exist_ok=True)
start_time = datetime(2026, 1, 1)

print("Creating streaming batch files...")

for batch in range(1, num_batches + 1):

    data = []

    for _ in range(records_per_batch):
        timestamp = start_time + timedelta(
            seconds=random.randint(0, 100000)
        )

        data.append([
            random.randint(1, 50000),
            random.randint(1, 5000),
            round(random.uniform(10, 5000), 2),
            timestamp.strftime("%Y-%m-%d %H:%M:%S")
        ])

    df = pd.DataFrame(data, columns=[
        "customer_id",
        "product_id",
        "amount",
        "transaction_timestamp"
    ])

    file_name = "batch_" + str(batch) + ".csv"
    file_path = os.path.join(output_folder, file_name)

    df.to_csv(file_path, index=False)

    print(file_name + " created")

print("All streaming batches created successfully!")
