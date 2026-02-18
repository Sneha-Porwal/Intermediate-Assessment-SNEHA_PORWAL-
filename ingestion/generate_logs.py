import json
import random
import os
from datetime import datetime, timedelta

random.seed(42)

services = ["auth", "order", "payment", "inventory"]
records = []
start = datetime(2026, 1, 1)

N = 10000

for i in range(N):
    ts = start + timedelta(seconds=i)

    status = random.choices(
        [200, 201, 400, 500, 503],
        weights=[60, 15, 5, 15, 5]
    )[0]

    records.append({
        "timestamp": ts.isoformat(),
        "service": random.choice(services),
        "status_code": status,
        "response_time_ms": random.randint(50, 2000)
    })

output_path = "../data/raw/app_logs.json"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w") as f:
    json.dump(records, f, indent=2)

print(f"Generated {N} logs.")
