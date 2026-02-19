import pandas as pd
import random
import os

random.seed(42)

N = 1000
categories = ["Electronics","Clothing","Home","Sports","Books"]

data = []
for i in range(1, N+1):
    data.append([
        i,
        f"Product{i}",
        random.choice(categories),
        round(random.uniform(5, 5000), 2)
    ])

df = pd.DataFrame(data, columns=[
    "product_id","product_name","category","price"
])

os.makedirs("../data/raw", exist_ok=True)
df.to_csv("../data/raw/products.csv", index=False)

print(f"Generated {len(df)} products.")
