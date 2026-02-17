import pandas as pd
import random

N = 5000
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

df.to_csv("../data/raw/products.csv", index=False)
print("Generated 5000 products.")
