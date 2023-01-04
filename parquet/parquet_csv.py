import pandas as pd

data = pd.read_parquet("../data/heart.parquet")

data.to_csv("../data/heart.csv", index_label=False, index=False)
