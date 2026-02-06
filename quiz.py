import pandas as pd

df = pd.read_excel("pytania.xlsx")
row = df.sample(n=1).iloc[0]

print(f"Pytanie: {row.iloc[1]}")
