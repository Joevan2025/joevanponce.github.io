import pandas as pd
import os

input_path = r"C:\Users\ADMIN\projects\etl\chocolate_sales\raw_data\Chocolate Sales (2).csv"

df = pd.read_csv(input_path)

print(df.shape)
print(df.dtypes)
print(df.head(3))

print("\n[Nulls after cleaning]")
print(df.isnull().sum())
print("\nDuplicates:", df.duplicated().sum())