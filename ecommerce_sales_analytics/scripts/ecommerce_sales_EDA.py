import pandas as pd
import os

input_path = r"C:\Users\ADMIN\projects\etl\ecommerce_sales_analytics\raw_data\ecommerce_sales_data.csv"

df = pd.read_csv(input_path, encoding="utf-8")

print(df.shape)
print(df.dtypes)

print("\n[Nulls before cleaning]")
print(df.isnull().sum())

print("\nDuplicates before cleaning")
print("Duplicates:", df.duplicated().sum())