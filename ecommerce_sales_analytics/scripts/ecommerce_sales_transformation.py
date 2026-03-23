import pandas as pd
import os

input_path = r"C:\Users\ADMIN\projects\etl\ecommerce_sales_analytics\raw_data\ecommerce_sales_data.csv"
output_path = r"C:\Users\ADMIN\projects\etl\ecommerce_sales_analytics\processed_data\ecommerce_sales_clean.csv"

df = pd.read_csv(input_path, encoding="utf-8")
print("Raw shape", df.shape)
print("\nDtypes before transfomation: \n", df.dtypes)

# Standardize column names
df.columns = [c.strip().lower().replace(" ","_") for c in df.columns]

# Standardize each columns
# Order date
df["order_date"] = pd.to_datetime(df["order_date"], dayfirst=True)

# Product name
df["product_name"] = df["product_name"].str.strip().str.title().replace(r"\s+"," ", regex=True)

# Category
df["category"] = df["category"].str.strip().str.title().replace(r"\s+"," ", regex=True)

# Region
df["region"] = df["region"].str.strip().str.title().replace(r"\s+"," ", regex=True)


# Quantitative columns
df["quantity"] = df["quantity"].astype(int)
df["sales"]    = df["sales"].astype(float).round(2)
df["profit"]   = df["profit"].round(2)


# Sort and Reset index
df = df.sort_values("order_date").reset_index(drop=True)

#---  Validate ---
print("\nFinal shape:", df.shape)
print("\nDtypes:\n", df.dtypes)
print("\nNull counts:\n", df.isnull().sum())
print("\nDuplicates:", df.duplicated().sum())
print("\nDate range:", df["order_date"].min().date(), "→", df["order_date"].max().date())
print("\nRegion:", sorted(df["region"].unique()))
print("\nSample:\n", df.head(5).to_string())

# Export
df.to_csv(output_path, index=False)