import pandas as pd
import os

input_path = r"C:\Users\ADMIN\projects\etl\chocolate_sales\raw_data\Chocolate Sales (2).csv"
output_path = r"C:\Users\ADMIN\projects\etl\chocolate_sales\processed_data\chocolate_sales_clean.csv"

df = pd.read_csv(input_path)

print("Raw shape:", df.shape)
print("\nDtypes before cleaning:\n", df.dtypes)

# Standardize columns names
df.columns = [c.strip().lower().replace(" ","_") for c in df.columns]

# Standardize each columns
#--- 1. sales_person ---
df["sales_person"] = df["sales_person"].str.strip().str.title().str.replace(r"\s+", " ", regex=True)


#--- 2. country ---
df["country"] = df["country"].str.strip().str.title().replace(r"\s+", " ", regex=True)

#--- 3. product ---
df["product"] = df["product"].str.strip().str.title().replace(r"\s+", " ", regex=True)

#---4. Date ---
df["date"] = pd.to_datetime(df["date"], dayfirst=True)


#--- 5. amount ---
df["amount"] = df["amount"].str.strip().str.replace(r"[$,]", "", regex=True).astype(float).round(2)

#--- 6. boxes shipped ---
df["boxes_shipped"] = df["boxes_shipped"].astype(float).astype(int)

#--- 7. Sort & Reset Index ---
df = df.sort_values("date").reset_index(drop=True)


#--- 19. Validate ---
print("\nFinal shape:", df.shape)
print("\nDtypes:\n", df.dtypes)
print("\nNull counts:\n", df.isnull().sum())
print("\nDuplicates:", df.duplicated().sum())
print("\nDate range:", df["date"].min().date(), "→", df["date"].max().date())
print("\nCountries:", sorted(df["country"].unique()))
print("\nSample:\n", df.head(5).to_string())
 
# ── 20. Export ────────────────────────────────────────────────────────────────
df.to_csv(output_path, index=False)
print("\n✓ Saved to chocolate_sales_clean_standardized.csv")
 
 