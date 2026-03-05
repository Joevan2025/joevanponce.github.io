import pandas as pd
import os

def transform(
    input_path=r"C:\Users\ADMIN\projects\etl\sales_forecasting\raw_data\superstore_sales_raw.csv",
    output_path=r"C:\Users\ADMIN\projects\etl\sales_forecasting\processed_data\superstore_sales_cleaned.csv"
):
    print("Loading data...")
    df = pd.read_csv(input_path, encoding="latin-1")

    # ── 1. Parse dates ──────────────────────────────────────────────────────
    df["Order Date"] = pd.to_datetime(df["Order Date"], dayfirst=True)
    df["Ship Date"]  = pd.to_datetime(df["Ship Date"], dayfirst=True)

    # ── 2. Derived date columns ──────────────────────────────────────────────
    df["Order Year"]    = df["Order Date"].dt.year
    df["Order Month"]   = df["Order Date"].dt.month
    df["Order Quarter"] = df["Order Date"].dt.quarter
    df["Days to Ship"]  = (df["Ship Date"] - df["Order Date"]).dt.days

    # ── 3. Clean text columns ────────────────────────────────────────────────
    str_cols = ["Customer Name", "City", "State", "Product Name",
                "Category", "Sub-Category", "Ship Mode", "Segment", "Region"]
    for col in str_cols:
        df[col] = df[col].str.strip().str.title()

    # ── 4. Handle missing values ─────────────────────────────────────────────
    df["Postal Code"] = df["Postal Code"].fillna(0).astype(int).astype(str)
    df.dropna(subset=["Sales", "Order ID", "Customer ID"], inplace=True)

    # ── 5. Remove duplicates ─────────────────────────────────────────────────
    df.drop_duplicates(subset=["Row ID"], inplace=True)

    # ── 6. Sales formatting ──────────────────────────────────────────────────
    df["Sales"] = df["Sales"].round(2)

    # ── 7. Reorder columns ───────────────────────────────────────────────────
    df = df[[
        "Row ID", "Order ID", "Order Date", "Order Year", "Order Month",
        "Order Quarter", "Ship Date", "Days to Ship", "Ship Mode",
        "Customer ID", "Customer Name", "Segment",
        "Country", "City", "State", "Postal Code", "Region",
        "Product ID", "Category", "Sub-Category", "Product Name",
        "Sales"
    ]]

    # ── 8. Save output ───────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"Rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print(f"Done! Saved to: {output_path}")

if __name__ == "__main__":
    transform()