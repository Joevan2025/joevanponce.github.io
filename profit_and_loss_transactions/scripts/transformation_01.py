"""
P&L Transaction ETL Pipeline
Columns: category, transaction_date, transaction_type, num, name,
         territory, class_full_name, memo_description,
         item_split_account, amount, balance
"""

import pandas as pd
import numpy as np


# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

INPUT_FILE  = r"C:\Users\ADMIN\projects\etl\P&L\processed_data\profit_and_loss_stage_01.csv"
OUTPUT_CSV  = r"C:\Users\ADMIN\projects\etl\P&L\processed_data\profit_and_loss_stage_02.csv"




# ─────────────────────────────────────────
# 1. LOAD
# ─────────────────────────────────────────

def load_data(filepath: str) -> pd.DataFrame:
    """Load CSV, treat blank/whitespace cells as NaN, normalize column names."""
    df = pd.read_csv(
        filepath,
        dtype=str,
        na_values=["", " ", "  ", "   ", "N/A", "n/a", "NA", "NULL", "null", "None", "none", "-"],
        keep_default_na=True,
    )

    # strip spaces, lowercase, replace spaces with underscores
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    print(f"✓ Loaded {len(df):,} rows | Columns: {list(df.columns)}")
    return df


# ─────────────────────────────────────────
# 2. CLEAN — DATES
# ─────────────────────────────────────────

def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse transaction_date to datetime."""
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], errors="coerce"
    )

    null_dates = df["transaction_date"].isna().sum()
    if null_dates:
        print(f"  ⚠ {null_dates} unparseable dates set to NaT")

    return df


# ─────────────────────────────────────────
# 3. CLEAN — NUMERIC (amount, balance)
# ─────────────────────────────────────────

def clean_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Strip formatting and coerce amount/balance to float."""
    for col in ["amount", "balance"]:
        if col not in df.columns:
            print(f"  ⚠ Column '{col}' not found — skipping")
            continue

        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.replace(",", "", regex=False)   # remove thousands separator
            .str.replace("(", "-", regex=False)  # accounting negatives (1,000) → -1000
            .str.replace(")", "",  regex=False)
            .str.replace("$", "",  regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .round(2)
        )


    return df


# ─────────────────────────────────────────
# 4. CLEAN — CATEGORICAL COLUMNS
# ─────────────────────────────────────────

def clean_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize case, fill nulls, and cast to category dtype."""
    cat_cols = ["category", "transaction_type", "class_full_name"]

    for col in cat_cols:
        if col not in df.columns:
            continue
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.title()
            .replace("Nan", pd.NA)
            .astype("category")
        )

    return df


def drop_incomplete_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where BOTH transaction_date AND transaction_type are null/blank."""
    df = df.copy()
    before = len(df)

    # convert blank strings to NaN
    df["transaction_type"] = df["transaction_type"].replace(r"^\s*$", pd.NA, regex=True)
    df["category"] = df["category"].replace(r"^\s*$", pd.NA, regex=True)

    df = df.dropna(subset=["transaction_date", "transaction_type"], how="all")
    df = df.dropna(subset=["category"])

    dropped = before - len(df)

    if dropped:
        print(f"⚠ Dropped {dropped:,} rows where transaction_date and transaction_type are both blank")
    else:
        print("✓ No rows with both transaction_date and transaction_type blank")

    return df.reset_index(drop=True)

# ─────────────────────────────────────────
# 5. CLEAN — TEXT COLUMNS
# ─────────────────────────────────────────

def clean_text(df: pd.DataFrame) -> pd.DataFrame:
    """Strip and standardize string fields."""
    if "name" in df.columns:
        df["name"] = df["name"].astype(str).str.strip().str.title().replace("Nan", pd.NA)

    if "territory" in df.columns:
        df["territory"] = (
            df["territory"].astype(str).str.strip().str.upper().replace("NAN", pd.NA)
        )

    if "memo_description" in df.columns:
        df["memo_description"] = (
            df["memo_description"]
            .astype(str)
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "Nan": pd.NA})
            
        )

    return df

# ─────────────────────────────────────────
# 6. CLEAN — ITEM_SPLIT_ACCOUNT
# ─────────────────────────────────────────

def clean_account(df: pd.DataFrame) -> pd.DataFrame:
    """Split hierarchical account string into two levels."""
    if "item_split_account" not in df.columns:
        return df

    df["item_split_account"] = df["item_split_account"].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA, "Nan": pd.NA})

    return df



# ─────────────────────────────────────────
# 7. NULL AUDIT
# ─────────────────────────────────────────

def null_audit(df: pd.DataFrame) -> None:
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0].sort_values(ascending=False)
    if nulls.empty:
        print("✓ No nulls remaining")
    else:
        print("\nNull audit (remaining):")
        print(nulls.to_string())


# ─────────────────────────────────────────
# 8. SUMMARY REPORT
# ─────────────────────────────────────────

def print_summary(df: pd.DataFrame) -> None:
    print("\n" + "=" * 50)
    print("PIPELINE SUMMARY")
    print("=" * 50)
    print(f"  Rows              : {len(df):,}")
    print(f"  Date range        : {df['transaction_date'].min().date()} → "
          f"{df['transaction_date'].max().date()}")
    print(f"  Total amount      : {df['amount'].sum():,.2f}")
    print(f"  Unique categories : {df['category'].nunique()}")
    print(f"  Unique accounts   : {df['item_split_account'].nunique()}")
    print(f"  Unique territories: {df['territory'].nunique()}")
    print("=" * 50)


# ─────────────────────────────────────────
# 9. EXPORT
# ─────────────────────────────────────────

def export(df: pd.DataFrame, csv_path: str) -> None:
    df.to_csv(csv_path, index=False)
    print(f"\n✓ CSV  saved → {csv_path}")



# ─────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────

def run_pipeline(input_file: str = INPUT_FILE) -> pd.DataFrame:
    print("\n─── P&L ETL Pipeline ───\n")

    df = load_data(input_file)

    print("\n[1] Cleaning dates...")
    df = clean_dates(df)

    print("[2] Cleaning numeric columns...")
    df = clean_numeric(df)

    df = drop_incomplete_rows(df)

    print("[3] Cleaning categorical columns...")
    df = clean_categoricals(df)

    print("[4] Cleaning text columns...")
    df = clean_text(df)


    print("[5] Splitting account hierarchy...")
    df = clean_account(df)


    print("\n[6] Null audit...")
    null_audit(df)

    print_summary(df)

    export(df, OUTPUT_CSV)

    return df


# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────

if __name__ == "__main__":
    df_clean = run_pipeline()