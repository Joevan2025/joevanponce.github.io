"""
Pipeline 1: Google Drive → BigQuery
Reads a CSV from Google Drive and upserts into a BigQuery table.

Requirements:
    pip install google-cloud-bigquery google-api-python-client google-auth pandas
"""

import io
from datetime import date, datetime, timezone
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import bigquery

from config import (
    SERVICE_ACCOUNT_FILE,
    BQ_PROJECT, BQ_DATASET, BQ_TABLE,
    DRIVE_FILE_ID,
)


# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

BQ_TABLE_REF = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
UPSERT_KEY   = ["num"]

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/bigquery",
]


# ─────────────────────────────────────────
# 1. AUTHENTICATE
# ─────────────────────────────────────────

def get_credentials():
    return service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )


# ─────────────────────────────────────────
# 2. DOWNLOAD CSV FROM GOOGLE DRIVE
# ─────────────────────────────────────────

def download_from_drive(file_id: str, credentials) -> pd.DataFrame:
    service    = build("drive", "v3", credentials=credentials)
    request    = service.files().get_media(fileId=file_id)
    buffer     = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    buffer.seek(0)
    df = pd.read_csv(buffer, dtype=str)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)

    print(f"✓ Downloaded {len(df):,} rows from Google Drive (file_id: {file_id})")
    return df


# ─────────────────────────────────────────
# 3. UPSERT INTO BIGQUERY
#    Strategy: delete matching keys + insert all incoming rows
#    - No DML (MERGE/UPDATE/DELETE) → free tier compatible
#    - Pulls existing keys from BQ, removes overlapping rows,
#      then appends the full incoming dataset
# ─────────────────────────────────────────

def upsert_to_bigquery(df: pd.DataFrame, credentials) -> None:
    client    = bigquery.Client(project=BQ_PROJECT, credentials=credentials)
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    # --- Step 1: Check if target table already has data ---
    try:
        existing = client.query(
            f"SELECT {', '.join(UPSERT_KEY)} FROM `{table_ref}`"
        ).to_dataframe()
        table_exists = True
        print(f"  ✓ Target table found with {len(existing):,} existing rows")
    except Exception:
        existing     = pd.DataFrame(columns=UPSERT_KEY)
        table_exists = False
        print(f"  ✓ Target table is empty or does not exist yet")

    # --- Step 2: Separate incoming rows into updates vs new inserts ---
    if table_exists and not existing.empty:
        # Cast key columns to string for safe comparison
        for col in UPSERT_KEY:
            existing[col] = existing[col].astype(str)
            df[col]       = df[col].astype(str)

        existing_keys = set(
            existing[UPSERT_KEY].apply(tuple, axis=1)
        )
        incoming_keys = df[UPSERT_KEY].apply(tuple, axis=1)

        df_update = df[incoming_keys.isin(existing_keys)]     # rows to replace
        df_insert = df[~incoming_keys.isin(existing_keys)]    # brand new rows

        print(f"  → {len(df_update):,} rows to update | {len(df_insert):,} new rows to insert")
    else:
        df_update = pd.DataFrame(columns=df.columns)
        df_insert = df
        print(f"  → {len(df_insert):,} rows to insert (fresh load)")

    # --- Step 3: Load full incoming df into a temp table (free tier safe) ---
    staging_ref              = f"{BQ_PROJECT}.{BQ_DATASET}._staging_{BQ_TABLE}"
    job_config               = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect    = True

    print(f"  Uploading {len(df):,} rows to staging table...")
    job = client.load_table_from_dataframe(df, staging_ref, job_config=job_config)
    job.result()
    print(f"  ✓ Staging table ready: {staging_ref}")

    # --- Step 4: Rebuild target table = (existing - overlapping) + incoming ---
    # Read current table rows that are NOT in the incoming update set
    if table_exists and not existing.empty and len(df_update) > 0:
        print(f"  Reading existing rows not affected by update...")
        existing_full = client.query(
            f"SELECT * FROM `{table_ref}`"
        ).to_dataframe()

        for col in UPSERT_KEY:
            existing_full[col] = existing_full[col].astype(str)

        keep_mask     = ~existing_full[UPSERT_KEY].apply(tuple, axis=1).isin(existing_keys & set(incoming_keys))
        df_keep       = existing_full[keep_mask]
        df_final      = pd.concat([df_keep, df], ignore_index=True)
        print(f"  → Final table will have {len(df_final):,} rows")
    else:
        df_final = pd.concat([
            client.query(f"SELECT * FROM `{table_ref}`").to_dataframe()
            if table_exists and len(df_update) == 0 and not existing.empty else pd.DataFrame(),
            df
        ], ignore_index=True) if table_exists and not existing.empty else df

    # --- Step 5: Write final result back to target table (overwrite) ---
    job_config2               = bigquery.LoadJobConfig()
    job_config2.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config2.autodetect    = True

    print(f"  Writing {len(df_final):,} rows to {table_ref}...")
    job2 = client.load_table_from_dataframe(df_final, table_ref, job_config=job_config2)
    job2.result()
    print(f"  ✓ Upsert complete → {table_ref}")

    # --- Step 6: Drop staging table ---
    client.delete_table(staging_ref, not_found_ok=True)
    print(f"  ✓ Staging table dropped")


# ─────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────

def run_pipeline():
    print("\n─── Google Drive → BigQuery Pipeline ───\n")

    creds = get_credentials()

    print("[1] Downloading CSV from Google Drive...")
    df = download_from_drive(DRIVE_FILE_ID, creds)

    print("\n[2] Adding export_date and pipeline_timestamp...")
    dates     = pd.to_datetime(df["transaction_date"], errors="coerce").dropna()
    first     = dates.min().strftime("%Y-%m-%d") if not dates.empty else None
    last      = dates.max().strftime("%Y-%m-%d") if not dates.empty else None
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    df["export_date"]        = f"{first} to {last}" if first and last else None
    df["pipeline_timestamp"] = timestamp

    print(f"  ✓ export_date        : {df['export_date'].iloc[0]}")
    print(f"  ✓ pipeline_timestamp : {df['pipeline_timestamp'].iloc[0]}")

    print("\n[3] Upserting into BigQuery...")
    upsert_to_bigquery(df, creds)

    print("\n✓ Pipeline complete.")


if __name__ == "__main__":
    run_pipeline()