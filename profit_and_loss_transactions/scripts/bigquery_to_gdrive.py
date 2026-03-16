"""
Pipeline 2: BigQuery → Google Drive
Queries a BigQuery table and uploads the result as a CSV to Google Drive.

Requirements:
    pip install google-cloud-bigquery google-api-python-client google-auth pandas
"""

import io
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.cloud import bigquery

from config import (
    SERVICE_ACCOUNT_FILE,
    BQ_PROJECT, BQ_DATASET, BQ_TABLE,
    DRIVE_FOLDER_ID, OUTPUT_FILENAME,
)


# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

BQ_TABLE_REF = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

# Optional: custom SQL query — set to None to export the full table
CUSTOM_QUERY = None
# CUSTOM_QUERY = f"""
#     SELECT *
#     FROM `{BQ_TABLE_REF}`
#     WHERE transaction_date >= '2024-01-01'
# """

# True  = overwrite existing file in Drive (preserves file ID and share links)
# False = create a new file each run
OVERWRITE_EXISTING = True

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery.readonly",
]


# ─────────────────────────────────────────
# 1. AUTHENTICATE
# ─────────────────────────────────────────

def get_credentials():
    return service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )


# ─────────────────────────────────────────
# 2. QUERY BIGQUERY
# ─────────────────────────────────────────

def query_bigquery(credentials) -> pd.DataFrame:
    client = bigquery.Client(project=BQ_PROJECT, credentials=credentials)
    sql    = CUSTOM_QUERY if CUSTOM_QUERY else f"SELECT * FROM `{BQ_TABLE_REF}`"

    print(f"  Running query on {BQ_TABLE_REF}...")
    df = client.query(sql).to_dataframe()
    print(f"  ✓ Fetched {len(df):,} rows, {len(df.columns)} columns")
    return df


# ─────────────────────────────────────────
# 3. UPLOAD CSV TO GOOGLE DRIVE
# ─────────────────────────────────────────

def find_existing_file(service, folder_id: str, filename: str):
    """Return file ID if a file with the same name exists in the folder."""
    query   = (
        f"name = '{filename}' "
        f"and '{folder_id}' in parents "
        f"and mimeType = 'text/csv' "
        f"and trashed = false"
    )
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files   = results.get("files", [])
    return files[0]["id"] if files else None


def upload_to_drive(df: pd.DataFrame, credentials) -> None:
    service = build("drive", "v3", credentials=credentials)

    buffer = io.BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    media    = MediaIoBaseUpload(buffer, mimetype="text/csv", resumable=True)
    metadata = {"name": OUTPUT_FILENAME}

    existing_id = find_existing_file(service, DRIVE_FOLDER_ID, OUTPUT_FILENAME) \
                  if OVERWRITE_EXISTING else None

    if existing_id:
        service.files().update(
            fileId=existing_id,
            media_body=media,
        ).execute()
        print(f"  ✓ Overwrote existing file '{OUTPUT_FILENAME}' (id: {existing_id})")
    else:
        metadata["parents"] = [DRIVE_FOLDER_ID]
        file = service.files().create(
            body=metadata,
            media_body=media,
            fields="id, name",
        ).execute()
        print(f"  ✓ Uploaded new file '{OUTPUT_FILENAME}' (id: {file['id']})")


# ─────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────

def run_pipeline():
    print("\n─── BigQuery → Google Drive Pipeline ───\n")

    creds = get_credentials()

    print("[1] Querying BigQuery...")
    df = query_bigquery(creds)

    print("\n[2] Uploading CSV to Google Drive...")
    upload_to_drive(df, creds)

    print("\n✓ Pipeline complete.")


if __name__ == "__main__":
    run_pipeline()
