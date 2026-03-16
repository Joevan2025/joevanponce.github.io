# P&L ETL Pipeline

A Python-based ETL pipeline for cleaning and transforming Profit & Loss transaction data exported from QuickBooks, with automated loading into BigQuery via Google Drive.

---

## Project Structure

```
etl/P&L/
├── scripts/
│   ├── transformation_01.py      # Stage 01 → Stage 02 cleaning pipeline
│   ├── gdrive_to_bigquery.py     # Google Drive → BigQuery pipeline
│   ├── config.py                 # Credentials and settings (do not commit)
│   └── service_account.json      # GCP service account key (do not commit)
├── raw_data/                     # Raw source files (unmodified)
├── processed_data/               # Cleaned and transformed output files
├── README.md
└── .gitignore
```

---

## Workflow Overview

```
Google Drive (raw)
      ↓  Step 1: Download manually
  raw_data/
      ↓  Step 2: Clean and transform
  processed_data/
      ↓  Step 3: Upload manually
Google Drive (processed)
      ↓  Step 4 & 5: Pipeline
    BigQuery
```

---

## Step 1 — Download Raw Data from Google Drive

Download the raw QuickBooks P&L export from Google Drive manually and save it into the `raw_data/` folder.

**File naming convention:**
```
raw_data/profit_and_loss_stage_01.csv
```

**Source columns expected:**
`category`, `transaction_date`, `transaction_type`, `num`, `name`, `territory`, `class_full_name`, `memo_description`, `item_split_account`, `amount`, `balance`

> Do not modify the raw file. Always keep the original untouched in `raw_data/` as a reference.

---

## Step 2 — Clean and Transform the Data

Cleaning is done in two passes:

### Pass A — Excel (manual pre-cleaning)
Before running the Python script, do a quick manual review in Excel:
- Check for obvious formatting issues (merged cells, extra header rows, irregular spacing)
- Confirm column names match the expected schema
- Save the file as `.csv` into `processed_data/` as `profit_and_loss_stage_01.csv`

### Pass B — Python (`transformation_01.py`)
Run the cleaning script to produce the final cleaned file:

```bash
cd scripts
python transformation_01.py
```

**What the script does:**
- Treats blank and whitespace-only cells as `null` on load (also handles `N/A`, `NULL`, `none`, `-`)
- Parses `transaction_date` to `YYYY-MM-DD` format
- Strips thousands separators and converts accounting negatives `(1,000)` → `-1000` for `amount` and `balance`
- Normalizes case and trims whitespace on all text and categorical columns
- Keeps `num` as a string to preserve leading zeros; drops duplicate transaction IDs
- Splits `item_split_account` on `:` into two new columns: `acct_level1` and `acct_level2`
- Preserves the original row order and column sequence from the source file
- Prints a null audit and summary report on each run
- Exports to both `.csv` and `.xlsx`

**Output columns:**
`category`, `transaction_date`, `transaction_type`, `num`, `name`, `territory`, `class_full_name`, `memo_description`, `item_split_account`, `acct_level1`, `acct_level2`, `amount`, `balance`

**Output files:**
```
processed_data/profit_and_loss_stage_02.csv
processed_data/profit_and_loss_stage_02.xlsx
```

---

## Step 3 — Upload Processed Data to Google Drive

After the cleaning script finishes, manually upload the processed file to Google Drive:

1. Go to [Google Drive](https://drive.google.com)
2. Navigate to your designated P&L processed data folder
3. Upload `processed_data/profit_and_loss_stage_02.csv`
4. Copy the **File ID** from the shareable link — you will need this for `config.py`

> The shareable link looks like:
> `https://drive.google.com/file/d/FILE_ID_HERE/view`

---

## Step 4 — Set Up Google Drive and BigQuery for the Pipeline

### 4A — GCP Project and Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create or select a project
3. Enable these APIs under **APIs & Services → Library**:
   - Google Drive API
   - BigQuery API
4. Go to **APIs & Services → Credentials → Create Credentials → Service account**
5. Grant the service account these roles under **IAM & Admin → IAM**:

| Role | Purpose |
|---|---|
| `BigQuery Data Editor` | Read and write BigQuery tables |
| `BigQuery Job User` | Run queries and load jobs |

6. Under the service account → **Keys** tab → **Add Key → JSON** → download the file
7. Save it as `scripts/service_account.json`

### 4B — BigQuery Dataset and Table

1. In BigQuery, create a new **Dataset** (choose the region closest to you, e.g. `asia-southeast1` for PH)
2. Inside the dataset, create a new **Table** with the following schema:

```json
[
  { "name": "category",            "type": "STRING"   },
  { "name": "transaction_date",    "type": "DATE"     },
  { "name": "transaction_type",    "type": "STRING"   },
  { "name": "num",                 "type": "STRING"   },
  { "name": "name",                "type": "STRING"   },
  { "name": "territory",           "type": "STRING"   },
  { "name": "class_full_name",     "type": "STRING"   },
  { "name": "memo_description",    "type": "STRING"   },
  { "name": "item_split_account",  "type": "STRING"   },
  { "name": "amount",              "type": "FLOAT"    },
  { "name": "balance",             "type": "FLOAT"    },
  { "name": "export_date",         "type": "STRING"   },
  { "name": "pipeline_timestamp",  "type": "DATETIME" }
]
```

### 4C — Google Drive Sharing

Share the processed CSV file and the export folder with the service account email (found in `service_account.json` under `client_email`):

- Right-click the file → **Share** → paste the service account email → set role to **Editor**

### 4D — Fill in `config.py`

```python
SERVICE_ACCOUNT_FILE = r"C:\Users\ADMIN\projects\etl\P&L\scripts\service_account.json"

BQ_PROJECT   = "your_project_id"       # GCP Console → project dropdown → Project ID
BQ_DATASET   = "your_dataset"          # BigQuery dataset name you created
BQ_TABLE     = "profit_and_loss"       # BigQuery table name you created

DRIVE_FILE_ID   = "your_file_id"       # from the processed CSV shareable link
DRIVE_FOLDER_ID = "your_folder_id"     # from the Drive folder URL
OUTPUT_FILENAME = "profit_and_loss_export.csv"
```

---

## Step 5 — Run the Pipeline

Install dependencies first (only needed once):

```bash
pip install google-cloud-bigquery google-api-python-client google-auth pandas pyarrow openpyxl
```

Then run the pipeline:

```bash
cd scripts
python gdrive_to_bigquery.py
```

**What the pipeline does:**
- Downloads `profit_and_loss_stage_02.csv` from Google Drive into memory
- Adds two metadata columns to every row:
  - `export_date` — the transaction date range of the data e.g. `2024-01-01 to 2024-12-31`
  - `pipeline_timestamp` — the UTC datetime when the pipeline ran e.g. `2025-07-01 14:30:00`
- Upserts into BigQuery using a **delete + insert** strategy (free tier compatible — no MERGE/DML billing required):
  - Compares incoming rows against existing rows using `num` as the unique key
  - Updates rows that already exist, inserts rows that are new
  - Uses a temporary staging table that is dropped after each run
- Prints a step-by-step progress log including row counts and timestamps

**Expected output:**
```
─── Google Drive → BigQuery Pipeline ───

[1] Downloading CSV from Google Drive...
✓ Downloaded 1,500 rows from Google Drive

[2] Adding export_date and pipeline_timestamp...
  ✓ export_date        : 2024-01-01 to 2024-12-31
  ✓ pipeline_timestamp : 2025-07-01 14:30:00

[3] Upserting into BigQuery...
  ✓ Target table found with 1,200 existing rows
  → 300 rows to update | 200 new rows to insert
  ✓ Staging table ready
  ✓ Upsert complete → your_project.your_dataset.profit_and_loss
  ✓ Staging table dropped

✓ Pipeline complete.
```

---

## Dependencies

| Package | Purpose |
|---|---|
| `google-cloud-bigquery` | BigQuery read and write |
| `google-api-python-client` | Google Drive API |
| `google-auth` | Service account authentication |
| `pandas` | Data cleaning and transformation |
| `pyarrow` | Required by `load_table_from_dataframe` |
| `openpyxl` | Excel `.xlsx` export in transformation script |

---

## Security

**Never commit `config.py` or `service_account.json` to version control.**

Add the following to `.gitignore`:

```
config.py
*.json
```

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError` | Missing dependency | Run `pip install` command above |
| `FileNotFoundError: service_account.json` | Wrong path in `config.py` | Check `SERVICE_ACCOUNT_FILE` path |
| `403 Forbidden` on Drive | File not shared with service account | Share the file with the service account email as Editor |
| `403 Forbidden` on BigQuery | Missing IAM roles | Add `BigQuery Data Editor` and `BigQuery Job User` in IAM |
| `403 billingNotEnabled` | Free tier does not support MERGE/DML | Pipeline already uses delete + insert to avoid this |
| `404 Not Found` on Drive file | Wrong File ID in `config.py` | Re-copy the File ID from the shareable link |
| `not found: dataset` | Wrong dataset name in `config.py` | Check `BQ_DATASET` matches exactly what is in BigQuery |