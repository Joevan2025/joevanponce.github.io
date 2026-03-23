# 🛒 E-Commerce Sales Analytics

An end-to-end ETL and analytics project that ingests raw e-commerce sales data, cleans and transforms it into an analysis-ready format, and surfaces business insights through a Power BI dashboard.

---

## 📁 Project Structure

```
ecommerce_sales_analytics/
├── raw_data/
│   └── ecommerce_sales_data.csv        # Source data (raw, unprocessed)
├── processed_data/
│   └── ecommerce_sales_clean.csv       # Cleaned and transformed output
├── ecommerce_sales_EDA.py              # Exploratory Data Analysis script
├── ecommerce_sales_transformation.py   # ETL / data cleaning script
└── ecommerce_sales.pbix                # Power BI dashboard
```

---

## 📊 Dataset Overview

| Attribute       | Details                              |
|----------------|--------------------------------------|
| **Rows**        | 3,500 records                        |
| **Columns**     | 7                                    |
| **Date Range**  | January 2022 – December 2024         |
| **Regions**     | East, North, South, West             |
| **Categories**  | Accessories, Electronics, Office     |

### Columns

| Column         | Type      | Description                          |
|----------------|-----------|--------------------------------------|
| `order_date`   | date      | Date the order was placed            |
| `product_name` | string    | Name of the product sold             |
| `category`     | string    | Product category                     |
| `region`       | string    | Sales region                         |
| `quantity`     | integer   | Units sold per order                 |
| `sales`        | float     | Revenue generated (USD)              |
| `profit`       | float     | Profit from the order (USD)          |

---

## ⚙️ Pipeline

### 1. EDA (`ecommerce_sales_EDA.py`)

Performs an initial inspection of the raw data before any transformation:

- Prints DataFrame shape and column data types
- Counts null values per column
- Checks for duplicate rows

Run this first to understand the raw data's structure and quality before cleaning.

### 2. Transformation (`ecommerce_sales_transformation.py`)

Cleans and standardizes the raw CSV into the processed output:

- **Column names** — stripped, lowercased, and underscored
- **`order_date`** — parsed to datetime (day-first format)
- **`product_name`, `category`, `region`** — trimmed, title-cased, and deduplicated whitespace
- **`quantity`** — cast to integer
- **`sales`, `profit`** — cast to float and rounded to 2 decimal places
- **Sort & reset index** — rows sorted chronologically, index reset
- **Validation output** — prints final shape, dtypes, null counts, duplicate count, date range, and unique regions before export

Output is saved to `processed_data/ecommerce_sales_clean.csv`.

---

## 🚀 Getting Started

### Prerequisites

```bash
pip install pandas
```

### Run the pipeline

```bash
# Step 1 — Explore the raw data
python ecommerce_sales_EDA.py

# Step 2 — Clean and transform
python ecommerce_sales_transformation.py
```

> **Note:** Update the `input_path` and `output_path` variables in both scripts to match your local directory structure before running.

---

## 📈 Dashboard

The Power BI file (`ecommerce_sales.pbix`) connects to the cleaned CSV and visualizes:

- Sales and profit trends over time
- Performance by region and product category
- Top-performing products

Open the `.pbix` file in [Power BI Desktop](https://powerbi.microsoft.com/desktop/) and refresh the data source to point to your local `ecommerce_sales_clean.csv`.

---

## 🛠️ Tech Stack

| Tool          | Purpose                     |
|---------------|-----------------------------|
| Python 3.x    | ETL scripting               |
| pandas        | Data cleaning & transformation |
| Power BI      | Dashboard & visualization   |

---

## 📝 Notes

- The raw source file (`ecommerce_sales_data.csv`) is not included in this repository. Place it in `raw_data/` before running the scripts.
- All monetary values are in USD.
