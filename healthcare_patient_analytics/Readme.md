# Healthcare Patient Analytics Dashboard

An end-to-end data analytics project built through self-study, analyzing 54,966 patient admissions across 2019–2024. The project covers data cleaning, SQL transformation, and an interactive Power BI dashboard.

---

## Tools & Technologies

| Tool | Usage |
|------|-------|
| Python (pandas) | Initial data inspection and cleaning |
| SQL | Data transformation and analysis queries |
| Power BI | Dashboard development and DAX measures |
| Excel | Data validation and quick exploration |

---

## Dataset

- **Source:** Healthcare dataset (CSV)
- **Rows:** 54,966 patient records
- **Columns:** name, age, gender, blood_type, medical_condition, date_of_admission, doctor, hospital, insurance_provider, billing_amount, room_number, admission_type, discharge_date, medication, test_results

---

## Data Cleaning & Transformation

### SQL Cleaning Operations

**1. Inspect for nulls**
```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN name IS NULL THEN 1 END) AS null_name,
    COUNT(CASE WHEN age IS NULL THEN 1 END) AS null_age,
    COUNT(CASE WHEN medical_condition IS NULL THEN 1 END) AS null_condition,
    COUNT(CASE WHEN billing_amount IS NULL THEN 1 END) AS null_billing
FROM healthcare_dataset;
```

**2. Remove duplicate records**
```sql
DELETE FROM healthcare_dataset
WHERE row_num > 1;
```

**3. Standardize text columns (trim whitespace, fix casing)**
```sql
UPDATE healthcare_dataset
SET
    medical_condition = TRIM(INITCAP(medical_condition)),
    admission_type    = TRIM(INITCAP(admission_type)),
    gender            = TRIM(INITCAP(gender)),
    test_results      = TRIM(INITCAP(test_results));
```

**4. Validate date logic (discharge must be after admission)**
```sql
SELECT *
FROM healthcare_dataset
WHERE discharge_date < date_of_admission;
```

**5. Check for out-of-range values**
```sql
SELECT *
FROM healthcare_dataset
WHERE age < 0 OR age > 120
   OR billing_amount < 0;
```

---

### SQL Transformation Operations

**6. Calculate length of stay**
```sql
SELECT
    name,
    date_of_admission,
    discharge_date,
    DATEDIFF(DAY, date_of_admission, discharge_date) AS admission_days
FROM healthcare_dataset;
```

**7. Admissions by year**
```sql
SELECT
    YEAR(date_of_admission) AS admission_year,
    COUNT(*) AS total_admissions
FROM healthcare_dataset
GROUP BY YEAR(date_of_admission)
ORDER BY admission_year;
```

**8. Average billing by medical condition**
```sql
SELECT
    medical_condition,
    ROUND(AVG(billing_amount), 2) AS avg_billing,
    COUNT(*) AS total_patients
FROM healthcare_dataset
GROUP BY medical_condition
ORDER BY avg_billing DESC;
```

**9. Abnormal test rate by condition**
```sql
SELECT
    medical_condition,
    COUNT(*) AS total_patients,
    SUM(CASE WHEN test_results = 'Abnormal' THEN 1 ELSE 0 END) AS abnormal_count,
    ROUND(
        100.0 * SUM(CASE WHEN test_results = 'Abnormal' THEN 1 ELSE 0 END) / COUNT(*), 2
    ) AS abnormal_rate_pct
FROM healthcare_dataset
GROUP BY medical_condition
ORDER BY abnormal_rate_pct DESC;
```

**10. Age bracket segmentation**
```sql
SELECT
    CASE
        WHEN age BETWEEN 1  AND 20 THEN '1-20'
        WHEN age BETWEEN 21 AND 30 THEN '21-30'
        WHEN age BETWEEN 31 AND 40 THEN '31-40'
        WHEN age BETWEEN 41 AND 50 THEN '41-50'
        WHEN age BETWEEN 51 AND 60 THEN '51-60'
        WHEN age BETWEEN 61 AND 70 THEN '61-70'
        WHEN age BETWEEN 71 AND 80 THEN '71-80'
        ELSE '80+'
    END AS age_bracket,
    COUNT(*) AS total_patients
FROM healthcare_dataset
GROUP BY age_bracket
ORDER BY age_bracket;
```

**11. Insurance provider distribution**
```sql
SELECT
    insurance_provider,
    COUNT(*) AS total_patients,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS share_pct
FROM healthcare_dataset
GROUP BY insurance_provider
ORDER BY total_patients DESC;
```

**12. Admission type breakdown per condition**
```sql
SELECT
    medical_condition,
    admission_type,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY medical_condition), 2) AS pct_within_condition
FROM healthcare_dataset
GROUP BY medical_condition, admission_type
ORDER BY medical_condition, admission_type;
```

---

## DAX Measures (Power BI)

```dax
-- Length of stay (calculated column)
admission_days =
DATEDIFF(
    healthcare_dataset_processed[date_of_admission],
    healthcare_dataset_processed[discharge_date],
    DAY
)

-- Abnormal test rate
Abnormal Test Rate =
DIVIDE(
    COUNTROWS(FILTER(healthcare_dataset_processed, healthcare_dataset_processed[test_results] = "Abnormal")),
    COUNTROWS(healthcare_dataset_processed)
)

-- Normal test rate
Normal Test Rate =
DIVIDE(
    COUNTROWS(FILTER(healthcare_dataset_processed, healthcare_dataset_processed[test_results] = "Normal")),
    COUNTROWS(healthcare_dataset_processed)
)

-- Inconclusive test rate
Inconclusive Test Rate =
DIVIDE(
    COUNTROWS(FILTER(healthcare_dataset_processed, healthcare_dataset_processed[test_results] = "Inconclusive")),
    COUNTROWS(healthcare_dataset_processed)
)
```

---

## Dashboard Features

- **KPI Cards** — Total Admissions, Avg Billing Amount, Avg Admission Days, Abnormal Test Rate
- **Admissions by Date** — yearly trend from 2019 to 2024
- **Admissions by Type and Age Bracket** — distribution across age groups
- **Avg Billing by Condition** — comparison across 6 medical conditions
- **Admissions by Medical Condition** — ranked table with total patients
- **Admissions by Gender** — Male vs Female split
- **Insurance Provider Share** — donut chart with percentage breakdown
- **Interactive Slicers** — filter by Medical Condition, Admission Type, and Insurance Provider

---

## Key Insights

- Total of **54,966** patient admissions recorded across 6 years
- Average billing amount is **$25.55K** per admission
- Average length of stay is **15.5 days** across all conditions
- Abnormal test rate is consistent at **~33.5%** across all medical conditions
- Admission volume peaked in **2020** at 11.2K and dipped in **2024** (partial year at 3.8K)
- Insurance provider distribution is nearly equal across all five providers (~20% each)
- Gender split is almost perfectly even — Male 27.50K, Female 27.47K

---

## Project Structure

```
healthcare-analytics-dashboard/
│
├── data/
│   └── healthcare_dataset_processed.csv
│
├── sql/
│   └── cleaning_and_transformation.sql
│
├── powerbi/
│   └── healthcare_patient_analytics_dashboard.pbix
│
└── README.md
```

---

## About

Built entirely through self-study as part of my journey into data analytics and engineering. Tools used span the core data stack — SQL for querying and transformation, Python for initial cleaning, Power BI for visualization, and Excel for validation.
