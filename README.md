# Project Stream

This repository contains a real data engineering project built with **Snowflake + Python**, demonstrating a complete pipeline from raw data ingestion to analytics and data quality verification.

---

## 🚀 Project Overview

This pipeline ingests a real dataset of Netflix titles, loads it into Snowflake, performs cleaning and transformation, and builds analytics tables. It also includes automated data quality checks to ensure correctness and integrity at each stage.

---

## 🧩 Architecture
Raw CSV Dataset
↓ (Python ingestion + validation)
Snowflake Internal Stage
↓ (COPY INTO)
RAW Table (Netflixed Titles)
↓ (SQL transforms)
Analytics Tables
├── TITLES_CLEAN
├── TITLE_GENRES
├── TITLE_COUNTRIES
└── YEARLY_COUNTS
↓ (Quality checks)
Automated Data Validations

---

## 📌 Key Components

### 📥 Data Ingestion
- Downloads a real Netflix titles dataset.
- Stores it locally under `data/raw/`.

### 📤 Loading to Snowflake
- Uses Snowflake internal staging and `COPY INTO` for efficient batch loading.

### 🛠 Transformations
- Cleans raw data to produce analytics-ready tables.
- Normalizes multi-valued attributes (genres, countries) into bridge tables.
- Aggregates metrics like title counts by year/type.

### ✅ Data Quality Checks
Automated tests verify:
- Non-empty raw and clean tables
- No null/duplicate primary identifiers
- Valid domain values
- Split tables have no blank entries

---

## 📌 Showcase Queries

These can be run in Snowflake to explore results:

```sql
-- Top genres by number of titles
SELECT genre, COUNT(*) AS titles
FROM NETFLIX_DE.ANALYTICS.TITLE_GENRES
GROUP BY 1
ORDER BY titles DESC
LIMIT 10;

-- Titles by year and type
SELECT release_year, type, titles
FROM NETFLIX_DE.ANALYTICS.YEARLY_COUNTS
ORDER BY release_year DESC, type;

-- Most common countries
SELECT country, COUNT(*) AS titles
FROM NETFLIX_DE.ANALYTICS.TITLE_COUNTRIES
GROUP BY 1
ORDER BY titles DESC
LIMIT 10;

