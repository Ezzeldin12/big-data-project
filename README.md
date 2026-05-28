# 🚕 Big Data Project — NYC Yellow Taxi ETL Pipeline

> An end-to-end big data pipeline that processes 24 months of NYC Yellow Taxi trip data using the Medallion Architecture on Databricks, with final analytics visualized in a Power BI dashboard.

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-0071C5?style=for-the-badge&logo=delta&logoColor=white)

---

## 📌 Overview

This project builds a production-style **big data ETL pipeline** for analyzing NYC Yellow Taxi trip records using the **Medallion Architecture** (Bronze → Silver → Gold). Raw parquet files from the NYC TLC (November 2014 – February 2015) are ingested, cleaned, deduplicated, and enriched across multiple pipeline stages using **Databricks Delta Live Tables (DLT)**.

The final Gold layer produces 6 aggregated analytical tables covering monthly trends, hourly demand patterns, trip distances, fare efficiency, passenger behavior, and vendor/payment analysis. These tables are exported as CSVs and loaded into a **Power BI dashboard** with custom DAX formulas and a dark theme.

---

## ✨ Features

- 🥉 **Bronze Layer** — raw data ingestion from NYC TLC parquet files into Delta tables
- 🥈 **Silver Layer** — schema standardization, data cleaning, deduplication, and feature enrichment
- 🥇 **Gold Layer** — 6 aggregated analytical tables ready for BI consumption
- ⚡ **Databricks DLT** — fully orchestrated pipeline with dependency management
- 📊 **Power BI Dashboard** — interactive dark-themed dashboard with DAX-powered HTML cards and charts
- 🧹 **Data Quality** — handles nulls, outliers, duplicate trips, and schema mismatches

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Compute | Databricks (Delta Live Tables) | Pipeline orchestration |
| Processing | Apache Spark (PySpark) | Large-scale data transformation |
| Storage | Delta Lake | ACID-compliant table format |
| Language | Python | Pipeline scripts |
| Visualization | Power BI Desktop | Interactive dashboard |
| DAX | Power BI DAX | Custom KPI cards and chart logic |

---

## 🏗️ Pipeline Architecture

```
NYC TLC Parquet Files (Raw)
        │
        ▼
┌─────────────────────┐
│   BRONZE LAYER      │  Raw ingestion, no transformations
│   01_bronze.py      │
└─────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│                  SILVER LAYER                        │
│  02_silver_standardize.py  → Schema standardization  │
│  03_silver_clean.py        → Remove nulls/outliers   │
│  04_silver_dedup.py        → Deduplicate trips        │
│  05_silver_enrich.py       → Add derived features    │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│                       GOLD LAYER                              │
│  06_gold_monthly_trends.py     → Revenue & trip trends        │
│  07_gold_trip_distance.py      → Distance distribution        │
│  08_gold_passenger_analysis.py → Passenger count patterns     │
│  09_gold_fare_efficiency.py    → Fare per mile metrics        │
│  10_gold_hourly_demand.py      → Peak hours analysis          │
│  11_gold_vendor_payment.py     → Vendor & payment breakdown   │
└──────────────────────────────────────────────────────────────┘
        │
        ▼
  6 Gold CSV Files → Power BI Dashboard
```

---

## 📁 Project Structure

```
big data project/
└── NYC_Pipeline_v2/
    ├── notebooks/
    │   ├── 00_download_data.py          # Data acquisition
    │   ├── 00_setup.py                  # Pipeline config reference
    │   ├── 01_bronze.py                 # Bronze: raw ingestion
    │   ├── 02_silver_standardize.py     # Silver: schema
    │   ├── 03_silver_clean.py           # Silver: cleaning
    │   ├── 04_silver_dedup.py           # Silver: deduplication
    │   ├── 05_silver_enrich.py          # Silver: enrichment
    │   ├── 06_gold_monthly_trends.py    # Gold: monthly KPIs
    │   ├── 07_gold_trip_distance.py     # Gold: distance analysis
    │   ├── 08_gold_passenger_analysis.py # Gold: passengers
    │   ├── 09_gold_fare_efficiency.py   # Gold: fare metrics
    │   ├── 10_gold_hourly_demand.py     # Gold: hourly demand
    │   └── 11_gold_vendor_payment.py    # Gold: vendor/payment
    ├── gold_*.csv                       # Exported Gold tables (6 files)
    ├── PIPELINE_DOCS.md                 # Full pipeline documentation
    ├── POWERBI_GUIDE.md                 # Power BI setup guide
    ├── DAX_HTML_CARDS.dax               # DAX for KPI cards
    ├── DAX_HTML_CHARTS.dax              # DAX for chart visuals
    └── nyc_taxi_dark_theme.json         # Power BI theme config
```

---

## ⚙️ Setup & Run

### Step 1 — Set up Databricks

1. Create a Databricks workspace
2. Upload the `NYC_Pipeline_v2/notebooks/` folder to your Databricks workspace
3. Create a **Delta Live Tables pipeline** pointing to the notebooks directory
4. Run `00_download_data.py` first to acquire the source data

### Step 2 — Run the DLT Pipeline

Trigger the pipeline from the Databricks UI. DLT handles the execution order automatically:
```
Bronze → Silver (standardize → clean → dedup → enrich) → Gold (all 6 tables)
```

### Step 3 — Export Gold Tables

Export the 6 Gold Delta tables as CSV files (already included as `gold_*.csv`).

### Step 4 — Build the Power BI Dashboard

Follow the instructions in [`POWERBI_GUIDE.md`](NYC_Pipeline_v2/POWERBI_GUIDE.md):
1. Open Power BI Desktop
2. Load the 6 gold CSV files
3. Apply the dark theme (`nyc_taxi_dark_theme.json`)
4. Add DAX measures from `DAX_HTML_CARDS.dax` and `DAX_HTML_CHARTS.dax`

---

## 📸 Screenshots

> _Add screenshots of the Power BI dashboard here_
