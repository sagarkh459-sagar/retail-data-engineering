# Retail Data Engineering Platform

## Overview

End-to-end retail data engineering project built using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Azure SQL and Power BI.

The platform ingests retail data from multiple sources including Azure SQL, JSON/API sources and Parquet files, processes the data through a Medallion Architecture, and exposes curated Gold-layer datasets for Power BI reporting.

## Architecture

Sources
   ↓
Azure Data Factory
   ↓
ADLS Gen2
   ↓
Bronze Layer
   ↓
Databricks / PySpark
   ↓
Silver Layer
   ↓
Gold Layer
   ↓
Power BI

## Technologies

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Azure SQL
- Power BI
- JSON
- Parquet
- Delta Lake

## Data Sources

1. Azure SQL
2. REST API / GitHub API
3. JSON files
4. Parquet files

## Pipeline

ADF extracts data from source systems and stores raw data in ADLS Gen2.Databricks processes the raw data using PySpark.

### Bronze
Raw data stored with minimal transformation.

### Silver
Data cleaning, type casting, null handling, duplicate removal, standardization and validation.

### Gold
Business-ready datasets and aggregations optimized for reporting.

## Power BI

Gold-layer data is consumed by Power BI to create dashboards covering sales, customers, products, regions and other retail KPIs.
