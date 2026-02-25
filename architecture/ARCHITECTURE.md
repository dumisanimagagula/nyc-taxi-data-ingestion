# NYC Taxi Data Lakehouse Architecture

## Overview
This document provides a high-level overview of the NYC Taxi Data Lakehouse architecture, following a medallion (Bronze/Silver/Gold) pattern. The architecture is designed for scalable, reproducible, and analytics-ready data processing using open-source tools and cloud-native patterns.

## Architecture Diagram
- The main architecture diagram is maintained in `nyc-taxi-data-ingestion-architecture.drawio` (open with draw.io/diagrams.net).
- A PNG/PDF export should be included for quick reference (add `nyc-taxi-data-ingestion-architecture.png` or `.pdf` when available).

## Key Components
- **Bronze Layer:**
  - Raw data ingestion from NYC Taxi sources
  - Data is appended to Iceberg tables on MinIO (object storage)
  - Ingestion orchestrated by Airflow
- **Silver Layer:**
  - Spark jobs transform and clean data from Bronze
  - Data quality checks are applied (configurable via YAML)
  - Output is written to Silver Iceberg tables
- **Gold Layer:**
  - dbt models aggregate and prepare data for analytics
  - Data is queried via Trino and visualized in Superset
- **Orchestration:**
  - Airflow DAGs coordinate ingestion and transformation steps
- **External Interfaces:**
  - Data sources: NYC Taxi public datasets
  - Data consumers: BI tools, analysts, dashboards

## Data Flow
1. **Ingest:** Airflow triggers Python scripts to ingest raw data into Bronze (append-only, no destructive ops).
2. **Transform:** Spark jobs (driven by YAML config) process Bronze data into Silver, applying cleaning and validation.
3. **Aggregate:** dbt models build Gold layer analytics tables, exposed via Trino and Superset.

## Diagram Legend
- **Yellow:** Bronze (Raw Ingest)
- **Blue:** Silver (Cleaned/Validated)
- **Green:** Gold (Analytics/BI)
- **Gray:** Orchestration/External Systems

## Version & Maintenance
- Diagram version: v1.0 (2026-02-25)
- Edit the `.drawio` file for updates; export PNG/PDF after major changes.

## How to Edit the Diagram
- Open `nyc-taxi-data-ingestion-architecture.drawio` with diagrams.net (web or desktop)
- Make changes, save, and export as PNG/PDF for documentation

---
For more details, see the main [README.md](../README.md) and [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md).
