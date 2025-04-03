# Sample Project: Data Engineering with Microsoft Fabric and Apache Spark

## Project Overview

This sample project demonstrates the creation and management of scalable data pipelines in the microfinance domain using Microsoft Fabric's Spark runtime. It simulates realistic workloads such as customer onboarding, loan data tracking, data quality enforcement, and SCD Type 2 history management.

## Project Structure

```
/data-engineering-fabric-spark
├── README.md
├── LICENSE
├── .gitignore
├── data
│   └── sample_data.csv
├── data-pipeline-configuration
│   └── fabric_spark_pipeline.json
├── flowchart
│   └── SCD_type2.drawio
├── scripts
│   ├── data_ingestion.py
│   ├── data_transformation.py
│   ├── data_quality_checks.py
│   └── scd_type2_handling.py
```

## Technologies Used
- Microsoft Fabric (Spark Runtime)
- PySpark
- Delta Lake
- JSON Notebook (Fabric-native)

## Core Capabilities

### 🔹 Ingestion
- Ingests microfinance domain data (e.g., customer, loan transactions) from CSV into a Lakehouse table.
- `data_ingestion.py` reads the raw CSV and writes it to a Delta table.

### 🔹 Transformation
- `data_transformation.py` performs cleaning, formatting, and joins for loan analysis.

### 🔹 Data Quality
- `data_quality_checks.py` verifies data integrity (null checks, schema validation).

### 🔹 SCD Type 2
- `scd_type2_handling.py` tracks historical changes to customer attributes (e.g., income, status).
- `SCD_type2.drawio` (in the `flowchart` folder) visually illustrates how historical changes are tracked over time.

## How to Run

### Step 1: Environment Setup
```bash
pip install pyspark
```

### Step 2: Run Each Pipeline Component
```bash
python scripts/data_ingestion.py
python scripts/data_transformation.py
python scripts/data_quality_checks.py
python scripts/scd_type2_handling.py
```

## Microsoft Fabric Usage
- The `.json` file under `data-pipeline-configuration/` is the exported notebook from Microsoft Fabric.
- Open it in Fabric's workspace to execute via UI.
- Schedule execution using Fabric Pipelines.

## Outcome
- Version-controlled, modular PySpark pipelines designed for microfinance analytics.
- Compatible with Microsoft Fabric’s Lakehouse and Notebook environment.
- Designed to be extensible for future integration into Azure DevOps or GitHub Actions workflows.

