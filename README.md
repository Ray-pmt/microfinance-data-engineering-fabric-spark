# Microfinance Data Engineering with Microsoft Fabric and Apache Spark

## Project Overview

This project demonstrates the creation and management of scalable data pipelines in the microfinance domain using Microsoft Fabric's Spark runtime. It simulates realistic workloads such as customer onboarding, loan data tracking, data quality enforcement, and SCD Type 2 history management.

## Pipeline Architecture

![Pipeline activities flowchart](docs/diagrams/ADF_activities_flowchart.png)

## Project Structure

```
/microfinance-data-engineering-fabric-spark
├── README.md
├── LICENSE
├── .gitignore
├── requirements.txt
├── run_pipeline.sh              # Pipeline orchestration entry point
├── pytest.ini
├── src                          # PySpark pipeline components
│   ├── common.py                # Shared SCD config, dedup, change detection, safe overwrite
│   ├── data_ingestion.py
│   ├── data_transformation.py
│   ├── data_quality_checks.py
│   ├── scd_type2_handling.py
│   └── scd_type4_handling.py
├── config
│   └── fabric_spark_pipeline.json   # Fabric pipeline definition
├── data
│   └── sample_data.csv          # Sample input data
├── tests                        # pytest suite (local Spark session)
└── docs
    └── diagrams
        ├── ADF_activities_flowchart.png
        ├── SCD_type2.drawio
        └── SCD_type4.drawio
```

## Technologies Used
- Microsoft Fabric (Spark Runtime)
- PySpark
- Delta Lake
- Bash scripting (for pipeline orchestration)

## Core Capabilities

### 🔹 Ingestion
- Ingests microfinance domain data (e.g., customer, loan transactions) from CSV into the Lakehouse.
- `data_ingestion.py` reads the raw CSV with an enforced schema, routes invalid and malformed rows to an error path, and writes valid rows as Parquet partitioned by `process_date`.
- Re-running the same `process_date` replaces that batch (dynamic partition overwrite) instead of appending a duplicate.

### 🔹 Transformation
- `data_transformation.py` calculates monthly payments and loan categories for a `process_date` batch, replacing that batch on re-run. A run audit log is written next to the output (`<output>_audit`).

### 🔹 Data Quality
- `data_quality_checks.py` verifies data integrity (null checks, value ranges, allowed statuses) and writes a JSON report plus the offending records.

### 🔹 SCD Type 2
- `scd_type2_handling.py` implements SCD Type 2 for tracking historical changes to customer and loan attributes
- Takes the latest version of each key (by `last_updated`), uses null-safe change detection, and only expires keys present in the incoming batch
- Rewrites the dimension through a staging directory, so reading and replacing the same path is safe, and recovers an interrupted swap on the next run

### 🔹 SCD Type 4
- `scd_type4_handling.py` implements the alternative history-table pattern: a compact *current* table (one row per business key) plus an append-only *history* table of superseded versions
- Shares the same dimension configs (customer, loan) and change-detection logic as the Type 2 module, so the two strategies are directly comparable
- History appends skip versions that are already archived, so retrying a failed run doesn't duplicate history
- Selectable at run time via the pipeline's `scd_type` parameter

**Choosing between Type 2 and Type 4:**

| | SCD Type 2 (in-table versioning) | SCD Type 4 (current + history tables) |
|---|---|---|
| Storage | One table with `effective_start/end_date`, `is_current` | `dim_*_current` + `dim_*_history` |
| Serving queries | Must filter `is_current = true` | Hit the small current table directly |
| Point-in-time joins | Natural (date-range join in one table) | Query the history table |
| Best when | History is queried as often as current state | Dashboards mostly need latest state; history is audit/compliance |

## How to Run

### Option 1: Shell Script (Recommended for Fabric)
The shell script orchestrates the pipeline components in sequence:

```bash
# Run with default parameters (dev environment, SCD Type 2)
./run_pipeline.sh

# Run with specific environment and date
./run_pipeline.sh prod 2025-04-11

# Run with SCD Type 4 (current + history tables) instead of Type 2
./run_pipeline.sh dev 2025-04-11 4
```

The date is the batch's `process_date`. Every step works on that batch only:
- Ingestion, transformation and data quality replace that date's output on a re-run, including clearing it when the re-run has no rows for it.
- The SCD step applies only that date's transformed batch, using the date as the effective date. Dimensions must be built in date order: each dimension records the latest batch date applied to it (in `<table>__watermark`, including batches that changed nothing). Re-running that date is safe as long as it changes nothing (e.g. a retry after a failure); applying an older date, or re-running the latest date with different data, is refused, since either would corrupt the history.

Any failing step exits non-zero, which stops the script. Dimension tables are replaced by writing a staging copy and swapping it in, keeping the previous version until the swap completes; the next run recovers an interrupted swap automatically, even if its batch is empty.

### Option 2: Manual Execution
```bash
python src/data_ingestion.py data/sample_data.csv fabric_ingested_data/parquet_data fabric_ingested_data/error_data 2025-04-11
python src/data_quality_checks.py fabric_ingested_data/parquet_data fabric_reports/quality_report.json 2025-04-11
python src/data_transformation.py fabric_ingested_data/parquet_data fabric_transformed_data/parquet_data 2025-04-11
python src/scd_type2_handling.py fabric_transformed_data/parquet_data fabric_dim_data fabric_dim_data 2025-04-11

# Or SCD Type 4 (writes dim_*_current and dim_*_history tables)
python src/scd_type4_handling.py fabric_transformed_data/parquet_data fabric_dim_data/current fabric_dim_data/history 2025-04-11
```

### Option 3: Microsoft Fabric Execution
- Import the pipeline configuration from `config/fabric_spark_pipeline.json`
- Configure parameters using Fabric's interface
- Schedule execution using Fabric Pipelines

### Correcting a batch that was already applied
Corrected data for a date the SCD step has already applied can't be patched in place. Replay the dimensions instead: the transformed data is kept per date, so the history can be rebuilt from it.

```bash
# 1. Re-run ingestion, quality checks and transformation for the corrected date (replaces that batch)
# 2. Drop the dimension tables and their watermarks (SCD Type 2 shown; for Type 4, drop
#    dim_*_current, dim_*_current__watermark and dim_*_history)
rm -r Files/fabric_data/dev/dim_data/dim_customer* Files/fabric_data/dev/dim_data/dim_loan*
# 3. Re-apply every batch date in order
for d in 2025-04-11 2025-04-12; do
  spark-submit src/scd_type2_handling.py Files/fabric_data/dev/transformed_data Files/fabric_data/dev/dim_data Files/fabric_data/dev/dim_data $d
done
```

## Microsoft Fabric Integration
- The pipeline is optimized for Microsoft Fabric's Delta Lake integration
- Use Fabric's built-in scheduling and monitoring capabilities
- Configure environment-specific parameters through Fabric's interface

## Development and Testing
The test suite runs every step against a local Spark session (Java 17+ required):

```bash
pip install -r requirements.txt
pytest
```

The tests cover schema enforcement and error routing, re-run idempotency, failure propagation, and the SCD Type 2 / Type 4 cases that matter (changed, new, and absent keys; latest-version-wins; null-to-value changes; one batch per date, date-order enforcement and refusal of changed re-runs; recovery from an interrupted table swap or a failed run).

## Outcome
- Version-controlled, production-ready PySpark pipelines for microfinance analytics
- Tested SCD Type 2 and Type 4 implementations
- Compatible with Microsoft Fabric's Lakehouse environment
- Pipeline orchestration using both Fabric's native tools and custom shell scripts