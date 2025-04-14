#!/bin/bash

# Microfinance Data Pipeline Execution Script
# Usage: ./run_pipeline.sh [environment] [date]

set -e

# Default parameters
ENV=${1:-"dev"}
PROCESS_DATE=${2:-$(date +%Y-%m-%d)}

# Config based on environment
if [ "$ENV" == "prod" ]; then
  INPUT_PATH="Files/raw_data/prod/microfinance_data_${PROCESS_DATE}.csv"
  CONFIG="--conf spark.executor.memory=16g --conf spark.driver.memory=8g"
else
  INPUT_PATH="Files/raw_data/dev/sample_data.csv"
  CONFIG="--conf spark.executor.memory=4g --conf spark.driver.memory=4g"
fi

# Base paths
BASE_PATH="Files/fabric_data/${ENV}"
INGESTED_PATH="${BASE_PATH}/ingested_data"
ERROR_PATH="${BASE_PATH}/error_data"
TRANSFORMED_PATH="${BASE_PATH}/transformed_data"
DIM_PATH="${BASE_PATH}/dim_data"
REPORTS_PATH="${BASE_PATH}/reports"

echo "Starting Microfinance pipeline for ${ENV} environment with data from ${PROCESS_DATE}"

# Data Ingestion
echo "Step 1: Data Ingestion"
spark-submit ${CONFIG} scripts/data_ingestion.py ${INPUT_PATH} ${INGESTED_PATH} ${ERROR_PATH}

# Data Quality Checks
echo "Step 2: Data Quality Checks"
spark-submit ${CONFIG} scripts/data_quality_checks.py ${INGESTED_PATH} ${REPORTS_PATH}/quality_report.json

# Data Transformation
echo "Step 3: Data Transformation"
spark-submit ${CONFIG} scripts/data_transformation.py ${INGESTED_PATH} ${TRANSFORMED_PATH}

# SCD Type 2 Handling
echo "Step 4: SCD Type 2 Processing"
spark-submit ${CONFIG} scripts/scd_type2_handling.py ${TRANSFORMED_PATH} ${DIM_PATH} ${DIM_PATH}

echo "Pipeline completed successfully"